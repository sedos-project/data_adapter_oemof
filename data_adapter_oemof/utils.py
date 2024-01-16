import warnings

import numpy as np
import pandas as pd
import yaml

from data_adapter_oemof.mappings import Mapper


def load_yaml(file_path):
    with open(file_path, "r", encoding="UTF-8") as file:
        dictionary = yaml.load(file, Loader=yaml.FullLoader)
    return dictionary


def has_mixed_types(column) -> bool:
    """
    Function to check if an element is of mixed type
    Parameters
    ----------
    column

    Returns bool
    -------

    """
    unique_types = set(type(element) for element in column)
    return len(unique_types) > 1


def convert_mixed_types_to_same_length(column):
    """
    Function to convert entries to arrays of the same length only for columns with mixed types

    Parameters
    ----------
    column

    Returns
    -------

    """
    if has_mixed_types(column):
        max_length = max(
            len(entry) if isinstance(entry, list) else 1 for entry in column
        )
        return [
            entry
            if isinstance(entry, list)
            else [entry for x in range(max_length)]
            if not pd.isna(entry)
            else np.nan  # Keep NaN as is
            for entry in column
        ]
    else:
        return column


# Define a function to aggregate differing values into a list
def _listify_to_periodic(group_df) -> pd.Series:
    """
    Method to aggregate scalar values to periodical values grouped by "name"
    For each group, check whether scalar values differ over the years.
    If yes, write as lists, if not, the original value is written.

    If there is no "year" column, assume the data is already aggregated and
    pass as given.

    Parameters
    ----------
    group_df

    Returns
    ----------
    pd.Series

    Examples
    ----------
    |   region |   year |   invest_relation_output_capacity |   fixed_costs |
    |---------:|-------:|----------------------------------:|--------------:|
    |       BB |   2016 |                               3.3 |             1 |
    |       BB |   2030 |                               3.3 |             2 |
    |       BB |   2050 |                               3.3 |             3 |

    ->
    |   type    | fixed_costs| name | region | year | invest_relation_output_capacity |
    |:--------- |-----------:|:------ |:---------|:---------------:|---:|
    | storage   | [1, 2, 3]  | BB_Lithium_storage_battery | BB |[2016, 2030, 2050]|3.3 |


    """

    if "year" not in group_df.columns:
        return group_df
    unique_values = pd.Series(dtype=object)
    for col in group_df.columns:
        if isinstance(group_df[col][group_df.index[0]], dict):
            # Unique input/output parameters are not allowed per period
            unique_values[col] = group_df[col][group_df.index[0]]
            continue
        # Lists and Series can be passed for special Facades only.
        # Sequences shall be passed as sequences (via links.csv):
        elif any(
            [isinstance(col_entry, (pd.Series, list)) for col_entry in group_df[col]]
        ):
            values = group_df[col].explode().unique()
        else:
            # FIXME: Hotfix "if not" statement to replace nan values from lists:
            #   in final data only complete datasets are expected.
            if not all(group_df[col].isna()) and any(group_df[col].isna()):
                group_df.loc[group_df[col].isna(), col] = (
                    group_df[col]
                    .dropna()
                    .sample(
                        group_df[col]
                        .isna()
                        .sum(),  # get the same number of values as are missing
                        replace=True,
                        random_state=0,
                    )
                    .values
                )  # throw out the index
            values = group_df[col].unique()
        if len(values) > 1:
            unique_values[col] = list(group_df[col])
        else:
            unique_values[col] = group_df[col].iloc[0]
    unique_values["name"] = "_".join(group_df.name)
    unique_values.drop("year")
    return unique_values


def yearly_scalars_to_periodic_values(scalar_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Turns yearly scalar values to periodic values

    First searches for the sequence length which is the length of the complete sequence.

    Then iterates for every element in parametrized elements, groups them for name
    then applies aggregation method

    This leads to aggregating periodically changing values to a list
    with as many entries as there are periods and
    non changing values are kept as what they have been.
    Only values should change periodically that can change and identifiers must be unique.
    Examples:
        |   region |   year |   invest_relation_output_capacity |   fixed_costs |
        |---------:|-------:|----------------------------------:|--------------:|
        |       BB |   2016 |                               3.3 |             1 |
        |       BB |   2030 |                               3.3 |             2 |
        |       BB |   2050 |                               3.3 |             3 |

    Returns:
        |   type    | fixed_costs| name | region | year | invest_relation_output_capacity |
        |:--------- |-----------:|:------ |:---------|:---------------:|---:|
        | storage   | [1, 2, 3]  | BB_Lithium_storage_battery | BB |[2016, 2030, 2050]|3.3 |


    """
    identifiers = ["region", "carrier", "tech"]
    # Check if the identifiers exist if not they will be omitted
    for poss, existing in enumerate(
        [id in scalar_dataframe.columns for id in identifiers]
    ):
        if existing:
            continue
        else:
            scalar_dataframe[identifiers[poss]] = identifiers[poss]

    scalar_dataframe = (
        scalar_dataframe.groupby(["region", "carrier", "tech"])
        .apply(lambda x: _listify_to_periodic(x))
        .reset_index(drop=True)
    )
    scalar_dataframe = scalar_dataframe.apply(convert_mixed_types_to_same_length)

    return scalar_dataframe


def __split_timeseries_into_years(parametrized_sequences):
    split_dataframes = {}
    for sequence_name, sequence_dataframe in parametrized_sequences.items():
        # Group the DataFrame by year using pd.Grouper
        grouped = sequence_dataframe.resample("Y")

        # Iterate over the groups and store each year's DataFrame
        for year, group in grouped:
            split_dataframes[sequence_name + "_" + str(year.year)] = group.copy()

    return split_dataframes


def get_foreign_keys(struct: dict[str, list[str]], mapper: Mapper, components: list) -> list:
    """
    Writes Foreign keys for one process.
    Searches in adapter class for sequences fields

    Parameters
    ----------
    struct: list
        Energy System structure defining input/outputs for Processes
    mapper: Mapper
        for one element of the Process
        (foreign keys have to be equal for all components of a Process)
    components: list
        all components as of a Process as dicts. Helps to check what columns
        that could be pointing to sequences are found in Sequences.

    Returns
    -------
    list of foreignKeys for Process including bus references and pointers to files
    containing `profiles`
    """
    new_foreign_keys = []
    components = pd.DataFrame(components)
    for bus in mapper.get_busses(struct).keys():
        new_foreign_keys.append(
            {"fields": bus, "reference": {"fields": "name", "resource": "bus"}}
        )

    for field in mapper.get_fields():
        if (
            mapper.is_sequence(field.type)
            and field.name in components.columns
            and pd.api.types.infer_dtype(components[field.name]) == "string"
        ):
            if all(components[field.name].isin(mapper.timeseries.columns)):
                new_foreign_keys.append(
                    {
                        "fields": field.name,
                        "reference": {"resource": f"{mapper.process_name}_sequence"},
                    }
                )
            elif any(components[field.name].isin(mapper.timeseries.columns)):
                # Todo clean up on examples:
                #   -remove DE from hackathon or
                #   -create propper example with realistic project data
                warnings.warn(
                    "Not all profile columns are set within the given profiles."
                    f" Please check if there is a timeseries for every Component in "
                    f"{mapper.process_name}"
                )
                new_foreign_keys.append(
                    {
                        "fields": field.name,
                        "reference": {"resource": f"{mapper.process_name}_sequence"},
                    }
                )
            else:
                # The Field is allowed to be a timeseries
                # -> and likely is a supposed to be a timeseries
                # but a scalar or `unused` is found.
                pass
    return new_foreign_keys


def get_periods_from_parametrized_sequences(
    parametrized_sequences,
) -> pd.DataFrame:
    """
    Takes Dictionary with all parametrized sequences per technology and tries to find periods
    csv. First sequence found will be to dervie periods.
    ----------
    parametrized_sequences

    Returns
    -------

    """
    for process_name, sequence in parametrized_sequences.items():
        if len(sequence) != 0:
            sequence = pd.DataFrame(index=pd.to_datetime(sequence.index))
            sequence["periods"] = sequence.groupby(sequence.index.year).ngroup()
            # TODO timeincrement might be adjusted later to modify objective weighting
            sequence["timeincrement"] = 1
            sequence.index.name = "timeindex"
            return sequence
        else:
            pass
    return pd.DataFrame()
