import numpy as np
import pandas as pd
import yaml


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
