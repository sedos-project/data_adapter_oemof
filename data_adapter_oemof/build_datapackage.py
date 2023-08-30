import dataclasses
import os
import warnings
from typing import Union

import pandas as pd
from data_adapter import core
from data_adapter.preprocessing import Adapter
from datapackage import Package

from data_adapter_oemof.adapters import FACADE_ADAPTERS
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP, Mapper


def refactor_timeseries(timeseries: pd.DataFrame):
    """
    Takes timeseries in single line parameter-model format (start, end, freq,
    region, ts-array...) and turns into Tabular matching format with timeindex
    as timeseries timestamps,  technology-region as header and columns
    containing data.

    Parameters
    ----------
    timeseries: pd.DataFrame
        timeseries in parameter-model format
        (https://github.com/sedos-project/oedatamodel#oedatamodel-parameter)

    Returns
    pd.DataFrame Tabular form of timeseries for multiple periods of similar
    technologies and regions.
    -------

    """
    # Combine all time series into one DataFrame
    df_timeseries = pd.DataFrame()
    if timeseries.empty:
        return df_timeseries
    # Iterate over different time periods/years
    for (start, end, freq), df in timeseries.groupby(
        ["timeindex_start", "timeindex_stop", "timeindex_resolution"]
    ):
        # Get column names of timeseries only
        ts_columns = set(df.columns).difference(core.TIMESERIES_COLUMNS.keys())

        # Iterate over timeseries columns/technologies
        # e.g. multiple efficiencies, onshore/offshore
        df_timeseries_year = pd.DataFrame()
        for profile_name in ts_columns:
            # Unnest timeseries arrays for all regions
            profile_column = df[["region", profile_name]].explode(profile_name)
            # Creating cumcount index as fake-timeindex for every region
            profile_column["index"] = profile_column.groupby("region").cumcount()
            # Pivot table to have regions as columns
            profile_column_pivot = pd.pivot_table(
                profile_column, values=profile_name, index=["index"], columns=["region"]
            )
            profile_column_pivot.reset_index(drop=True)
            # Rename column to: profile_name/technology + region
            profile_column_pivot.columns = [
                f"{profile_name}_{region}" for region in profile_column_pivot.columns
            ]
            # Add additional timeseries for same timeindex as columns
            df_timeseries_year = pd.concat(
                [df_timeseries_year, profile_column_pivot], axis=1
            )

        # Replace timeindex with actual date range
        timeindex = pd.date_range(start=start, end=end, freq=pd.Timedelta(freq))
        df_timeseries_year.index = timeindex
        # Append additional date ranges
        df_timeseries = pd.concat([df_timeseries, df_timeseries_year], axis=0)

    return df_timeseries


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
    -------

    """

    if "year" not in group_df.columns:
        return group_df

    unique_values = pd.Series()
    for col in group_df.columns:  # Exclude 'name' column
        if isinstance(group_df[col][group_df.index[0]], dict):
            # Unique input/output parameters are not allowed per period
            unique_values[col] = group_df[col][group_df.index[0]]
            continue
        # Lists and Series can be passed for special Facades only.
        # Sequences shall be passed as sequences (via links.csv):
        elif any(
            [
                isinstance(col_entry, Union[list, pd.Series])
                for col_entry in group_df[col]
            ]
        ):
            values = group_df[col].explode().unique()
        else:
            values = group_df[col].unique()
        if len(values) > 1:
            unique_values[col] = list(group_df[col])
        else:
            unique_values[col] = group_df[col].iloc[0]
    unique_values["name"] = "_".join(group_df.name)
    unique_values.drop("year")
    return unique_values


@dataclasses.dataclass
class DataPackage:
    parametrized_elements: dict[
        str, pd.DataFrame()
    ]  # datadict with scalar data in form of {type:pd.DataFrame(type)}
    parametrized_sequences: dict[
        str, pd.DataFrame()
    ]  # timeseries in form of {type:pd.DataFrame(type)}
    foreign_keys: dict  # foreign keys for timeseries profiles
    adapter: Adapter
    periods: pd.DataFrame()

    @staticmethod
    def __split_timeseries_into_years(parametrized_sequences):
        split_dataframes = {}
        for sequence_name, sequence_dataframe in parametrized_sequences.items():
            # Group the DataFrame by year using pd.Grouper
            grouped = sequence_dataframe.resample("Y")

            # Iterate over the groups and store each year's DataFrame
            for year, group in grouped:
                split_dataframes[sequence_name + "_" + str(year.year)] = group.copy()

        return split_dataframes

    @staticmethod
    def get_foreign_keys(struct: list, mapper: Mapper, components: list) -> list:
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

        for field in dataclasses.fields(mapper.adapter):
            if (
                mapper.is_sequence(field.type)
                and field.name in components.columns
                and pd.api.types.infer_dtype(components[field.name]) == "string"
            ):
                if all(components[field.name].isin(mapper.timeseries.columns)):
                    new_foreign_keys.append(
                        {
                            "fields": field.name,
                            "reference": {
                                "resource": f"{mapper.process_name}_sequence"
                            },
                        }
                    )
                elif any(components[field.name].isin(mapper.timeseries.columns)):
                    # Todo clean up on examples:
                    #   -remove DE from hackerthon or
                    #   -create propper example with realistic project data
                    warnings.warn(
                        "Not all profile columns are set within the given profiles."
                        f" Please check if there is a timeseries for every Component in "
                        f"{mapper.process_name}"
                    )
                    new_foreign_keys.append(
                        {
                            "fields": field.name,
                            "reference": {
                                "resource": f"{mapper.process_name}_sequence"
                            },
                        }
                    )
                else:
                    # The Field is allowed to be a timeseries
                    # -> and likely is a supposed to be a timeseries
                    # but a scalar or `unused` is found.
                    pass
        return new_foreign_keys

    @staticmethod
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
                return sequence
            else:
                pass
        return pd.DataFrame()

    def save_datapackage_to_csv(self, destination: str) -> None:
        """
        Saving the datapackage to a given destination in oemof.tabular readable format

        Parameters
        ----------
        self: DataPackage
            DataPackage to save
        destination: str
            String to where the datapackage save to. More convenient to use os.path.
            If last level of folder stucture does not exist, it will be created
            (as well as /elements and /sequences)

        Returns
        -------
        None if the Datapackage has been saved correctly (no checks implemented)

        """
        # Check if filestructure is existent. Create folders if not:
        elements_path = os.path.join(destination, "data", "elements")
        sequences_path = os.path.join(destination, "data", "sequences")
        periods_path = os.path.join(destination, "data", "periods")

        os.makedirs(elements_path, exist_ok=True)
        os.makedirs(sequences_path, exist_ok=True)
        os.makedirs(periods_path, exist_ok=True)

        if not self.periods.empty:
            self.periods.to_csv(
                os.path.join(
                    periods_path,
                    "periods.csv",
                ),
                index=True,
                sep=";",
            )

        # Save elements to elements folder named by keys + .csv
        for process_name, process_adapted_data in self.parametrized_elements.items():
            process_adapted_data.to_csv(
                os.path.join(elements_path, f"{process_name}.csv"),
                index=False,
                sep=";",
            )

        # Save Sequences to sequence folder named as keys + _sequence.csv
        for process_name, process_adapted_data in self.parametrized_sequences.items():
            process_adapted_data.to_csv(
                os.path.join(sequences_path, f"{process_name}_sequence.csv"),
                sep=";",
                index_label="timeindex",
                date_format="%Y-%m-%dT%H:%M:%SZ",
            )

        # From saved elements and keys create a Package
        package = Package(base_path=destination)
        package.infer(pattern="**/*.csv")

        # Add foreign keys from self to Package
        for resource in package.descriptor["resources"]:
            field_names = [field["name"] for field in resource["schema"]["fields"]]
            if resource["name"] in self.foreign_keys.keys():
                resource["schema"].update(
                    {"foreignKeys": self.foreign_keys[resource["name"]]}
                )
            else:
                resource["schema"].update({"foreignKeys": []})
            if "name" in field_names:
                resource["schema"].update({"primaryKey": "name"})
            else:
                warnings.warn("Primary keys differing from `name` not implemented yet")

        # re-initialize Package with added foreign keys and save datapackage.json
        Package(package.descriptor).save(os.path.join(destination, "datapackage.json"))

        return None

    @staticmethod
    def yearly_scalars_to_periodic_values(scalar_dataframe) -> None:
        """
        Turns yearly scalar values to periodic values

        First searches for the sequence length which is the length of the complete sequence.

        Then iterates for every element in parametrized elements, groups them for name
        then applies aggregation method

        Returns None
        -------

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
        return scalar_dataframe

    @classmethod
    def build_datapackage(cls, adapter: Adapter):
        """
        Creating a Datapackage from the oemof_data_adapter that fits oemof.tabular Datapackages.

        Parameters
        ----------
        adapter: Adapter
            Adapter from oemof_data_adapter that is able to handle parameter model data
            from Databus. Adapter needs to be initialized with `structure_name`. Use `links_

        Returns
        -------
        DataPackage

        """
        es_structure = adapter.get_structure()
        parametrized_elements = {"bus": []}
        parametrized_sequences = {}
        foreign_keys = {}
        # Iterate Elements
        for process_name, struct in es_structure.items():
            process_data = adapter.get_process(process_name)
            timeseries = process_data.timeseries
            if isinstance(timeseries.columns, pd.MultiIndex):
                # FIXME: Will Regions be lists of strings or strings?
                timeseries.columns = (
                    timeseries.columns.get_level_values(0)
                    + "_"
                    + [x[0] for x in timeseries.columns.get_level_values(1).values]
                )
            facade_adapter_name: str = PROCESS_TYPE_MAP[process_name]
            facade_adapter = FACADE_ADAPTERS[facade_adapter_name]
            components = []
            process_busses = []
            process_scalars = cls.yearly_scalars_to_periodic_values(
                process_data.scalars
            )
            # Build class from adapter with Mapper and add up for each component within the Element
            for component_data in process_scalars.to_dict(orient="records"):
                component_mapper = Mapper(
                    adapter=facade_adapter,
                    process_name=process_name,
                    data=component_data,
                    timeseries=timeseries,
                )
                component = facade_adapter.parametrize_dataclass(
                    struct, component_mapper
                )
                components.append(component.as_dict())
                # Fill with all busses occurring, needed for foreign keys as well!
                process_busses += list(component_mapper.get_busses(struct).values())

            process_busses = list(pd.unique(process_busses))
            parametrized_elements["bus"] += process_busses

            # getting foreign keys with last component
            #   foreign keys have to be equal for every component within a Process
            #   as foreign key columns cannot have mixed meaning
            foreign_keys[process_name] = cls.get_foreign_keys(
                struct, component_mapper, components
            )

            parametrized_elements[process_name] = pd.DataFrame(components)

            parametrized_sequences = {process_name: timeseries}
        # Create Bus Element from all unique `busses` found in elements
        parametrized_elements["bus"] = pd.DataFrame(
            {
                "name": (names := pd.unique(parametrized_elements["bus"])),
                "type": ["bus" for i in names],
                "balanced": [True for i in names],
            }
        )
        periods = cls.get_periods_from_parametrized_sequences(parametrized_sequences)

        return cls(
            parametrized_elements=parametrized_elements,
            parametrized_sequences=parametrized_sequences,
            adapter=adapter,
            foreign_keys=foreign_keys,
            periods=periods,
        )
