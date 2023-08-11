import dataclasses
import warnings

import numpy as np
import pandas as pd
import os

from datapackage import Package
from typing import Union

from data_adapter import core
from data_adapter.preprocessing import Adapter
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
        timeseries in parameter-model format (https://github.com/sedos-project/oedatamodel#oedatamodel-parameter)

    Returns
    pd.DataFrame Tabular form of timeseries for multiple periods of similar
    technologies and regions.
    -------

    """
    # Combine all time series into one DataFrame
    df_timeseries = pd.DataFrame()
    timeseries_timesteps = []
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
            for one element of the Process (foreign keys have to be equal for all components of a Process)
        components: list
            all components as of a Process as dicts. Helps to check what columns that could be pointing to sequences
            are found in Sequences.

        Returns
        -------
        list of foreignKeys for Process including bus references and pointers to files containing `profiles`
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
                    warnings.warn(
                        "Not all profile columns are set within the given profiles."
                        f" Please check if there is a timeseries for every Component in {mapper.process_name}"
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
                    # Most likely the field may be a Timeseries in this case, but it is a scalar or unused.
                    pass
        return new_foreign_keys

    def save_datapackage_to_csv(self, destination: str) -> None:
        """
        Saving the datapackage to a given destination in oemof.tabular readable format

        Parameters
        ----------
        self: DataPackage
            DataPackage to save
        destination: str
            String to where the datapackage save to. More convenient to use os.path. If last level of folder stucture
            does not exist, it will be created (as well as /elements and /sequences)

        Returns
        -------
        None if the Datapackage has been saved correctly (no checks implemented)

        """
        # Check if filestructure is existent. Create folders if not:
        elements_path = os.path.join(destination, "data", "elements")
        sequences_path = os.path.join(destination, "data", "sequences")

        os.makedirs(elements_path, exist_ok=True)
        os.makedirs(sequences_path, exist_ok=True)

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

    # Define a function to aggregate differing values into a list
    def _listify_to_periodic(
        self, group_df: pd.DataFrame, element_name: str, sequence_length: int
    ) -> pd.Series:
        """
        Meant to be called from "yearly_scalars_to_periodic_values"

        Aggregation is happening as follows:
        1: If the Entries are dicts (like input_parameter) it will take the first entry and leave it unchanged
        2: Elif the Entries are lists or sequences it will check whether they are sequences for the full period
            and write them to sequences, update foreign keys and write the foreign key.
        3: Elif the entries are all equal the first entry will be written
        4: Elif the entries are not equal they are written as {"len":sequence_length, "values": [1, 2, 3]}

        Parameters
        ----------
        group_df
        element_name
        sequence_length

        Returns
        -------

        """
        unique_values = pd.Series()
        for col in group_df.columns:  # Exclude 'name' column
            if isinstance(group_df[col][group_df.index[0]], dict):
                # Unique input/output parameters are not allowed per period
                unique_values[col] = group_df[col][group_df.index[0]]
                continue
            # Full Timeseries shall be moved to sequence folder and read via foreign key:
            elif all(
                [
                    isinstance(col_entry, Union[list, pd.Series])
                    for col_entry in group_df[col]
                ]
            ):
                if (
                    sum([len(col_entry) for col_entry in group_df[col]])
                    == sequence_length
                ):
                    if element_name in self.parametrized_sequences.keys():
                        self.parametrized_sequences[element_name][group_df.name] = [
                            item for sublist in group_df[col] for item in sublist
                        ]
                    else:
                        self.parametrized_sequences[element_name] = pd.DataFrame(
                            [item for sublist in group_df[col] for item in sublist],
                            columns=[group_df.name],
                        )
                    self.foreign_keys[element_name].append(
                        {
                            "fields": col,
                            "reference": {"resource": f"{element_name}_sequence"},
                        }
                    )
                    unique_values[col] = group_df.name
                    continue
                else:
                    warnings.warn(
                        "You provided lists in scalar Data that is not matching to the Timeseries"
                    )
                    unique_values[col] = group_df[col][group_df.index[0]]
                    continue

            values = group_df[col].unique()
            if len(values) > 1:
                unique_values[col] = {"len": sequence_length, "values":  list(group_df[col])}
            else:
                unique_values[col] = values[0]
        unique_values["name"] = group_df.name
        return unique_values

    def yearly_scalars_to_periodic_values(self) -> None:
        """
        Turns yearly sclar values to periodic values

        First searches for the sequence length which is the length of the complete sequence.

        Then iterates for every element in parametrized elements, groups them for name and applies aggregation method

        Returns None
        -------

        """
        # Check integrity if all indexes in sequences are equal:
        if all(
            [
                df.index.equals(next(iter(self.parametrized_sequences.values())).index)
                for df in self.parametrized_sequences.values()
            ]
        ):
            sequence_length = [
                len(sequence) for sequence in self.parametrized_sequences.values()
            ]
            if len(np.unique((sequence_length))) != 1:
                raise Exception(
                    "All provided Sequences should have the same total length"
                )
            else:
                warnings.warn("Provided Sequences are not equal but of same length")
            sequence_length = sequence_length[0]

        for element_name, element in self.parametrized_elements.items():
            if "year" in element.columns:
                element.sort_values(by="year", ascending=True, inplace=True)

                self.parametrized_elements[element_name] = (
                    element.groupby("name")
                    .apply(self._listify_to_periodic, *[element_name, sequence_length])
                    .reset_index(drop=True)
                )
        return None

    @classmethod
    def build_datapackage(cls, adapter: Adapter):
        """
        Creating a Datapackage from the oemof_data_adapter that fits oemof.tabular Datapackages.

        Parameters
        ----------
        adapter: Adapter
            Adapter from oemof_data_adapter that is able to handle parameter model data from Databus.
            Adapter needs to be initialized with `structure_name`. Use `links_

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
                #FIXME: Will Regions be lists of strings or stings?
                timeseries.columns = timeseries.columns.get_level_values(0)+"_"+[x[0] for x in timeseries.columns.get_level_values(1).values]
            facade_adapter_name: str = PROCESS_TYPE_MAP[process_name]
            facade_adapter = FACADE_ADAPTERS[facade_adapter_name]
            components = []
            process_busses = []
            # Build class from adapter with Mapper and add up for each component within the Element
            for component_data in process_data.scalars.to_dict(orient="records"):
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
            #   foreign keys have to be equal for every component within a Process as foreign key columns cannot
            #   have mixed meaning
            foreign_keys[process_name] = cls.get_foreign_keys(
                struct, component_mapper, components
            )

            parametrized_elements[process_name] = pd.DataFrame(components)

            parametrized_sequences = {process_name: timeseries}
        parametrized_elements["bus"] = pd.DataFrame(
            {
                "name": (names := pd.unique(parametrized_elements["bus"])),
                "type": ["bus" for i in names],
                "balanced": [True for i in names],
            }
        )

        return cls(
            parametrized_elements=parametrized_elements,
            parametrized_sequences=parametrized_sequences,
            adapter=adapter,
            foreign_keys=foreign_keys,
        )
