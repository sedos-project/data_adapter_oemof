import dataclasses
import os
import warnings
from typing import Optional, Type

import numpy as np
import pandas as pd
import tsam.timeseriesaggregation as tsam
from data_adapter.preprocessing import Adapter
from datapackage import Package

from data_adapter_oemof.adapters import FACADE_ADAPTERS
from data_adapter_oemof.adapters import Adapter as FacadeAdapter
from data_adapter_oemof.calculations import handle_nans
from data_adapter_oemof.settings import BUS_MAP, PARAMETER_MAP, PROCESS_ADAPTER_MAP
from data_adapter_oemof.utils import convert_mixed_types_to_same_length


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
    group_df = handle_nans(group_df)
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
            values = group_df[col].unique()
        if len(values) > 1:
            if isinstance(group_df[col].iloc[0], list):
                unique_values[col] = list(group_df[col].apply(lambda x: x[0]))
            else:
                unique_values[col] = list(group_df[col])
        else:
            if isinstance(group_df[col].iloc[0], list):
                unique_values[col] = group_df[col].iat[0][0]
            else:
                unique_values[col] = group_df[col].iat[0]
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
    location_to_save_to: str = None
    tsa_parameters: pd.DataFrame = None

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
    def get_foreign_keys(facade_adapter: FacadeAdapter, components: list) -> list:
        """
        Writes Foreign keys for one process.
        Searches in adapter class for sequences fields

        Parameters
        ----------
        facade_adapter: FacadeAdapter
            Adapter related to current process
        components: list
            all components as of a Process as dicts. Helps to check what columns
            that could be pointing to sequences are found in Sequences.

        Returns
        -------
        list of foreignKeys for Process including bus references and pointers to files
        containing `profiles`
        """
        if facade_adapter is None:
            return []
        new_foreign_keys = []
        components = pd.DataFrame(components)
        for bus in facade_adapter.get_busses().keys():
            new_foreign_keys.append(
                {"fields": bus, "reference": {"fields": "name", "resource": "bus"}}
            )

        for field in facade_adapter.get_fields():
            if (
                facade_adapter.is_sequence(field.type)
                and field.name in components.columns
                and pd.api.types.infer_dtype(components[field.name]) == "string"
            ):
                if all(components[field.name].isin(facade_adapter.timeseries.columns)):
                    new_foreign_keys.append(
                        {
                            "fields": field.name,
                            "reference": {
                                "resource": f"{facade_adapter.process_name}_sequence"
                            },
                        }
                    )
                elif any(
                    components[field.name].isin(facade_adapter.timeseries.columns)
                ):
                    # Todo clean up on examples:
                    #   -remove DE from hackerthon or
                    #   -create propper example with realistic project data
                    warnings.warn(
                        "Not all profile columns are set within the given profiles."
                        f" Please check if there is a timeseries for every Component in "
                        f"{facade_adapter.process_name}"
                    )
                    new_foreign_keys.append(
                        {
                            "fields": field.name,
                            "reference": {
                                "resource": f"{facade_adapter.process_name}_sequence"
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
                # create mapping to count up unique years
                year_mapping = {
                    year: i for i, year in enumerate(sequence.index.year.unique())
                }
                # Map unique years to sequence instead of using groupby
                sequence["periods"] = sequence.index.year.map(year_mapping)
                sequence["timeincrement"] = 1
                sequence.index.name = "timeindex"
                return sequence
            else:
                pass
        return pd.DataFrame()

    def save_datapackage_to_csv(
        self,
        location_to_save_to: str = None,
        datapackage_name: str = "datapackage.json",
    ) -> None:
        """
        Saving the datapackage to a given destination in oemof.tabular readable format
        Using frictionless datapckage module

        Parameters
        ----------
        self: DataPackage
            DataPackage to save
        location_to_save_to: str
            String to where the datapackage save to. More convenient to use os.path.
            If last level of folder stucture does not exist, it will be created
            (as well as /elements and /sequences)

        Returns
        -------
        None if the Datapackage has been saved correctly (no checks implemented)

        """

        # check if datapackage already has defined its destination
        if location_to_save_to:
            pass
        elif self.location_to_save_to:
            location_to_save_to = self.location_to_save_to
        else:
            raise ValueError(
                "Please state location_to_save_to either in datapackage or saving call"
            )

        # Check if filestructure is existent. Create folders if not:
        elements_path = os.path.join(location_to_save_to, "data", "elements")
        sequences_path = os.path.join(location_to_save_to, "data", "sequences")
        periods_path = os.path.join(location_to_save_to, "data", "periods")
        tsam_path = os.path.join(location_to_save_to, "data", "tsam")

        os.makedirs(elements_path, exist_ok=True)
        os.makedirs(sequences_path, exist_ok=True)
        os.makedirs(periods_path, exist_ok=True)
        os.makedirs(tsam_path, exist_ok=True)

        if not self.periods.empty:
            self.periods.to_csv(
                os.path.join(
                    periods_path,
                    "periods.csv",
                ),
                index=True,
                sep=";",
            )
        if self.tsa_parameters is not None:
            if "timeindex" in self.tsa_parameters:
                self.tsa_parameters.drop("timeindex", inplace=True, axis=1)
            self.tsa_parameters.to_csv(
                os.path.join(tsam_path, "tsa_parameters.csv"), sep=";"
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
        package = Package(base_path=location_to_save_to)
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

            elif (
                "sequence" in resource["name"].split("_")
                or resource["name"] == "periods"
            ):
                pass
            else:
                warnings.warn(
                    "Primary keys differing from `name` not implemented yet."
                    f"Check primary Keys for resource {resource['name']}"
                )

        # re-initialize Package with added foreign keys and save datapackage.json
        Package(package.descriptor).save(
            os.path.join(location_to_save_to, datapackage_name)
        )

        return None

    @staticmethod
    def yearly_scalars_to_periodic_values(scalar_dataframe) -> pd.DataFrame:
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

    def time_series_aggregation(
        self, tsam_config: str, location_to_save_to: str = None
    ):
        """
        Aggregates time series in datapackage and saves the new datapackage with updated
        (sequence)Resources as well as aggregated sequence resources.
        Parameters
        ----------
        tsam_config
        destination

        Returns
        -------

        """
        # Refactor sequences into one Dataframe
        sequences = pd.concat(
            self.parametrized_sequences.values(),
            axis=1,
            keys=self.parametrized_sequences.keys(),
        )
        # Setting sequence indices to be same as periods
        sequences.index = self.periods.index
        # Group sequences by Periods
        tsam_aggregated_typical_periods = []
        tsa_parameters = []
        for period in pd.unique(self.periods["periods"]):
            # Saving the old Index to have it for later periods creation
            index_old = self.periods.index[self.periods["periods"] == period]
            period_sequence = sequences.loc[index_old]
            # Aggregate
            aggregation = tsam.TimeSeriesAggregation(
                period_sequence, **tsam_config[period]
            )
            tsa_parameters.append(
                {
                    "timesteps_per_period": aggregation.hoursPerPeriod,
                    "order": aggregation.clusterOrder,
                    "timeindex": aggregation.timeIndex,
                }
            )
            aggregation = aggregation.createTypicalPeriods()
            # Use old Index with as many as needed entries
            aggregation.index = index_old[: len(aggregation)]
            tsam_aggregated_typical_periods.append(aggregation)
        # Aggregate split periods back together again
        tsam_aggregated_typical_periods = pd.concat(
            tsam_aggregated_typical_periods, ignore_index=False
        )
        tsa_parameters = pd.DataFrame(tsa_parameters)
        tsa_parameters.index = list(pd.unique(self.periods["periods"]))
        tsa_parameters.index.name = "periods"
        self.tsa_parameters = tsa_parameters

        # Rewrite the aggregated sequences to datapackage sequences
        for (
            sequence_file_name
        ) in tsam_aggregated_typical_periods.columns.get_level_values(level=0).unique():
            # Get all sequences that belong in one sheet together
            sequence = tsam_aggregated_typical_periods.loc[
                :,
                tsam_aggregated_typical_periods.columns.get_level_values(level=0)
                == sequence_file_name,
            ]
            # Remove disturbing Multiindex again
            sequence.columns = sequence.columns.droplevel()
            self.parametrized_sequences[sequence_file_name] = sequence
        # Recreate Periods
        self.periods = self.get_periods_from_parametrized_sequences(
            self.parametrized_sequences
        )
        # Save with newly introduced tsam trigger
        self.save_datapackage_to_csv(location_to_save_to=location_to_save_to)

    @classmethod
    def build_datapackage(
        cls,
        adapter: Adapter,
        process_adapter_map: Optional[dict] = PROCESS_ADAPTER_MAP,
        parameter_map: Optional[dict] = PARAMETER_MAP,
        bus_map: Optional[dict] = BUS_MAP,
        location_to_save_to: str = None,
    ):
        """
        Creating a Datapackage from the oemof_data_adapter that fits oemof.tabular Datapackages.

        Parameters
        ----------
        adapter: Adapter
            Adapter from data_adapter that is able to handle parameter model data
            from Databus. Adapter needs to be initialized with `structure_name`.
            Use `links` to add data from different processes to each other.
            Use `structure` to map busses to "processes" and "Adapters"
        process_adapter_map
            Maps process names to adapter names, if not set default mapping is used
        parameter_map
            Maps parameter names from adapter to facade, if not set default mapping is used.
            Make sure to map "sequence" entries on "sequence profile names" (see example)
        bus_map
            Maps facade bus names to adapter bus names, if not set default mapping is used

        Returns
        -------
        DataPackage

        """

        def _reduce_lists(x):
            """Unnest list of single tuple or list of single list"""
            if isinstance(x[0], (list, tuple)) and len(x[0]) == 1:
                x = x.map(lambda x: x[0])
            return x

        parametrized_elements = {"bus": []}
        parametrized_sequences = {}
        foreign_keys = {}
        # Iterate Elements
        for process_name, struct in adapter.structure.processes.items():
            process_data = adapter.get_process(process_name)
            timeseries = process_data.timeseries
            if isinstance(timeseries.columns, pd.MultiIndex):
                timeseries.columns = (
                    _reduce_lists(timeseries.columns.get_level_values(0))
                    + "_"
                    + _reduce_lists(timeseries.columns.get_level_values(1))
                )
            facade_adapter_name: str = process_adapter_map[process_name]
            facade_adapter: Type[FacadeAdapter] = FACADE_ADAPTERS[facade_adapter_name]
            component_adapter: Optional[FacadeAdapter] = None
            components = []
            process_busses = []
            process_scalars = cls.yearly_scalars_to_periodic_values(
                process_data.scalars
            )
            # Build class from adapter with Mapper and add up for each component within the Element
            for component_data in process_scalars.to_dict(orient="records"):
                component_adapter: FacadeAdapter = facade_adapter(
                    process_name=process_name,
                    data=component_data,
                    timeseries=timeseries,
                    structure=struct,
                    parameter_map=parameter_map,
                    bus_map=bus_map,
                )
                components.append(component_adapter.facade_dict)
                # Fill with all buses occurring, needed for foreign keys as well!
                process_busses += list(component_adapter.get_busses().values())

            process_busses = list(np.unique(process_busses))
            parametrized_elements["bus"] += process_busses

            # getting foreign keys with last component
            # foreign keys have to be equal for every component within a Process
            # as foreign key columns cannot have mixed meaning.
            # thus reading foreign keys only from last facade adapter is sufficient.
            foreign_keys[process_name] = cls.get_foreign_keys(
                component_adapter, components
            )

            parametrized_elements[process_name] = pd.DataFrame(components)
            if not timeseries.empty:
                parametrized_sequences.update({process_name: timeseries})
        # Create Bus Element from all unique `busses` found in elements
        parametrized_elements["bus"] = pd.DataFrame(
            {
                "name": (names := np.unique(parametrized_elements["bus"])),
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
            location_to_save_to=location_to_save_to,
        )
