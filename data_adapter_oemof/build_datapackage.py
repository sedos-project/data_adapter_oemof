import dataclasses
import warnings

import pandas as pd

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

    :return: pd.DataFrame:
        Tabular form of timeseries for multiple periods of similar
        technologies and regions.
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
    parametrized_elements: dict  # datadict with scalar data in form of {type:pd.DataFrame(type)}
    parametrized_sequences: dict  # timeseries in form of {type:pd.DataFrame(type)}
    foreign_keys: dict  # foreign keys for timeseries profiles
    adapter: Adapter

    def foreign_keys_dict(self):
        """

        :return: Returns dictionary with foreign keys that is necessary for tabular `infer_metadata`
        """
        pass

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
    def get_foreign_keys(
        process_busses: list, mapper: Mapper, components: list
    ) -> list:
        """
        Writes Foreign keys for one process.
        Searches in adapter class for sequences fields
        :param components:
        :rtype: list
        :param process_busses:
        :param adapter:
        :return:
        """
        new_foreign_keys = []
        components = pd.DataFrame(components)
        for bus in process_busses:
            new_foreign_keys.append(
                {"fields": bus, "reference": {"fields": "name", "resource": "bus"}}
            )

        for field in dataclasses.fields(mapper.adapter):
            if mapper.is_sequence(field.type):
                if all(components[field.name].isin(mapper.timeseries.columns)):
                    new_foreign_keys.append(
                        {
                            "fields": field.name,
                            "reference": {
                                "resource": f"{mapper.process_name}_sequence"
                            },
                        }
                    )
        return new_foreign_keys

    def save_datapackage_to_csv(self, destination):
        """
        Saving the datapackage to a given destination in oemof.tabular readable format
        :param destination:
        :return:
        """

    @classmethod
    def build_datapackage(cls, adapter: Adapter):
        """
        Adapting the resource scalars for a Datapackage using adapters from
        adapters.py
        :type adapter: Adapter class from preprocessing
        :return: Instance of datapackage class

        """
        es_structure = adapter.get_structure()
        parametrized_elements = {"bus": []}
        parametrized_sequences = {}
        foreign_keys = {}
        # Iterate Elements
        for process_name, struct in es_structure.items():
            process_data = adapter.get_process(process_name)
            timeseries = refactor_timeseries(process_data.timeseries)
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

            process_busses = pd.unique(process_busses)
            parametrized_elements["bus"] += process_busses

            # getting foreign keys with last component (foreign keys have to be equal for every component within
            # a Process
            foreign_keys[process_name] = cls.get_foreign_keys(
                process_busses, component_mapper, components
            )

            parametrized_elements[process_name] = pd.DataFrame(components)

            parametrized_sequences = {process_name: timeseries}
        parametrized_elements["bus"] = pd.DataFrame(
            {
                "name": (names := pd.unique(parametrized_elements["bus"])),
                "type": ["bus" for i in names],
                "blanced": [True for i in names],
            }
        )
        return cls(
            parametrized_elements=parametrized_elements,
            parametrized_sequences=parametrized_sequences,
            adapter=adapter,
            foreign_keys=foreign_keys,
        )
