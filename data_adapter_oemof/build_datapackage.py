import dataclasses
import warnings

import pandas as pd

from data_adapter import core
from data_adapter.preprocessing import Adapter
from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP


def refactor_timeseries(timeseries: pd.DataFrame):
    """
    Takes timeseries in parameter-model format (as a single line entry) And
    turns into Tabular matching format with index as timeseries timestamps
    and columns containing data

    :return: pd.DataFrame:

    """

    # Combine all time series into one DataFrame
    df_timeseries = pd.DataFrame()
    timeseries_timesteps = []
    if timeseries.empty:
        return df_timeseries
    for (start, end, freq), df in timeseries.groupby(
        ["timeindex_start", "timeindex_stop", "timeindex_resolution"]
    ):
        # Get column names of timeseries only
        ts_columns = set(df.columns).difference(core.TIMESERIES_COLUMNS.keys())

        # Iterate over columns in case there are multiple timeseries
        profiles = []
        for profile_name in ts_columns:
            profile_column = df[["region", profile_name]].explode(profile_name)

            # Creating cumcount index for Regions:
            profile_column["index"] = profile_column.groupby("region").cumcount()
            profile_column_pivot = pd.pivot_table(profile_column, values=profile_name, index=["index"], columns=["region"])
            profile_column_pivot.reset_index(drop=True)
            # Rename the columns
            profile_column_pivot.columns = [f"{profile_name}_{col}" for col in profile_column_pivot.columns]

            # Reset the index
            profiles.append(profile_column_pivot)

        df_timeseries = pd.concat(profiles, axis=1)
        timeindex = pd.date_range(start=start, end=end, freq=pd.Timedelta(freq))
        df_timeseries.index = timeindex
        timeseries_timesteps.append(df_timeseries)

        # TODO: Regions have not be filtered in group by, but instead build to columns
        # TODO: Timeseries with different timeindex have to appended with axis=0
    df_timeseries = pd.concat(timeseries_timesteps, axis = 0)
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
        for (sequence_name, sequence_dataframe) in parametrized_sequences.items():
            # Group the DataFrame by year using pd.Grouper
            grouped = sequence_dataframe.resample("Y")

            # Iterate over the groups and store each year's DataFrame
            for year, group in grouped:
                split_dataframes[sequence_name + "_" + str(year.year)] = group.copy()

        return split_dataframes

    def get_foreign_keys(self):
        pass

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
        parametrized_elements = {}
        parametrized_sequences = {}
        foreign_keys = {}
        for (process_name, struct) in es_structure.items():
            process_data = adapter.get_process(process_name)
            timeseries = refactor_timeseries(process_data.timeseries)
            facade_adapter_name: str = PROCESS_TYPE_MAP[process_name]
            facade_adapter = TYPE_MAP[facade_adapter_name]

            components = []
            for component_data in process_data.scalars.to_dict(orient="records"):
                component = facade_adapter.parametrize_dataclass(component_data, timeseries, struct)
                components.append(component.as_dict())

            scalars = pd.DataFrame(components)

            # check if facade_adapter already exists
            if facade_adapter_name in parametrized_elements.keys():
                # concat processes to existing
                parametrized_elements[facade_adapter_name] = pd.concat(
                    [parametrized_elements[facade_adapter_name], scalars],
                    axis=0,
                    ignore_index=True,
                )
            else:
                # add new facade_adapter
                parametrized_elements[facade_adapter_name] = scalars

            parametrized_sequences = {process_name: timeseries}

        # Splitting timeseries into multiple timeseries.
        # We don't know yet what requirements multiple year optimisation will have.
        parametrized_sequences = cls.__split_timeseries_into_years(
            parametrized_sequences
        )

        return cls(
            parametrized_elements=parametrized_elements,
            parametrized_sequences=parametrized_sequences,
            adapter=adapter,
            foreign_keys=foreign_keys,
        )
