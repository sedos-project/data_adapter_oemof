import dataclasses
import os
import warnings
from collections import defaultdict

import pandas as pd

from data_adapter import preprocessing, core
from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP, GLOBAL_PARAMETER_MAP
from oemof.tabular.datapackage.building import infer_metadata

def refactor_timeseries(timeseries: pd.DataFrame):
    """
    Takes timeseries in parameter-model format (as a single line entry) And
    turns into Tabular matching format with index as timeseries timestamps
    and columns containing data

    :return: pd.DataFrame:

    """

    columns = [col for col in timeseries.columns if "timeindex" in col]

    # Combine all time series into one DataFrame
    df_timeseries = pd.DataFrame()
    for (start, end, freq), df in timeseries.groupby(columns):
        timeindex = pd.date_range(start=start, end=end, freq=pd.Timedelta(freq))

        # Get column names of timeseries only
        ts_columns = set(df.columns).difference(core.TIMESERIES_COLUMNS.keys())

        # Iterate over columns in case there are multiple timeseries
        for profile_name in ts_columns:
            profile_column = df[profile_name].dropna()
            profile_column = profile_column.explode().to_frame()
            profile_column["timeindex"] = profile_column.groupby(
                level=0
            ).cumcount()
            profile_column = profile_column.reset_index().pivot(
                index="timeindex", columns="index", values=profile_name
            )

            # Rename columns to regions, each region should have its own row
            profile_column.columns = df["region"].to_dict().values()

            # Add multindex level to column with tech name
            # TODO column_name == profile_name should come from links.csv needs to be changed
            profile_column.columns = pd.MultiIndex.from_product(
                [[profile_name], profile_column.columns]
            )

            # Add timeindex as index
            profile_column.index = timeindex

            # Append to timeseries DataFrame
            df_timeseries = pd.concat([df_timeseries, profile_column], axis=1)

    return df_timeseries

    # name of csv file
    # TODO change facade_adapter to facade_type?!
    facade_year = f"{facade_adapter}_{year}"
    # check if facade_adapter already exists
    if facade_year in parametrized_sequences.keys():
        # concat processes to existing
        parametrized_sequences[facade_year] = pd.concat(
            [parametrized_sequences[facade_year], df_timeseries_year],
            axis=1,
        )
    else:
        # add new facade_adapter/facade
        parametrized_sequences[facade_year] = df_timeseries_year


@dataclasses.dataclass
class datapackage:
    parametrized_elements: dict  # datadict with scalar data in form of {type:pd.DataFrame(type)}
    parametrized_sequences: dict  # timeseries in form of {type:pd.DataFrame(type)}
    foreign_keys: dict  # foreign keys for timeseries profiles
    struct: dict
    process_data: dict

    def foreign_keys_dict(self):
        """

        :return: Returns dictionary with foreign keys that is necessary for tabular `infer_metadata`
        """

    @classmethod
    def build_datapackage(
        cls, es_structure, **process_data: dict[str, preprocessing.Process]
    ):
        """
        Adapting the resource scalars for a Datapackage using adapters from
        adapters.py

        :param es_structure:
        :param process_data: preprocessing.Process
        :return:
        """
        parametrized_elements = {}
        struct_io: dict
        parametrized_sequences = {}
        foreign_keys = {}
        # Todo: Schleife ohne zip bzw. Zip verkürzen
        #  Ohne .apply? Siehe test_data_adapter.test_adapter
        for (process, data), (process, struct_io) in zip(
            process_data.items(), es_structure.items()
        ):
            facade_adapter: str = PROCESS_TYPE_MAP[process]

            adapter = TYPE_MAP[facade_adapter]
            scalars = data.scalars.apply(
                func=adapter.parametrize_dataclass,
                struct=struct_io,
                axis=1,
            )

            scalars = pd.DataFrame(scalars.to_list())
            if "profiles" in adapter.__dict__:
                if not data.timeseries.empty:
                    if not facade_adapter in parametrized_sequences.keys():
                        parametrized_sequences[facade_adapter] = refactor_timeseries(timeseries=data.timeseries)
                    else:
                        parametrized_sequences[facade_adapter].update(refactor_timeseries(timeseries=data.timeseries))
                else:
                    warnings.warn(message=f"Please include a timeseries for facade adapter {adapter} for process {process}"
                                          f"Or adapt links (see `get_process`) to include timeseries for this process")
                if len(adapter.profiles) == 1:
                    scalars[adapter.profiles[0]]
                else:
                    warnings.warn(message="Functionality to use more than one timeseries per process is not "
                                          "implemented yet")

            ts_columns = set(data.timeseries.columns).difference(core.TIMESERIES_COLUMNS.keys())

            # check if facade_adapter already exists
            if facade_adapter in parametrized_elements.keys():
                # concat processes to existing
                parametrized_elements[facade_adapter] = pd.concat(
                    [parametrized_elements[facade_adapter], scalars],
                    axis=0,
                    ignore_index=True,
                )
            else:
                # add new facade_adapter
                parametrized_elements[facade_adapter] = scalars



        return cls(
            parametrized_elements=parametrized_elements,
            parametrized_sequences=parametrized_sequences,
            process_data=process_data,
            foreign_keys=foreign_keys,
            struct=es_structure,
        )

    def get_foreign_keys(self):
        pass

    def save_datapackage_to_csv(self, destination):
        for key, value in datapackage.items():
            file_path = os.path.join(destination, key + ".csv")
            value = value.dropna(axis="columns")
            value.to_csv(file_path, sep=";", index=False)


