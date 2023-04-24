import dataclasses
import os
from collections import defaultdict

import pandas as pd

from data_adapter import preprocessing, core
from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP, GLOBAL_PARAMETER_MAP
from oemof.tabular.datapackage.building import infer_metadata


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

    @staticmethod
    def __refactor_timeseries(timeseries: pd.DataFrame):
        """
        Takes timeseries in parameter-model format (as a single line entry) And
        turns into Tabular matching format with index as timeseries timestamps
        and columns containing data

        :return: pd.DataFrame:

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
            process_type: str = PROCESS_TYPE_MAP[process]

            adapter = TYPE_MAP[process_type]
            scalars = data.scalars.apply(
                func=adapter.parametrize_dataclass,
                struct=struct_io,
                process_type=process_type,
                axis=1,
            )

            scalars = pd.DataFrame(scalars.to_list())

            # check if process_type already exists
            if process_type in parametrized_elements.keys():
                # concat processes to existing
                parametrized_elements[process_type] = pd.concat(
                    [parametrized_elements[process_type], scalars],
                    axis=0, ignore_index=True,
                )
            else:
                # add new process_type
                parametrized_elements[process_type] = scalars

            if not data.timeseries.empty:


                # def match_sequences_to_scalar():
                # Add profile column to volatile facade
                # match timeseries and row
                # refactor timeseries add name/id
                # place name/id in profile columns of scalar

                columns = [i for i in data.timeseries.columns if 'timeindex'in i]
                # TODO column order might not be correct in groupby? hardcode column names?
                # iterate over different timeindex
                # timeseries with different timeindex needs to be stores in
                # separate csv files
                # naming convention process_year? or process_profile_year?
                # TODO this iteration should be appended to one dataframe to
                #  have all timeseries/periods in one file.
                for (start, end, freq), df in data.timeseries.groupby(columns):
                    timeindex = pd.date_range(start=start,
                                  end=end,
                                  freq=pd.Timedelta(
                                      freq))
                    # get column names of timeseries only
                    ts_column = set(data.timeseries.columns).difference(
                        core.TIMESERIES_COLUMNS.keys())
                    year = pd.to_datetime(start).year

                    df_timeseries_year = pd.DataFrame()
                    # iterate over columns in case there are multiple timeseries
                    # e.g. extraction turbine
                    for profile_name, profile_column in df.loc[:,
                                                        ts_column].items():
                        profile_column = profile_column.dropna()
                        # unnest timeseries
                        profile_column = profile_column.explode()
                        profile_column = profile_column.to_frame()
                        # add hour of year
                        profile_column["hour_of_year"] = profile_column.groupby(
                            axis=0,
                            level=0).cumcount()
                        profile_column = profile_column.reset_index().pivot(
                            columns="index", index="hour_of_year",
                            values="onshore")
                        # rename columns to regions
                        profile_column = profile_column.rename(
                            columns=df["region"].to_dict())
                        # add multindex level to column with tech name
                        # TODO column_name == profile_name should come from
                        #  links.csv needs to be changed
                        profile_column.columns = pd.MultiIndex.from_product(
                            [[profile_name], profile_column.columns])
                        profile_column.index = timeindex
                        # append other timeseries of facade e.g. extraction
                        # turbine has multiple efficiency ts
                        df_timeseries_year = pd.concat(
                            [df_timeseries_year, profile_column],
                            axis=1)

                    # name of csv file
                    # TODO change process_type to facade_type?!
                    facade_year = f"{process_type}_{year}"
                    # check if process_type already exists
                    if facade_year in parametrized_sequences.keys():
                        # concat processes to existing
                        parametrized_sequences[facade_year] = pd.concat(
                            [parametrized_sequences[facade_year], df_timeseries_year],
                            axis=1,
                        )
                    else:
                        # add new process_type/facade
                        parametrized_sequences[facade_year] = df_timeseries_year


        return cls(
            parametrized_elements=parametrized_elements,
            parametrized_sequences=parametrized_sequences,
            process_data=process_data,
            foreign_keys=foreign_keys,
            struct=es_structure
        )

    def get_foreign_keys(self):
        pass

    def save_datapackage_to_csv(self, destination):
        for key, value in datapackage.items():
            file_path = os.path.join(destination, key + ".csv")
            value.to_csv(file_path, sep=";", index=False)
