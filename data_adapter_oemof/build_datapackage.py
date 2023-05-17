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
    for (start, end, freq, region), df in timeseries.groupby(
        ["timeindex_start", "timeindex_stop", "timeindex_resolution", "region"]
    ):
        timeindex = pd.date_range(start=start, end=end, freq=pd.Timedelta(freq))

        # Get column names of timeseries only
        ts_columns = set(df.columns).difference(core.TIMESERIES_COLUMNS.keys())

        # Iterate over columns in case there are multiple timeseries
        for profile_name in ts_columns:
            profile_column = df[profile_name].dropna()
            profile_column = profile_column.explode().to_frame()
            profile_column["timeindex"] = profile_column.groupby(level=0).cumcount()
            profile_column = profile_column.reset_index().pivot(
                index="timeindex", columns="index", values=profile_name
            )

            # Rename columns to regions, each region should have its own row

            profile_column.columns = [profile_name + "_" + region]

            # Add multiindex level to column with tech name
            # TODO column_name == profile_name should come from links.csv needs to be changed
            #   @Julian, I think we should get it from structure.csv. Links.csv will always be the same as the column
            #   name since that is how the Adapter.get_process is getting the column for us
            # profile_column.columns = pd.MultiIndex.from_product(
            #     [[profile_name], profile_column.columns]
            # ).to_flat_index()

            # Add timeindex as index
            profile_column.index = timeindex

            # Append to timeseries DataFrame
            df_timeseries = pd.concat([df_timeseries, profile_column], axis=1)

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
            facade_adapter_name: str = PROCESS_TYPE_MAP[process_name]
            facade_adapter = TYPE_MAP[facade_adapter_name]

            components = []
            for component_data in process_data.scalars.to_dict(orient="records"):
                component = facade_adapter.parametrize_dataclass(component_data, struct)
                components.append(component.as_dict())

            scalars = pd.DataFrame(components)
            if "profiles" in facade_adapter.__dict__:
                if len(facade_adapter.profiles) != 1:
                    warnings.warn(
                        message="Functionality to use more than one timeseries per process is not "
                        "implemented yet"
                    )

                else:  # only one timeseries is implemented yet
                    if not process_data.timeseries.empty:
                        # Creating Sequences Dataframes
                        if facade_adapter_name not in parametrized_sequences.keys():
                            parametrized_sequences[
                                facade_adapter_name
                            ] = refactor_timeseries(timeseries=process_data.timeseries)

                        else:
                            parametrized_sequences[facade_adapter_name].update(
                                refactor_timeseries(timeseries=process_data.timeseries)
                            )

                        # Adding profile column and naming to scalars:
                        # Tabular will search for this
                        column_name = facade_adapter.profiles[0]

                        scalars[column_name] = (
                            process_data.timeseries.columns.difference(
                                core.TIMESERIES_COLUMNS.keys()
                            )[0]
                            + "_"
                            + scalars["region"]
                        )
                    else:
                        warnings.warn(
                            message=f"Please include a timeseries for facade adapter {facade_adapter} "
                            f"for process {process_name}"
                            f"Or adapt links (see `get_process`) to include timeseries for this process"
                        )

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
