import dataclasses
import os
from collections import defaultdict

import pandas as pd

import data_adapter.preprocessing as preprocessing
from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP
from oemof.tabular.datapackage.building import infer_metadata


@dataclasses.dataclass
class datapackage:
    data: dict  # datadict with scalar data in form of {type:pd.DataFrame(type)}
    struct: dict
    process_data: dict

    def foreign_keys_dict(self):
        """

        :return: Returns dictionary with foreign keys that is necessary for tabular `infer_metadata`
        """
    @staticmethod
    def __refactor_timeseries(timeseries:pd.DataFrame):
        """
        Takes timeseries in parameter-model format (as a single line entry) And turns into
        Tabular matching format with index as timeseries timestamps and
        first column containing data
        :return: pd.DataFrame:

        """

    @classmethod
    def build_datapackage(cls, es_structure, **process_data: dict[str, preprocessing.Process]):
        """
        Adapting the resource scalars for a Datapackage using adapters from adapters.py

        :param es_structure:
        :param process_data: preprocessing.Process
        :return:
        """
        parametrized_elements = {}
        struct_io: dict
        parametrized_sequences = {}
        # Todo: Schleife ohne zip
        #  Ohne .apply? Siehe test_adapter
        for (process, data), (process, struct_io) in zip(
                process_data.items(), es_structure.items()
        ):
            process_type: str = PROCESS_TYPE_MAP[process]

            adapter = TYPE_MAP[process_type]
            scalars = data.scalars.apply(
                adapter.parametrize_dataclass,
                struct=struct_io,
                process_type=process_type,
                axis=1,
            )
            sequences = data.timeseries
            if process_type in parametrized_elements.keys():
                parametrized_elements[process_type] = pd.concat(
                    [
                        pd.DataFrame([param.as_dict() for param in scalars.values]),
                        parametrized_elements[process_type],
                    ],
                    ignore_index=True,
                )
            else:
                parametrized_elements[process_type] = pd.DataFrame(
                    [param.as_dict() for param in scalars.values]
                )
        return cls(data=parametrized_elements, struct=es_structure, process_data=process_data)



    def get_foreign_keys(self):
        pass

    def save_datapackage_to_csv(self, destination):
        for key, value in datapackage.items():
            file_path = os.path.join(destination, key + ".csv")
            # FIXME droping na only for tests!
            value = value.dropna(axis="columns")
            value.to_csv(file_path, sep=";", index=False)
