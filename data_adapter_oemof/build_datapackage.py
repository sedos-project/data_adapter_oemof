import dataclasses
import os
from collections import defaultdict

import pandas as pd

from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP


@dataclasses.dataclass
class datapackage:
    @classmethod
    def build_datapackage(cls, es_structure, **process_data):
        """
        Adapting the resources for a Datapackage using adapters from adapters.py

        :param es_structure:
        :param process_data:
        :return:
        """
        parametrized = {}
        struct_io: dict
        for (process, data), (process, struct_io) in zip(
                process_data.items(), es_structure.items()
        ):
            process_type: str = PROCESS_TYPE_MAP[process]

            adapter = TYPE_MAP[process_type]
            paramet = data.scalars.apply(
                adapter.parametrize_dataclass,
                struct=struct_io,
                process_type=process_type,
                axis=1,
            )
            if process_type in parametrized.keys():
                parametrized[process_type] = pd.concat(
                    [
                        pd.DataFrame([param.as_dict() for param in paramet.values]),
                        parametrized[process_type],
                    ],
                    ignore_index=True,
                )
            else:
                parametrized[process_type] = pd.DataFrame(
                    [param.as_dict() for param in paramet.values]
                )
        return cls

    def save_datapackage_to_csv(self, datapackage, destination):
        for key, value in datapackage.items():
            file_path = os.path.join(destination, key + ".csv")
            # FIXME droping na only for tests!
            value = value.dropna(axis="columns")
            value.to_csv(file_path, sep=";", index=False)



