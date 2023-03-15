import os
from collections import defaultdict

import pandas as pd

from data_adapter_oemof.adapters import TYPE_MAP
from data_adapter_oemof.mappings import PROCESS_TYPE_MAP

import inspect

def build_datapackage(**process_data):
    parametrized = defaultdict(list)
    for process, data in process_data.items():
        process_type = PROCESS_TYPE_MAP[process]

        adapter = TYPE_MAP[process_type]
        paramet = data.scalars.apply(adapter.parametrize_dataclass, axis=1)

        parametrized[process_type].extend(paramet.values)



    # create a dictionary of dataframes
    datapackage = {
        type: pd.DataFrame(adapted) for type, adapted in parametrized.items()
    }
    return datapackage


def save_datapackage_to_csv(datapackage, destination):
    for key, value in datapackage.items():
        file_path = os.path.join(destination, key + ".csv")
        value.to_csv(file_path)
