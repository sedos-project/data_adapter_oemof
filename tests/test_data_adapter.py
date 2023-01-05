import pathlib
from collections import namedtuple

import pandas as pd

from data_adapter_oemof.adapters import Adapter
from data_adapter_oemof import dataclasses

TEST_DIR = pathlib.Path(__file__).parent
TEMP_DIR = TEST_DIR / "_temp"


def test_adapter():
    Component = namedtuple("Component", ["region", "carrier", "tech", "type"])

    # first, just a list of components
    components = [
        Component(region="B", carrier="wind", tech=tech, type="volatile")
        for tech in ["onshore", "offshore"]
    ]

    # collect instances of the adapters
    type_adapters = {}
    for component in components:
        if component.type not in type_adapters:
            type_adapters[component.type] = []

        datacls = dataclasses.TYPE_MAP[component.type]

        adapter = Adapter(datacls)

        type_adapters[component.type].append(adapter)

    # parametrize
    type_adapted = {}
    for typ, adapters in type_adapters.items():
        type_adapted[typ] = [adapter.parametrize_dataclass() for adapter in adapters]

    # create a dictionary of dataframes
    dataframes = {type: pd.DataFrame(adapted) for type, adapted in type_adapted.items()}

    df = dataframes["volatile"]

    path_default = (
        TEST_DIR
        / "_files"
        / "tabular_datapackage_mininmal_example"
        / "data"
        / "elements"
        / "volatile.csv"
    )

    df_default = pd.read_csv(path_default, sep=";")

    assert set(df.columns) == set(df_default.columns)
    print(df)
    print(df_default)

    # pd.testing.assert_frame_equal(df, df_default)


test_adapter()
