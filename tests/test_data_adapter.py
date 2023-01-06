import pathlib
from collections import namedtuple

import pandas as pd

from data_adapter_oemof.adapters import Adapter
from data_adapter_oemof import dataclasses

TEST_DIR = pathlib.Path(__file__).parent
TEMP_DIR = TEST_DIR / "_temp"


def test_adapter():
    Component = namedtuple("Component", ["type", "data"])

    # first, a list of components and some data
    components = [
        Component(
            type="volatile",
            data={
                "region": "B",
                "carrier": "wind",
                "tech": tech,
                "sedos-capacity": 12,
                "sedos-marginal_cost": 1,
                "sedos-overnight_cost": 1000,
                "sedos-fixed_cost": 100,
                "sedos-lifetime": 25,
                "sedos-wacc": 0.05,
            },
        )
        for tech in ["onshore", "offshore"]
    ]

    # collect instances of the adapters
    adapters = {}
    for component in components:
        if component.type not in adapters:
            adapters[component.type] = []

        datacls = dataclasses.TYPE_MAP[component.type]

        adapter = Adapter(datacls, data=component.data)

        adapters[component.type].append(adapter)

    # parametrize
    parametrized = {}
    for typ, adapters in adapters.items():
        parametrized[typ] = [adapter.parametrize_dataclass() for adapter in adapters]

    # create a dictionary of dataframes
    dataframes = {type: pd.DataFrame(adapted) for type, adapted in parametrized.items()}

    for typ, df in dataframes.items():

        path_default = (
            TEST_DIR
            / "_files"
            / "tabular_datapackage_mininmal_example"
            / "data"
            / "elements"
            / f"{typ}.csv"
        )

        df_default = pd.read_csv(path_default, sep=";")

        assert set(df.columns) == set(df_default.columns)

        print(df)
        print(df_default)

        # pd.testing.assert_frame_equal(df, df_default)


test_adapter()
