import pathlib
from collections import namedtuple

import pandas as pd

from data_adapter_oemof.adapters import TYPE_MAP

TEST_DIR = pathlib.Path(__file__).parent
TEMP_DIR = TEST_DIR / "_temp"


def test_adapter():
    Component = namedtuple("Component", ["type", "data"])

    # first, a list of components and some data
    components = [
        Component(
            type="volatile",
            data={
                "type": "volatile",
                "region": "B",
                "carrier": "wind",
                "tech": tech,
                "capacity": 12,
                "sedos-marginal_cost": 1,
                "sedos-overnight_cost": 1000,
                "capital_costs": 100,
                "sedos-lifetime": 25,
                "sedos-wacc": 0.05,
            },
        )
        for tech in ["onshore", "offshore"]
    ]

    struct = {"default": {"inputs": ["onshore"], "outputs": ["electricity"]}}

    # collect instances of the adapters
    parametrized = {}
    for component in components:
        if component.type not in parametrized:
            parametrized[component.type] = []

        adapter = TYPE_MAP[component.type]

        parametrized[component.type].append(
            adapter.parametrize_dataclass(component.data, struct, None)
        )
    # create a dictionary of dataframes
    dataframes = {
        type: pd.DataFrame([adapted.as_dict() for adapted in adapted_list])
        for type, adapted_list in parametrized.items()
    }
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

        pd.testing.assert_frame_equal(df, df_default)
