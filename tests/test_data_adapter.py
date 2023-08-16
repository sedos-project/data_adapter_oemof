import pathlib
from collections import namedtuple

import pandas as pd

from data_adapter_oemof.mappings import Mapper
from data_adapter_oemof.adapters import VolatileAdapter, FACADE_ADAPTERS

TEST_DIR = pathlib.Path(__file__).parent
TEMP_DIR = TEST_DIR / "_temp"


def test_simple_adapter():
    data = {
        "type": "volatile",
        "region": "B",
        "carrier": "wind",
        "bus": "electricity",
        "tech": "onshore",
        "capacity": 12,
        "sedos-marginal_cost": 1,
        "sedos-overnight_cost": 1000,
        "capital_costs": 100,
        "sedos-lifetime": 25,
        "sedos-wacc": 0.05,
    }
    timeseries = pd.DataFrame({"a": [0, 0, 0], "onshore_B": [1, 2, 3]})
    struct = {"default": {"inputs": ["onshore"], "outputs": ["electricity"]}}
    mapping = {
        "modex_tech_wind_turbine_onshore": {"profile": "onshore", "lifetime": "sedos-lifetime"}
    }
    mapper = Mapper(
        adapter=VolatileAdapter,
        process_name="modex_tech_wind_turbine_onshore",
        data=data,
        timeseries=timeseries,
        mapping=mapping,
    )
    adapter = VolatileAdapter(struct, mapper)
    expected_dict = {
        "bus": "electricity",
        "capacity": 12,
        "carrier": "wind",
        "name": "B-None-onshore",
        "profile": "onshore_B",
        "tech": "onshore",
        "type": "volatile",
        "lifetime": 25,
        "region": "B",
        "year": None
    }
    assert adapter.as_dict() == expected_dict


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
