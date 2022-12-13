import pathlib
import pandas as pd

from data_adapter_oemof import adapter_classes


TEST_DIR = pathlib.Path(__file__).parent
TEMP_DIR = TEST_DIR / "_temp"


def test_adapter():
    regions = ["B"]
    carriers = ["wind"]
    techs = ["onshore"]
    types = ["volatile"]
    adapters = {type: adapter_classes.ADAPTER_MAP[type] for type in types}

    for type, adapter in adapters.items():
        instances = []
        for region in regions:
            for carrier in carriers:
                for tech in techs:
                    instances.append(adapter.parametrize())

        df = pd.DataFrame(instances)

        path_default = (
            TEST_DIR
            / "_files"
            / "tabular_datapackage_mininmal_example"
            / "data"
            / "elements"
            / f"{type}.csv"
        )

        df_default = pd.read_csv(path_default, sep=";")

        pd.testing.assert_frame_equal(df, df_default)
