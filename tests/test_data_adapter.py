import pandas as pd

from data_adapter_oemof import adapter_classes


def test_adapter():
    regions = ["B", "BB"]
    carriers = ["wind", "pv"]
    techs = ["onshore"]
    types = ["volatile"]
    adapters = [adapter_classes.ADAPTER_MAP[type] for type in types]

    for adapter in adapters:
        instances = []
        for region in regions:
            for carrier in carriers:
                for tech in techs:
                    instances.append(adapter.parametrize())

        df = pd.DataFrame(instances)
        print(df)


if __name__ == "__main__":
    test_adapter()