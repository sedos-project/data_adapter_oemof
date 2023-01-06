from data_adapter_oemof.builders import BUILDER_MAP


class Adapter:
    def __init__(self, datacls, data=None, builders=None):
        if builders is None:
            builders = BUILDER_MAP

        self.datacls = datacls
        self.data = data
        self.builders = builders

    def parametrize_dataclass(self):

        build_element = self.builders[self.datacls]

        instance = build_element(self)

        return instance
