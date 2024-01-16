import dataclasses
import os
from typing import Optional

from data_adapter.preprocessing import Adapter
from datapackage import Package

from data_adapter_oemof.adapters import FACADE_ADAPTERS
from data_adapter_oemof.settings import BUS_MAP, PARAMETER_MAP, PROCESS_ADAPTER_MAP
from data_adapter_oemof.utils import *


@dataclasses.dataclass
class DataPackage:
    parametrized_elements: dict[
        str, pd.DataFrame()
    ]  # datadict with scalar data in form of {type:pd.DataFrame(type)}
    parametrized_sequences: dict[
        str, pd.DataFrame()
    ]  # timeseries in form of {type:pd.DataFrame(type)}
    foreign_keys: dict  # foreign keys for timeseries profiles
    adapter: Adapter
    periods: pd.DataFrame()

    def save_datapackage_to_csv(self, destination: str) -> None:
        """
        Saving the datapackage to a given destination in oemof.tabular readable format
        Using frictionless datapackage module

        Parameters
        ----------
        self: DataPackage
            DataPackage to save
        destination: str
            String to where the datapackage save to. More convenient to use os.path.
            If last level of folder structure does not exist, it will be created
            (as well as /elements and /sequences)

        Returns
        -------
        None if the Datapackage has been saved correctly (no checks implemented)

        """
        # Check if filestructure is existent. Create folders if not:
        elements_path = os.path.join(destination, "data", "elements")
        sequences_path = os.path.join(destination, "data", "sequences")
        periods_path = os.path.join(destination, "data", "periods")

        os.makedirs(elements_path, exist_ok=True)
        os.makedirs(sequences_path, exist_ok=True)
        os.makedirs(periods_path, exist_ok=True)

        if not self.periods.empty:
            self.periods.to_csv(
                os.path.join(
                    periods_path,
                    "periods.csv",
                ),
                index=True,
                sep=";",
            )

        # Save elements to elements folder named by keys + .csv
        for process_name, process_adapted_data in self.parametrized_elements.items():
            process_adapted_data.to_csv(
                os.path.join(elements_path, f"{process_name}.csv"),
                index=False,
                sep=";",
            )

        # Save Sequences to sequence folder named as keys + _sequence.csv
        for process_name, process_adapted_data in self.parametrized_sequences.items():
            process_adapted_data.to_csv(
                os.path.join(sequences_path, f"{process_name}_sequence.csv"),
                sep=";",
                index_label="timeindex",
                date_format="%Y-%m-%dT%H:%M:%SZ",
            )

        # From saved elements and keys create a Package
        package = Package(base_path=destination)
        package.infer(pattern="**/*.csv")

        # Add foreign keys from self to Package
        for resource in package.descriptor["resources"]:
            field_names = [field["name"] for field in resource["schema"]["fields"]]
            if resource["name"] in self.foreign_keys.keys():
                resource["schema"].update(
                    {"foreignKeys": self.foreign_keys[resource["name"]]}
                )
            else:
                resource["schema"].update({"foreignKeys": []})
            if "name" in field_names:
                resource["schema"].update({"primaryKey": "name"})

            elif (
                "sequence" in resource["name"].split("_")
                or resource["name"] == "periods"
            ):
                pass
            else:
                warnings.warn(
                    "Primary keys differing from `name` not implemented yet."
                    f"Check primary Keys for resource {resource['name']}"
                )

        # re-initialize Package with added foreign keys and save datapackage.json
        Package(package.descriptor).save(os.path.join(destination, "datapackage.json"))

        return None

    @classmethod
    def build_datapackage(
        cls,
        adapter: Adapter,
        process_adapter_map: Optional[dict] = PROCESS_ADAPTER_MAP,
        parameter_map: Optional[dict] = PARAMETER_MAP,
        bus_map: Optional[dict] = BUS_MAP,
    ):
        """
        Creating a Datapackage from the oemof_data_adapter that fits oemof.tabular Datapackage.

        Parameters
        ----------
        adapter: Adapter
            Adapter from data_adapter that is able to handle parameter model data
            from Databus. Adapter needs to be initialized with `structure_name`.
            Use `links` to add data from different processes to each other.
            Use `structure` to map busses to "processes" and "Adapters"
        process_adapter_map
            Maps process names to adapter names, if not set default mapping is used
        parameter_map
            Maps parameter names from adapter to facade, if not set default mapping is used.
            Make sure to map "sequence" entries on "sequence profile names" (see example)
        bus_map
            Maps facade bus names to adapter bus names, if not set default mapping is used

        Returns
        -------
        DataPackage

        """
        parametrized_elements = {"bus": []}
        parametrized_sequences = {}
        foreign_keys = {}
        # Iterate Elements
        for process_name, struct in adapter.get_process_list().items():
            process_data = adapter.get_process(process_name)
            timeseries = process_data.timeseries
            if isinstance(timeseries.columns, pd.MultiIndex):
                timeseries.columns = (
                    timeseries.columns.get_level_values(0)
                    + "_"
                    + [x[0] for x in timeseries.columns.get_level_values(1).values]
                )
            facade_adapter_name: str = process_adapter_map[process_name]
            facade_adapter = FACADE_ADAPTERS[facade_adapter_name]
            components = []
            process_busses = []
            process_scalars = yearly_scalars_to_periodic_values(process_data.scalars)
            # Build class from adapter with Mapper and add up for each component within the Element
            for component_data in process_scalars.to_dict(orient="records"):
                component_mapper = Mapper(
                    adapter=facade_adapter,
                    process_name=process_name,
                    data=component_data,
                    timeseries=timeseries,
                    mapping=parameter_map,
                    bus_map=bus_map,
                )
                components.append(
                    FACADE_ADAPTERS[facade_adapter_name](
                        struct=struct, mapper=component_mapper
                    ).facade_dict
                )
                # Fill with all busses occurring, needed for foreign keys as well!
                process_busses += list(component_mapper.get_busses(struct).values())

            process_busses = list(pd.unique(process_busses))
            parametrized_elements["bus"] += process_busses

            # getting foreign keys with last component
            # foreign keys have to be equal for every component within a Process
            # as foreign key columns cannot have mixed meaning
            foreign_keys[process_name] = get_foreign_keys(
                struct, component_mapper, components
            )

            parametrized_elements[process_name] = pd.DataFrame(components)
            if not timeseries.empty:
                parametrized_sequences.update({process_name: timeseries})
        # Create Bus Element from all unique `busses` found in elements
        parametrized_elements["bus"] = pd.DataFrame(
            {
                "name": (names := pd.unique(parametrized_elements["bus"])),
                "type": ["bus" for i in names],
                "balanced": [True for i in names],
            }
        )
        periods = get_periods_from_parametrized_sequences(parametrized_sequences)

        return cls(
            parametrized_elements=parametrized_elements,
            parametrized_sequences=parametrized_sequences,
            adapter=adapter,
            foreign_keys=foreign_keys,
            periods=periods,
        )
