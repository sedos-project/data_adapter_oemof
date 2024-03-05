import collections
from unittest import mock

import pandas as pd
from data_adapter.preprocessing import Adapter

Mock = collections.namedtuple(
    typename="Mock",
    field_names=["mock_adapter", "process_adapter_map", "parameter_map"],
)


def define_mock():
    """
    Defines a Mock to be used in tests
    Returns
    Mock tuple with mappers
    -------

    """

    def mock_get_process(process_name):
        """
        Adding side effects and .scalar and .timeseries
        Parameters
        ----------
        process_name

        Returns
        -------
        mocked function .get_process for Adapter

        """
        if process_name == "modex_tech_storage_battery":
            process_mock = mock.Mock()
            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "capacity": {0: 17.8, 1: 17.8, 2: 17.8},
                    "invest_relation_output_capacity": {0: 3.3, 1: 3.3, 2: 3.3},
                    "tech": {
                        0: "storage_battery",
                        1: "storage_battery",
                        2: "storage_battery",
                    },
                    "carrier": {
                        0: "Lithium",
                        1: "Lithium",
                        2: "Lithium",
                    },
                    "fixed_costs": {0: 1, 1: 2, 2: 3},
                }
            )
            process_mock.timeseries = pd.DataFrame()
            return process_mock
        elif process_name == "modex_tech_generator_gas":
            process_mock = mock.Mock()

            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "installed_capacity": {
                        0: 330.2,
                        1: 330.2,
                        2: 330.2,
                    },
                    "emission_factor": {
                        0: 0.2,
                        1: 0.2,
                        2: 0.2,
                    },
                    "fuel_costs": {0: 25.9, 1: 25.9, 2: 49.36},
                    "tech": {
                        0: "generator_gas",
                        1: "generator_gas",
                        2: "generator_gas",
                    },
                    "carrier": {
                        0: "gas",
                        1: "gas",
                        2: "gas",
                    },
                    "condensing_efficiency": {
                        0: 0.16,
                        1: 0.3,
                        2: 0.5,
                    },
                    "electric_efficiency": {0: 0.35, 1: 0.35, 2: 0.4},
                    "thermal_efficiency": {0: 0.5, 1: 0.5, 2: 0.45},
                }
            )

            process_mock.timeseries = pd.DataFrame()
            return process_mock
        elif process_name == "modex_tech_wind_turbine_onshore":
            process_mock = mock.Mock()
            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "installed_capacity": {
                        0: 5700.03,
                        1: 5700.03375,
                        2: 5700.03375,
                    },
                    "fixed_costs": {0: 23280.0, 1: 12600.0, 2: 11340.0},
                    "lifetime": {0: 25.4, 1: 30.0, 2: 30.0},
                    "wacc": {0: 0.07, 1: 0.07, 2: 0.07},
                    "tech": {
                        0: "wind_turbine_onshore",
                        1: "wind_turbine_onshore",
                        2: "wind_turbine_onshore",
                    },
                    "carrier": {
                        0: "wind",
                        1: "wind",
                        2: "wind",
                    },
                }
            )
            process_mock.timeseries = pd.DataFrame(
                {
                    "onshore_BB": {
                        "2016-01-01T00:00:00": 0.0516,
                        "2016-01-01T01:00:00": 0.051,
                        "2016-01-01T02:00:00": 0.0444,
                        "2030-01-01T00:00:00": 0.0526,
                        "2030-01-01T01:00:00": 0.051,
                        "2030-01-01T02:00:00": 0.0444,
                        "2050-01-01T00:00:00": 0.0536,
                        "2050-01-01T01:00:00": 0.051,
                        "2050-01-01T02:00:00": 0.0444,
                    },
                }
            )
            return process_mock
        elif process_name == "modex_tech_photovoltaic_utility":
            process_mock = mock.Mock()
            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "installed_capacity": {
                        0: 9700.03,
                        1: 10700.03375,
                        2: 12700.03375,
                    },
                    "fixed_costs": {0: 10280.0, 1: 8600.0, 2: 6340.0},
                    "lifetime": {0: 25.4, 1: 30.0, 2: 30.0},
                    "wacc": {0: 0.07, 1: 0.07, 2: 0.07},
                    "tech": {
                        0: "photovoltaic_utility",
                        1: "photovoltaic_utility",
                        2: "photovoltaic_utility",
                    },
                    "carrier": {
                        0: "solar",
                        1: "solar",
                        2: "solar",
                    },
                }
            )
            process_mock.timeseries = pd.DataFrame(
                {
                    "photovoltaic_BB": {
                        "2016-01-01T00:00:00": 0.0,
                        "2016-01-01T01:00:00": 0.0,
                        "2016-01-01T02:00:00": 0.16,
                        "2030-01-01T00:00:00": 0.0,
                        "2030-01-01T01:00:00": 0.0,
                        "2030-01-01T02:00:00": 0.30,
                        "2050-01-01T00:00:00": 0.0,
                        "2050-01-01T01:00:00": 0.0,
                        "2050-01-01T02:00:00": 0.50,
                    },
                }
            )
            return process_mock
        elif process_name == "modex_tech_Load":
            process_mock = mock.Mock()
            process_mock.scalars = pd.DataFrame(
                {
                    "region": {0: "BB", 1: "BB", 2: "BB"},
                    "year": {0: 2016, 1: 2030, 2: 2050},
                    "amount": {0: 1, 1: 1, 2: 2},
                    "carrier": {
                        0: "electricity",
                        1: "electricity",
                        2: "electricity",
                    },
                }
            )
            process_mock.timeseries = pd.DataFrame(
                {
                    "Load_BB": {
                        "2016-01-01T00:00:00": 1.0,
                        "2016-01-01T01:00:00": 1.0,
                        "2016-01-01T02:00:00": 1.16,
                        "2030-01-01T00:00:00": 1.0,
                        "2030-01-01T01:00:00": 1.0,
                        "2030-01-01T02:00:00": 1.30,
                        "2050-01-01T00:00:00": 1.0,
                        "2050-01-01T01:00:00": 1.0,
                        "2050-01-01T02:00:00": 1.50,
                    },
                }
            )
            return process_mock

    # Create a mock adapter object for testing
    mock_adapter = mock.Mock(spec=Adapter)
    # Mock the required methods and attributes of the Adapter

    # Create a mock object for the "structure" attribute
    mock_structure = mock.Mock()

    # Assign the "structure" attribute to the mock object
    mock_adapter.structure = mock_structure

    # Define the processes dictionary
    processes = {
        "modex_tech_storage_battery": {
            "default": {"inputs": ["electricity"], "outputs": []}
        },
        "modex_tech_generator_gas": {
            "default": {"inputs": ["ch4"], "outputs": ["electricity"]},
        },
        "modex_tech_wind_turbine_onshore": {
            "default": {"inputs": ["onshore"], "outputs": ["electricity"]}
        },
        "modex_tech_photovoltaic_utility": {
            "default": {"inputs": [], "outputs": ["electricity"]}
        },
        "modex_tech_Load": {"default": {"inputs": ["electricity"], "outputs": []}},
    }

    # Set the processes dictionary as an attribute of the "structure" mock object
    mock_adapter.structure.processes = processes

    mock_adapter.get_process.side_effect = mock_get_process

    process_adapter_map = {
        "modex_tech_generator_gas": "ExtractionTurbineAdapter",
        "modex_tech_storage_battery": "StorageAdapter",
        "modex_tech_wind_turbine_onshore": "VolatileAdapter",
        "modex_tech_photovoltaic_utility": "VolatileAdapter",
        "modex_tech_Load": "LoadAdapter",
    }
    parameter_map = {
        "DEFAULT": {
            "marginal_cost": "variable_costs",
            "fixed_cost": "fixed_costs",
            "capacity_cost": "capital_costs",
        },
        "ExtractionTurbineAdapter": {
            "carrier_cost": "fuel_costs",
            "capacity": "installed_capacity",
        },
        "modex_tech_wind_turbine_onshore": {"profile": "onshore"},
    }
    return Mock(mock_adapter, process_adapter_map, parameter_map)
