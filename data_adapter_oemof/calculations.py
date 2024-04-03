import logging

import numpy as np
from oemof.tools.economics import annuity


class CalculationError(Exception):
    """Raise this exception if calculation goes wrong"""


def calculation(func):
    """
    This is a decorator that allows calculations to fail
    """

    def decorated_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:  # pylint: disable=broad-except
            raise CalculationError(
                f"Calculation function '{func.__name__}' \n"
                f"called with {args, kwargs} \n"
                f"failed because of: \n" + str(e)
            )

    return decorated_func


@calculation
def get_name(*args, counter=None):
    name = "--".join(args)
    if counter:
        name += f"--{next(counter)}"
    return name


@calculation
def get_capacity_cost(overnight_cost, fixed_cost, lifetime, wacc):
    return annuity(overnight_cost, lifetime, wacc) + fixed_cost


def decommission(adapter_dict: dict) -> dict:
    """

    Takes adapter dictionary from adapters.py with mapped values.

    Supposed to be called when getting default parameters

    Non investment objects must be decommissioned in multi period to take end of lifetime
     for said objet into account

    Returns
    dictionary (
    -------

    """
    capacity_column = "capacity"
    max_column = "max"

    if capacity_column not in adapter_dict.keys():
        logging.info("Capacity missing for decommissioning")
        return adapter_dict

    if not isinstance(adapter_dict[capacity_column], list):
        logging.info("No capacity fading out that can be decommissioned.")
        return adapter_dict

    if max_column in adapter_dict.keys():
        if adapter_dict[capacity_column] == adapter_dict[max_column]:
            adapter_dict[max_column] = adapter_dict[capacity_column] / np.max(
                adapter_dict[capacity_column]
            )
        else:
            adapter_dict[max_column] = list(
                (adapter_dict[max_column] / np.max(adapter_dict[capacity_column]))
            )
    else:
        # FIXME: Does `max`/`full_load_time_max`
        adapter_dict[max_column] = adapter_dict[capacity_column] / np.max(
            adapter_dict[capacity_column]
        )

    adapter_dict[capacity_column] = np.max(adapter_dict[capacity_column])
    return adapter_dict
