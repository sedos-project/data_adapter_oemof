import collections
import logging
import warnings

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

    I:
    Takes largest found capacity and sets this capacity for all years
    Each yearly changing capacity value is divided by max capacity and
    quotient from `max capacity`/`yearly capacity` is set as max value.

    II:
    If Max value is already set by another parameter function will issue info
    Recalculating max value to max_new = (max_old * capacity)/`the largest capacity`
    Overwriting max value in `output_parameters`
    Then is setting capacity to the largest found capacity

    Supposed to be called when getting default parameters
    Non investment objects must be decommissioned in multi period to take end of lifetime
    for said objet into account.

    Returns
    adapter_dictionary with max values in output parameters and a single capacity
    -------

    """

    def multiply_two_lists(l1, l2):
        """
        Multiplies two lists

        Lists must be same length

        Parameters
        ----------
        l1
        l2

        Returns divided list
        -------

        """
        return [i * j for i, j in zip(l1, l2)]

    capacity_column = "capacity"
    max_column = "max"

    # check if capacity column is there and if it has to be decommissioned
    if capacity_column not in adapter_dict.keys():
        logging.info("Capacity missing for decommissioning")
        return adapter_dict

    if not isinstance(adapter_dict[capacity_column], list):
        logging.info("No capacity fading out that can be decommissioned.")
        return adapter_dict

    # I:
    if max_column not in adapter_dict["output_parameters"].keys():
        adapter_dict["output_parameters"][max_column] = adapter_dict[
            capacity_column
        ] / np.max(adapter_dict[capacity_column])
    # II:
    else:
        logging.info("Decommissioning and max value can not be set in parallel")
        adapter_dict["output_parameters"][max_column] = multiply_two_lists(
            adapter_dict["output_parameters"][max_column], adapter_dict[capacity_column]
        ) / np.max(adapter_dict[capacity_column])

    adapter_dict[capacity_column] = np.max(adapter_dict[capacity_column])
    return adapter_dict


def normalize_activity_bonds(adapter):
    """
    Normalizes activity bonds in order to be used as min/max values
    Parameters
    ----------
    adapter

    Returns
    -------

    """

    def divide_two_lists(dividend, divisor):
        """
        Divides two lists returns quotient, returns 0 if divisor is 0

        Lists must be same length

        Parameters
        ----------
        dividend
        divisor

        Returns divided list
        -------

        """
        return [i / j if j != 0 else 0 for i, j in zip(dividend, divisor)]

    if "activity_bound_fix" in adapter.data.keys():
        adapter.data["activity_bound_min"] = divide_two_lists(
            adapter.data["activity_bound_fix"], adapter.get("capacity")
        )
        adapter.data["activity_bound_max"] = adapter.data["activity_bound_min"]
        adapter.data.pop("activity_bound_fix")
        return adapter

    if "activity_bound_min" in adapter.data.keys():
        adapter.data["activity_bound_min"] = divide_two_lists(
            adapter.data["activity_bound_min"], adapter.get("capacity")
        )
        return adapter

    if "activity_bound_max" in adapter.data.keys():
        adapter.data["activity_bound_max"] = divide_two_lists(
            adapter.data["activity_bound_max"], adapter.get("capacity")
        )
        return adapter


def floor_lifetime(mapped_defaults):
    """

    Parameters
    ----------
    adapter

    Returns
    -------

    """
    if not isinstance(mapped_defaults["lifetime"], collections.abc.Iterable):
        mapped_defaults["lifetime"] = int(np.floor(mapped_defaults["lifetime"]))
    elif all(x == mapped_defaults["lifetime"][0] for x in mapped_defaults["lifetime"]):
        mapped_defaults["lifetime"] = int(np.floor(mapped_defaults["lifetime"][0]))
    else:
        warnings.warn("Lifetime cannot change in Multi-period modeling")
        mapped_defaults["lifetime"] = int(np.floor(mapped_defaults["lifetime"][0]))
    return mapped_defaults
