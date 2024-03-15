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


def decommission(facade_adapter) -> dict:
    """
    Non investment objects must be decomisioned in multi period to take end of lifetime
     for said objet into account
    Returns
    -------

    """
    capacity_column = "capacity"
    max_column = "max"

    if capacity_column not in facade_adapter.keys():
        raise AttributeError("Capacity missing for decommissioning")

    if max_column in facade_adapter.keys():
        if facade_adapter[capacity_column] == facade_adapter[max_column]:
            pass
        else:
            pass
            # do something to recalculate max values
            # FIXME: waiting for implementation of non-oemof value calculation
    else:
        # FIXME: Does `max`/`full_load_time_max`
        facade_adapter[max_column] = facade_adapter[capacity_column]

    max_capacity = np.max(facade_adapter[capacity_column])
    facade_adapter[capacity_column] = [
        max_capacity for i in facade_adapter[capacity_column]
    ]
    return facade_adapter

    # divide each capacity by max capacity and insert fraction as max value
