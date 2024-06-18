import collections
import logging
import warnings

from utils import divide_two_lists, multiply_two_lists

import numpy as np
import pandas as pd
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


def decommission(process_name, adapter_dict: dict) -> dict:
    """

    Takes adapter dictionary from adapters.py with mapped values.

    I:
    Takes largest found capacity and sets this capacity for all years
    Each yearly changing capacity value is divided by max capacity and
    quotient from `max capacity`/`yearly capacity` is set as max value.

    II:
    If Max value is already set by another parameter function will issue info
    Recalculating max value to

    .. math::
        max_{new} = \frac{(max_{column} * capacity_{column})}{capacity_{max}}

    Overwriting max value in `output_parameters`
    Then is setting capacity to the largest found capacity


    Supposed to be called when getting default parameters
    Non investment objects must be decommissioned in multi period to take end of lifetime
    for said objet into account.

    Returns
    adapter_dictionary with max values in output parameters and a single capacity
    -------

    """
    capacity_column = "capacity"
    max_column = "max"

    # check if capacity column is there and if it has to be decommissioned
    if capacity_column not in adapter_dict.keys():
        logging.info(
            f"Capacity missing for decommissioning " f"of Process `{process_name}`"
        )
        return adapter_dict

    if not isinstance(adapter_dict[capacity_column], list):
        logging.info(
            f"No capacity fading out that can be decommissioned"
            f" for Process `{process_name}`."
        )
        return adapter_dict

    # I:
    if max_column not in adapter_dict["output_parameters"].keys():
        adapter_dict["output_parameters"][max_column] = adapter_dict[
            capacity_column
        ] / np.max(adapter_dict[capacity_column])
    # II:
    else:
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

    if "activity_bound_fix" in adapter.data.keys():
        adapter.data["activity_bound_fix"] = divide_two_lists(
            adapter.data["activity_bound_fix"], adapter.get("capacity")
        )
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


def handle_nans(group_df: pd.DataFrame) -> pd.DataFrame:
    """
    This function should find and fill in missing min and max values in the data

    Missing min value is set to 0.
    Missing max value is set to 9999999999999.

    Min values:
    capacity_p_min
    capacity_e_min
    capacity_w_min
    flow_share_min_<commodity>

    Max values:
    potential_annual_max
    capacity_p_max
    capacity_e_max
    capacity_w_max
    capacity_p_abs_new_max
    capacity_e_abs_new_max
    capacity_w_abs_new_max
    availability_timeseries_max
    capacity_tra_connection_max
    flow_share_max_<commodity>
    sto_cycles_max
    sto_max_timeseries

    Returns
    -------

    """

    max_value = 9999999999999
    min_value = 0

    min = ["capacity_p_min", "capacity_e_min", "capacity_w_min", "flow_share_min_"]

    max = [
        "potential_annual_max",
        "capacity_p_max",
        "capacity_e_max",
        "capacity_w_max",
        "capacity_p_abs_new_max",
        "capacity_e_abs_new_max",
        "capacity_w_abs_new_max",
        "availability_timeseries_max",
        "capacity_tra_connection_max",
        "flow_share_max_",
        "sto_cycles_max",
        "sto_max_timeseries",
    ]

    for column in group_df.columns:
        if column in ["method", "source", "comment", "bandwidth_type"]:
            continue

        if group_df.isna().sum>0:
            if column in max:
                group_df[column].fillna(max_value, inplace=True)
            elif column in min:
                group_df[column].fillna(min_value, inplace=True)
            else:
                if "type" in group_df.columns:

                    raise ValueError(f"In column {column} for process {group_df['type']} nan values are found"
                                     f"please make sure to only provide complete datasets")
                else:
                    raise ValueError(f"In column {column} nan values are found"
                                     f"please make sure to only provide complete datasets")
