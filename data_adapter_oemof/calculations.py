import collections
import logging
import warnings
import json

import numpy as np
import pandas as pd
from oemof.tools.economics import annuity

from .utils import divide_two_lists, multiply_two_lists


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


def decommission(
    process_name, adapter_dict: dict, column: str = "capacity", max_column: str = "max", change_parameter: str = "output_parameters",
) -> dict:
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

    # check if capacity column is there and if it has to be decommissioned
    if column not in adapter_dict.keys():
        logging.info(
            f"{column} missing for decommissioning " f"of Process `{process_name}`"
        )
        return adapter_dict

    if not isinstance(adapter_dict[column], list):
        logging.info(
            f"No {column} fading out that can be decommissioned"
            f" for Process `{process_name}`."
        )
        return adapter_dict

    # I:
    if change_parameter not in ["input_parameters", "output_parameters"]:
        # this occurs e.g. for flow_share_max of mimo
        # still max might be set in parameters
        parameter = "output_parameters"
    else:
        parameter = change_parameter
    if max_column not in adapter_dict[parameter].keys():
        max = list(adapter_dict[column] / np.nanmax(adapter_dict[column]))

    # II:
    else:
        max = list(multiply_two_lists(
                adapter_dict[parameter][max_column],
                adapter_dict[column]
            ) / np.nanmax(adapter_dict[column]))
        if change_parameter != parameter:
            # drop max output_parameters, as max_column is saved in `change_parameter`
            del adapter_dict[parameter][max_column]

    if change_parameter != parameter:
        # e.g. flow_share_max_<bus_name>
        adapter_dict[change_parameter] = max
    else:
        # max must be extended to time series over all time steps of each
        # period as output_parameters are not extended in oemof.tabular
        column_name = [f"max_timeseries_{process_name}"]
        timeseries = pd.DataFrame(columns=column_name)
        for y in adapter_dict["year"]:
            ts = pd.DataFrame(data=[1 for i in range(8760)],
                              columns=column_name,
                              index=pd.date_range(f"1/1/{y}", periods=8760,
                                                  freq="h"),
                              dtype="float64")
            timeseries = ts.copy() if timeseries.empty else pd.concat(
                [timeseries, ts])
        max_time_series = adapt_profile_with_yearly_value(profile=timeseries,
                                                          value=max)

        max_time_series = reduce_data_frame(max_time_series)

        adapter_dict[change_parameter][max_column] = list(max_time_series[column_name[0]].values)
        adapter_dict[change_parameter] = json.dumps(adapter_dict[change_parameter])

    # set `column` value to maximum value
    adapter_dict[column] = np.nanmax(adapter_dict[column])
    return adapter_dict


def adapt_profile_with_yearly_value(profile, value):

    # Map amount to years
    years = sorted(profile.index.year.unique())
    values_mapped_to_years = dict(zip(years, value))

    profile["value"] = profile.index.year.map(values_mapped_to_years)

    # Multiply profile with value
    col_name = profile.columns[0]
    profile["adjusted_ts"] = (profile[col_name] * profile["value"])
    profile.drop(columns=[col_name, "value"], inplace=True)
    profile.rename(columns={"adjusted_ts": col_name}, inplace=True)

    return profile

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


def process_availability_constant_to_full_load_time_max(adapter):
    """ Calculate full load time max from availability constant."""
    if "availability_constant" in adapter.data.keys():
        availability_constant = adapter.data["availability_constant"]
        if availability_constant > 1:  # assumption: then the unit is %
            availability_constant = availability_constant / 100
        adapter.data["full_load_time_max"] = 8760 * availability_constant


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
    This function shall handle found nans in the data.

    Identifiers are set pre-mapping! (Might implement mapping feature later)

    Providing data for one process with changing values and missing some values
    cannot be handled by oemof.solph multi period feature. Either a value can be set
    or it can be None but cannot be None in some year and be set in another.

    Sometimes data is still missing for some periods.
    For most of these occurrences the missing data does not matter:
        - The Investment in the invest-object is not allowed in these years
        - Existing process is already decommissioned.
    For these cases the missing data is marked `irrelevant`.

    The found nans are replaced:
        - min/max values replaced by 0 or 9999999999999 (see `handle_min_max()`)
        - `irrelevant` data is replaced by mean (arithmetic)
        - Other is replaced by mean and warning is issued

    Parameters
    ----------
    group_df

    Returns
    -------

    """

    def handle_min_max(group_df: pd.DataFrame) -> pd.DataFrame:
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
            "availability_timeseries_max",
            "capacity_tra_connection_max",
            "flow_share_max_",
            "sto_cycles_max",
            "sto_max_timeseries",
            "capacity_p_abs_new_max",
            "capacity_e_abs_new_max",
            "capacity_w_abs_new_max",
        ]

        for column in group_df.columns:
            if column in ["method", "source", "comment", "bandwidth_type"]:
                continue

            """
            Following is a check whether nans can be filled.

            Commented check for columns that are faulty and need to be changed
            Commented Error for incomplete columns as we dont know where it may cause errors yet

            """
            if column in max:
                group_df[column] = group_df[column].fillna(max_value)
            elif column in min:
                group_df[column] = group_df[column].fillna(min_value)

        return group_df

    def find_and_replace_irrelevant_data(group_df: pd.DataFrame) -> pd.DataFrame:
        """
        Finds and replaces irrelevant Data.

        Searches for where investment is allowed
            - If allowed Investmet is 0, nan data is replaced by mean.
        Searches for decomissioned Processes
            - If capacity of a process is 0, nan data is replaced by mean.

        Parameters
        ----------
        group_df

        Returns
        -------

        """

        capacity_columns = [
            "capacity_p_inst_0",
            "capacity_e_inst_0",
            "capacity_w_inst_0",
            "capacity_tra_inst_0",
        ]

        invest_zero = [
            "capacity_p_abs_new_max",
            "capacity_e_abs_new_max",
            "capacity_w_abs_new_max",
        ]

        max_zero = ["capacity_p_max", "capacity_e_max", "capacity_w_max"]

        # Get relevant columns that appear in dataframe
        max_col = [d for d in max_zero if d in group_df.columns]
        invest_col = [d for d in invest_zero if d in group_df.columns]
        capacity_col = [d for d in capacity_columns if d in group_df.columns]

        # Set all indices to "not be filled" (False)
        fill_indices = pd.Series([False] * len(group_df), index=group_df.index)

        # Capacity and Investment cannot be set in parallel. If both columns appear in dataframe
        # Fill the ones where capacity is set to 0 (decomissioned)
        if len(capacity_col) == 1 and (len(invest_col) != 0 or len(max_col) != 0):
            # Add Indices where capacity is 0
            fill_indices += group_df[capacity_col[0]] == 0
        elif len(max_col) == 1:
            # Add indices where capacity max == 0 (making investment impossible)
            fill_indices += group_df[max_col[0]] == 0
        elif len(invest_col) == 1:
            # Add indices where investment is not allowed
            fill_indices += group_df[invest_col[0]] == 0

        # Fill indices
        group_df.loc[fill_indices] = group_df.fillna(
            group_df.mean(numeric_only=True)
        ).loc[fill_indices]

        return group_df

    group_df = handle_min_max(group_df)
    return find_and_replace_irrelevant_data(group_df)


def reduce_data_frame(data_frame, steps=4):
    """reduces `df` to less time steps per period"""
    df = data_frame.copy()
    df["ind"] = df.index
    df["ind"] = df["ind"].apply(
        lambda
            x: True if x.month == 1 and x.day == 1 and x.hour <= steps else False
    )
    df_reduced = df.loc[df["ind"] == 1].drop(columns=["ind"])
    return df_reduced
