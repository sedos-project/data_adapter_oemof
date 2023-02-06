import warnings

from oemof.tools.economics import annuity


def calculation(func):
    r"""
    This is a decorator that allows calculations to fail
    """

    def decorated_func(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            warnings.warn(
                f"Calculation function '{func.__name__}' \n"
                f"called with {args, kwargs} \n"
                f"failed because of: \n" + str(e)
            )

    return decorated_func


@calculation
def get_name(region, carrier, tech):
    return f"{region}-{carrier}-{tech}"


@calculation
def get_capacity_cost(overnight_cost, fixed_cost, lifetime, wacc):
    return annuity(overnight_cost, lifetime, wacc) + fixed_cost
