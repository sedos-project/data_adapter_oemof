import numpy as np
import pandas as pd
import yaml


def load_yaml(file_path):
    with open(file_path, "r", encoding="UTF-8") as file:
        dictionary = yaml.load(file, Loader=yaml.FullLoader)
    return dictionary


def has_mixed_types(column) -> bool:
    """
    Function to check if an element is of mixed type
    Parameters
    ----------
    column

    Returns bool
    -------

    """
    unique_types = set(type(element) for element in column)
    return len(unique_types) > 1


def convert_mixed_types_to_same_length(column):
    """
    Function to convert entries to arrays of the same length only for columns with mixed types

    Parameters
    ----------
    column

    Returns
    -------

    """
    if has_mixed_types(column):
        max_length = max(
            len(entry) if isinstance(entry, list) else 1 for entry in column
        )
        return [
            entry
            if isinstance(entry, list)
            else list(np.full(max_length, entry))
            if not pd.isna(entry)
            else np.nan  # Keep NaN as is
            for entry in column
        ]
    else:
        return column
