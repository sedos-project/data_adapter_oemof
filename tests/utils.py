import os
import pathlib

import pandas as pd
from oemof.solph.helpers import extend_basic_path

PATH_TEST_FILES = pathlib.Path(__file__).parent

PATH_TMP = extend_basic_path("data_adapter_oemof")


def get_all_file_paths(dir):
    r"""
    Finds all paths of files in a directory.

    Parameters
    ----------
    dir : str
        Directory

    Returns
    -------
    file_paths : list
        list of str
    """
    # pylint: disable=unused-variable
    file_paths = []
    for dir_path, dir_names, file_names in os.walk(dir):
        for file_name in file_names:
            file_paths.append(os.path.join(dir_path, file_name))

    return file_paths


def check_if_csv_files_equal(csv_file_a, csv_file_b):
    r"""
    Compares two csv files.

    Parameters
    ----------
    csv_file_a
    csv_file_b

    """
    df1 = pd.read_csv(csv_file_a, delimiter=";")
    df2 = pd.read_csv(csv_file_b, delimiter=";")

    assert pd.testing.assert_frame_equal(df1, df2, check_like=True) is None


def check_if_csv_dirs_equal(dir_a, dir_b):
    r"""
    Compares the csv files in two directories and asserts that
    they are equal.

    The function asserts that:

    1. The file names of csv files found in the directories are the same.
    2. The file contents are the same.

    Parameters
    ----------
    dir_a : str
        Path to first directory containing csv files

    dir_b : str
        Path to second directory containing csv files

    """
    files_a = get_all_file_paths(dir_a)
    files_b = get_all_file_paths(dir_b)

    files_a = [file for file in files_a if file.split(".")[-1] == "csv"]
    files_b = [file for file in files_b if file.split(".")[-1] == "csv"]
    files_a.sort()
    files_b.sort()

    f_names_a = [os.path.split(f)[-1] for f in files_a]
    f_names_b = [os.path.split(f)[-1] for f in files_b]

    diff = list(set(f_names_a).symmetric_difference(set(f_names_b)))

    assert not diff, f"Lists of filenames are not the same." f" The diff is: {diff}"

    for file_a, file_b in zip(files_a, files_b):
        try:
            check_if_csv_files_equal(file_a, file_b)
        except AssertionError:
            diff.append([file_a, file_b])

    if diff:
        error_message = ""
        for pair in diff:
            short_name_a, short_name_b = (
                os.path.join(*f.split(os.sep)[-4:]) for f in pair
            )
            line = " - " + short_name_a + " and " + short_name_b + "\n"
            error_message += line

        raise AssertionError(
            f" The contents of these file are different:\n{error_message}"
        )
