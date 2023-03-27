import json
import os

import yaml

os.environ[
    "COLLECTION_DIR"
] = "/home/local/RL-INSTITUT/felix.maurer/rli/Felix.Maurer/SEDOS/Python/data_adapter_oemof/tests/collections"
os.environ[
    "STRUCTURES_DIR"
] = "/home/local/RL-INSTITUT/felix.maurer/rli/Felix.Maurer/SEDOS/Python/data_adapter_oemof/tests/structure/"
from data_adapter.preprocessing import get_process
from data_adapter.structure import get_energy_structure
from data_adapter.databus import download_collection

from utils import PATH_TEST_FILES, PATH_TMP, check_if_csv_dirs_equal
from data_adapter_oemof.build_datapackage import (
    build_datapackage,
    save_datapackage_to_csv,
)


def test_struct():
    download_collection(
        "https://energy.databus.dbpedia.org/felixmaur/collections/hack-a-thon/"
    )

    es_structure = get_energy_structure(structure="structure")

    processes = es_structure.keys()

    return es_structure


def test_process_data(es_struct=test_struct()):
    processes = es_struct.keys()
    process_data = {
        process: get_process("hack-a-thon", process) for process in processes
    }
    datapackage = build_datapackage()
