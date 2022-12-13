import os

from data_adapter.databus import download_collection
from data_adapter_oemof import settings


def test_download_collection():
    collection_url = "https://energy.databus.dbpedia.org/felixmaur/collections/modex_test_renewable"
    collection_output_directory=settings.COLLECTIONS_DIR
    if not os.path.exists(collection_output_directory):
        os.makedirs(collection_output_directory)
    download_collection(
        collection_url=collection_url,
        collection_output_directory=collection_output_directory,
    )
