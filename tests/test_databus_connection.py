from data_adapter.databus import download_collection


def test_download_collection():
    collection_url = (
        "https://energy.databus.dbpedia.org/felixmaur/collections/modex_test_renewable"
    )

    download_collection(
        collection_url=collection_url,
    )
