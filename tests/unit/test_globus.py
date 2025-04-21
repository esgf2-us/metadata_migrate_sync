import json

from metadata_migrate_sync.globus import GlobusClient


def test_globus_index(snapshot):

    gc = GlobusClient()
    cm = gc.get_client(name = "test")

    index_dict = cm.list_index()
    index_dict["test_1"].pop("size_in_mb")
    index_dict["test_1"].pop("num_subjects")
    index_dict["test_1"].pop("num_entries")
    actual_output = json.dumps({"test":index_dict["test_1"]})

    snapshot.assert_match(json.dumps(actual_output, indent=4)+"\n", "test_index_output.json")


def test_globus_search():

    gc = GlobusClient()
    cm = gc.get_client(name = "test")
    sc = cm.search_client
    sq = cm.search_query


    sq.add_filter("mip_era", ["CMIP6"]).add_filter("source_id", ["CESM2"])

    pages = sc.paginated.post_search("ea4595f4-7b71-4da7-a1f0-e3f5d8f7f062", sq)
    pages.limit = 10

    for page in pages:
        assert len(page.data['gmeta']) == 10
        break
