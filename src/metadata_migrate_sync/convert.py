from metadata_migrate_sync.esgf_index_schema.schema_solr import FileDocs, DatasetDocs

from typing import Any


def convert_to_esgf_1_5(solr_doc: FileDocs | DatasetDocs) -> dict[Any, Any]:

    esgf_doc = solr_doc

    # change.md #3a78b9f
    if "index_node" in esgf_doc:
        esgf_doc["index_node"] = "us-index"

    return esgf_doc
