"""Document conversion module."""
from typing import Any, Literal

from metadata_migrate_sync.esgf_index_schema.schema_solr import DatasetDocs, FileDocs
from metadata_migrate_sync.provenance import provenance


def convert_to_esgf_1_5(
    solr_doc: FileDocs | DatasetDocs | dict[str, Any],
    metatype: Literal["datasets", "files"]
) -> dict[Any, Any] | None:
    """Convert solr documents to the ESGF-1.5 documents."""
    esgf_doc = solr_doc

    # change.md #3a78b9f
    if "index_node" in esgf_doc:
        esgf_doc["index_node"] = "us-index"

    # remove the uri in datasets
    if "url" in esgf_doc and metatype == "datasets":
        _ = esgf_doc.pop('url', None)

    # filter out ornl copies
    if provenance._instance.source_index_name == "ornl" and "data_node" in esgf_doc:
        data_node = esgf_doc["data_node"]

        if ".llnl.gov" in data_node or ".anl.gov" in data_node or data_node == "esgf-node.ornl.gov":
            return esgf_doc
        else:
            return None

    # for e3sm
    if provenance._instance.source_index_name == "llnl" and "source_id" in esgf_doc:

        source_id = esgf_doc["source_id"][0]
        if source_id == "E3SM-2-1": # or "E3SM-2-1" in source_id
            return None

    return esgf_doc
