from metadata_migrate_sync.esgf_index_schema.schema_solr import FileDocs, DatasetDocs

from typing import Any, Literal

from metadata_migrate_sync.provenance import provenance

def convert_to_esgf_1_5(solr_doc: FileDocs | DatasetDocs, metatype: Literal["datasets", "files"]) -> dict[Any, Any] | None:

    esgf_doc = solr_doc

    # change.md #3a78b9f
    if "index_node" in esgf_doc:
        esgf_doc["index_node"] = "us-index"

    # remove the uri in datasets
    if "url" in esgf_doc and metatype == "datasets":
        _ = esgf_doc.pop('url', None)

    # filter out ornl copies
    if provenance._instance.source_index_name == "ornl":
        if "data_node" in esgf_doc:
            data_node = esgf_doc["data_node"]

            if ".llnl.gov" in data_node or ".anl.gov" in data_node or "esgf-node.ornl.gov" == data_node:
                return esgf_doc 
            else:
                return None 

    # for e3sm
    if provenance._instance.source_index_name == "llnl":
        if "source_id" in esgf_doc:
            source_id = esgf_doc["source_id"][0]
            if source_id == "E3SM-2-1": # or "E3SM-2-1" in source_id
                return None

    return esgf_doc
