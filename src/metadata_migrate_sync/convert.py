
from metadata_migrate_sync.esgf_index_schema.schema_solr import FileDocs, DatasetDocs

from typing import Any


def convert_to_esgf_1_5 (solr_doc: FileDocs|DatasetDocs) -> dict[Any, Any]:

    esgf_doc = solr_doc

    # change.md
    #-if "index_node" in esgf_doc:
    #-    esgf_doc["index_node"] = "us-index"

    #-# strip or add?
    #-if "data_node" in esgf_doc and "dataset_id" in esgf_doc:
    #-    if '|' in esgf_doc.get("dataset_id"):
    #-        # strip?
    #-        esgf_doc["dataset_id"] = esgf_doc["dataset_id"].split('|')[0]
    #-    else: 
    #-        # add
    #-        pass 

    return esgf_doc
