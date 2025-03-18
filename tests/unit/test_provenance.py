from metadata_migrate_sync.provenance import provenance

import logging
import pytest
import os


@pytest.fixture
def task_info():
    return {
        "task_name": "migrate",
        "source_index_id": "esgf-node.ornl.gov",
        "source_index_type": "solr",
        "source_index_schema": "solr",
        "ingest_index_id": "c123975a-246d-421f-8819-4659edf91e44",
        "ingest_index_type": "globus",
        "ingest_index_schema": "ESGF1.5",
        "log_file": "test_prov.log",
    }

def test_provenance_prov(task_info):

    prov = provenance(**task_info)

    assert "task_name" in prov.model_dump()
    assert "ingest_index_type" in prov.model_dump()


def test_proveneance_log(task_info):

    prov = provenance(**task_info)
    logger = prov.get_logger(__name__)
    logger.setLevel(logging.INFO)

    assert provenance._log_file == os.path.basename(logger.root.handlers[0].baseFilename)

    logger.info("test")

