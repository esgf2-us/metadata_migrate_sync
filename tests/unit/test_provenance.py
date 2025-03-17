from metadata_migrate_sync.provenance import provenance

import logging
import pytest


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
    }

def test_provenance_prov(task_info):

    prov = provenance(**task_info)

    assert "task_name" in prov.model_dump()
    assert "ingest_index_type" in prov.model_dump()


def test_proveneance_log(task_info):

    prov = provenance(**task_info)
    logger = prov.get_logger()
    logger.setLevel(logging.INFO)

    logger.debug('This is a debug message')
    logger.info('This is an info message')
    logger.warning('This is a warning message')
    logger.error('This is an error message')
    logger.critical('This is a critical message')
