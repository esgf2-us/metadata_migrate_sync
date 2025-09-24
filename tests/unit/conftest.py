import pathlib

import pytest

import json
from metadata_migrate_sync.database import Query
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Update the original_datadir to specify where the expected values go
@pytest.fixture(scope="session")
def original_datadir():
    return pathlib.Path(__file__).parent / "test_datasets"



@pytest.fixture
def gmeta_sample_wrong_type(datadir):
    with open(datadir / "test_gmeta_sample.json" ) as fh:
        content = json.loads(fh.read())

    return content


@pytest.fixture
def gmeta_sample_right_type(gmeta_sample_wrong_type):
    content = gmeta_sample_wrong_type
    
    content["gmeta"][0]["entries"][0]["content"]["version"] = int(
        content["gmeta"][0]["entries"][0]["content"]["version"]
    )
    content["gmeta"][0]["entries"][0]["content"]["deprecated"] = False
    return content


@pytest.fixture
def mock_globus_query(mocker, gmeta_sample_wrong_type):
    """Fixture to mock GlobusQuery for all tests."""
    mock_class = mocker.patch('metadata_migrate_sync.replica.GlobusQuery')
    mock_instance = MagicMock()
    mock_class.return_value = mock_instance
    # Since gq.run() is used as an iterator, mock it to return an iterable

    mock_instance.run.return_value = [gmeta_sample_wrong_type]


    query = Query(
        project="my_project",
        project_type="research",
        index_id="some_index_id",
        query_str="climate data",
        query_type="search",
        query_time=2.5,
        date_range="2020-2021",
        query_datetime=datetime.utcnow(),
        numFound="1000",
        n_datasets=50,
        n_files=200,
        pages=10,
        rows=100,
        cursorMark="abc123",
        cursorMark_next="def456",
        n_failed=2,
        doc_size=1024
    )

    # cannot mock the query as the object_session will
    # return the mocked Query object, but cannot be ingested into the db
    #-mock_current_query = MagicMock()
    #-mock_current_query.index_id = "mock_index_123"
    #-mock_instance._current_query = mock_current_query
    mock_instance._current_query = query

    return {
        'class': mock_class,
        'instance': mock_instance
    }
