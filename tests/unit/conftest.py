import pathlib

import pytest


# Update the original_datadir to specify where the expected values go
@pytest.fixture(scope="session")
def original_datadir():
    return pathlib.Path(__file__).parent / "test_datasets"

