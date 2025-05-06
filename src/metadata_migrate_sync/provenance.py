"""Provenance module."""
import os
import platform
import sys
from importlib.metadata import distributions
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import AnyUrl, BaseModel
from pydantic._internal._model_construction import ModelMetaclass


class SingletonMeta(ModelMetaclass):
    """Metaclass to enforce singleton behavior while preserving Pydantic's functionality."""

    _instance = None

    def __call__(cls, *args: Any, **kwargs: Any):  # noqa ANN204 D102
        if not cls._instance:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class provenance(BaseModel, metaclass=SingletonMeta):
    """provenance class for the meta data ingest and sync.

    it is a singleton instance
    """

    task_name: Literal["migrate", "ingest", "sync"]
    source_index_id: str | UUID | AnyUrl
    source_index_type: Literal["solr", "globus"]
    source_index_name: Literal["ornl", "anl", "llnl", "stage", "test", "test_1"]
    source_index_schema: str = "solr"

    ingest_index_id: str | UUID
    ingest_index_type: Literal["solr", "globus"]
    ingest_index_name: Literal["test", "test_1", "public", "stage", "backup"]
    ingest_index_schema: str = "ESGF1.5"
    cmd_line: str

    prov_file: Path = Path("test.json")
    db_file: Path = Path("test.db")

    successful: bool = False

    operation_system: str = platform.platform()

    os_environment: dict[str, str | None] = {
        "USERNAME": os.environ.get("USERNAME"),
        "SHELL": os.environ.get("SHELL"),
        "HOSTNAME": os.environ.get("HOSTNAME"),
    }
    python_version: str = sys.version

    python_modules: dict[str, str] | None = {
        p.metadata["Name"]: p.version for p in distributions()
    }
