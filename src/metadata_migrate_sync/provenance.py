from pydantic import BaseModel, AnyUrl, field_validator
from typing import Literal, ClassVar
from datetime import datetime
import pathlib
import os
import sys
import platform

import uuid
import logging
import logging.config
from uuid import UUID

from importlib.metadata import distributions

class provenance(BaseModel):
    """
    provenance class for the meta data ingest and sync
    """

    _log_file: ClassVar[str] = "test.log"

    task_name: Literal["migrate", "ingest", "sync"]
    source_index_id: str | UUID | AnyUrl
    source_index_type: Literal["solr", "global"]
    source_index_schema: str = "solr"

    ingest_index_id: str | UUID
    ingest_index_type: Literal["solr", "globus"]
    ingest_index_schema: str = "ESGF1.5"

    log_file: str | pathlib.Path = "test.log"
    prov_file: str | pathlib.Path = "test.json"
    db_file: str | pathlib.Path = "test.db"

    sucessful: bool = False

    source_index_query: str | None = None
    type_query: Literal["Datasets", "Files"] = "Datasets"
    success_query: bool = False

    timestamp_query: datetime | None = None
    time_query: float | None = None
    pass_validate_query: bool = False

    pass_validate_ingest: bool = False
    timestamp_ingest: datetime | None = None

    operation_system: str = platform.platform()

    os_environment: dict[str, str | None] = {
        "USERNAME": os.environ.get("USERNAME"),
        "SHELL": os.environ.get("SHELL"),
    }
    python_version: str = sys.version

    python_modules: dict[str, str] | None = {
        p.metadata["Name"]: p.version for p in distributions()
    }

    @field_validator("log_file")
    @classmethod
    def set_log_file(cls, value:str) -> str:
        cls._log_file = value
        return value 


    @classmethod
    def get_logger(cls) -> logging.Logger:

        logging_config = {
            "version": 1,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "console": {
                    "level": "INFO",
                    "class": "logging.StreamHandler",
                    "formatter": "standard",
                },
                "file": {
                    "level": "DEBUG",
                    "class": "logging.FileHandler",
                    "filename": cls._log_file,
                    "formatter": "standard",
                },
            },
            "loggers": {
                "": {
                    "handlers": ["file"],
                    "level": "DEBUG",
                    "propagate": True,
                },
            },
        }

        logging.config.dictConfig(logging_config)
        logger = logging.getLogger(__name__)
        return logger
