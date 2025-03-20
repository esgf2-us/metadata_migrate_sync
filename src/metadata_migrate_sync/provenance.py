from pydantic import BaseModel, AnyUrl
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
from pydantic._internal._model_construction import ModelMetaclass



class SingletonMeta(ModelMetaclass):
    """
    Metaclass to enforce singleton behavior while preserving Pydantic's functionality.
    """
    _instance = None

    def __call__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__call__(*args, **kwargs)
        return cls._instance


class provenance(BaseModel, metaclass=SingletonMeta):
    """
    provenance class for the meta data ingest and sync
    it is a singleton instance
    """


    #_log_file: ClassVar[str] = "test_pov.log"

    task_name: Literal["migrate", "ingest", "sync"]
    source_index_id: str | UUID | AnyUrl
    source_index_type: Literal["solr", "global"]
    source_index_schema: str = "solr"

    ingest_index_id: str | UUID
    ingest_index_type: Literal["solr", "globus"]
    ingest_index_schema: str = "ESGF1.5"
    cmd_line: str

    log_file: str | pathlib.Path = "test.log"
    prov_file: str | pathlib.Path = "test.json"
    db_file: str | pathlib.Path = "test.db"

    successful: bool = False

    #source_index_query: str | None = None
    #type_query: Literal["Datasets", "Files"] = "Datasets"
    #success_query: bool = False

    #timestamp_query: datetime | None = None
    #time_query: float | None = None
    #pass_validate_query: bool = False

    #pass_validate_ingest: bool = False
    #timestamp_ingest: datetime | None = None

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

    #-_instance = None  # Class variable to store the singleton instance

    #-def __new__(cls, *args, **kwargs):
    #-    """
    #-    Override __new__ to ensure only one instance is created.
    #-    """
    #-    if cls._instance is None:
    #-        cls._instance = super().__new__(cls)
    #-    return cls._instance

    #-def __init__(self, **data):
    #-    """
    #-    Override __init__ to avoid reinitialization of the singleton instance.
    #-    """
    #-    if not hasattr(self, "initialized"):
    #-        super().__init__(**data)
    #-        self.initialized = True


    @classmethod
    def get_logger(cls, name:str) -> logging.Logger:

        if cls._instance is None:
            log_filename = "test.log"

        else:
            log_filename = cls._instance.log_file

        logging_config = {
            "version": 1,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(funcName)s - %(levelname)s - %(message)s",
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
                    "filename": log_filename,
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
        logger = logging.getLogger()

        return logger
