"""Sqlite database for index migrationa and sync."""

import pathlib
from datetime import datetime
from typing import Any, ClassVar, Optional

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    create_engine,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    relationship,
    sessionmaker,
)

from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.solr import SolrIndexes


# Create a base class for models
class Base(DeclarativeBase):
    """The base class for the migration database"""
    pass


class Index(Base):
    """The index table class."""
    __tablename__ = "index"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_id = Column(String, nullable=False, unique=True)
    index_name = Column(String, nullable=False)
    index_type = Column(String, nullable=False)

    query = relationship("Query", back_populates="index")
    ingest = relationship("Ingest", back_populates="index")


class Query(Base):
    """The query table class."""
    __tablename__ = "query"

    id = Column(Integer, primary_key=True, autoincrement=True)
    project = Column(String, nullable=False)
    project_type = Column(String, nullable=False)

    index_id = Column(String, ForeignKey("index.index_id"))
    index = relationship("Index", back_populates="query")

    query_str = Column(String, nullable=False)

    query_type = Column(String)
    query_time = Column(Numeric)
    date_range = Column(String)

    query_datetime = Column(DateTime, default=datetime.utcnow)
    numFound = Column(String)   # noqa N815
    n_datasets = Column(Integer)
    n_files = Column(Integer)

    pages = Column(Integer, unique=True)
    ingest = relationship("Ingest", back_populates="query")
    datasets = relationship("Datasets", back_populates="query")
    files = relationship("Files", back_populates="query")

    rows = Column(Integer)
    cursorMark = Column(String)   # noqa N815
    cursorMark_next = Column(String) # noqa N815
    n_failed = Column(Integer, default=0)
    doc_size = Column(Integer)


# success and n_failed are updated in the check code
class Ingest(Base):
    """The ingest table class."""
    __tablename__ = "ingest"

    id = Column(Integer, primary_key=True, autoincrement=True)
    n_ingested = Column(Integer)
    n_datasets = Column(Integer)
    n_files = Column(Integer)

    index_id = Column(String, ForeignKey("index.index_id"))
    index = relationship("Index", back_populates="ingest")

    pages = Column(Integer, ForeignKey("query.pages"))
    query = relationship("Query", back_populates="ingest")

    task_id = Column(String)
    ingest_response = Column(String)
    ingest_datetime = Column(DateTime, default=datetime.utcnow)
    submitted = Column(Integer, default=0)

    succeeded = Column(Integer, default=0)
    n_failed = Column(Integer)


class Datasets(Base):
    """The dataset table class."""
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pages = Column(Integer, ForeignKey("query.pages"))
    query = relationship("Query", back_populates="datasets")

    datasets_id = Column(String)
    source_index = Column(String)
    target_index = Column(String)
    uri = Column(String)
    success = Column(Integer)


class Files(Base):
    """The file table class."""
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pages = Column(Integer, ForeignKey("query.pages"))
    query = relationship("Query", back_populates="files")
    files_id = Column(String)
    size = Column(Integer)
    source_index = Column(String)
    target_index = Column(String)
    uri = Column(String)
    success = Column(Integer)


class MigrationDB:
    """it is a singleton class."""

    _instance: ClassVar[Optional["MigrationDB"]] = None
    initialized: bool
    def __new__(cls, *args:Any, **kwargs:Any) -> "MigrationDB":  # noqa D102
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.initialized = False
        return cls._instance


    def __init__(self, db_filename: str | pathlib.Path, insert_index: bool):
        if not self.initialized:

            self._DATABASE_URL = f"sqlite:///{db_filename}"
            self._engine = create_engine(self._DATABASE_URL, echo=False)
            Base.metadata.create_all(self._engine)

            logger = provenance.get_logger(__name__)


            logger.info("this is the only initalization in database")

            if insert_index:
                self.DBsession = sessionmaker(bind=self._engine)
                with self.DBsession() as session:

                    if session.query(Index).count() > 0:
                        pass
                    else:

                        # solr indexes
                        index_list = []
                        for n, s in SolrIndexes.indexes.items():

                            index_list.append(
                                Index(
                                    index_id=s.index_id,
                                    index_name=s.index_name,
                                    index_type=s.index_type,
                                )
                            )
                        # globus indexes
                        for d in [
                            GlobusClient._client_test["indexes"],
                            GlobusClient._client_prod_migration["indexes"],
                            GlobusClient._client_prod_sync["indexes"],
                        ]:
                            if isinstance(d, dict):
                                for name, index in d.items():
                                    index_list.append(
                                        Index(
                                            index_id=index, index_name=name, index_type="globus"
                                        )
                                    )

                        session.add_all(index_list)
                        session.commit()
    @classmethod
    def get_session(cls) -> sessionmaker[Session]:
        """Get the database session."""
        if cls._instance is not None or (not hasattr(cls._instance, "DBsession")):
            cls._instance.DBsession = sessionmaker(bind=cls._instance._engine)
        else:
            raise ValueError("database is not initialized")

        return cls._instance.DBsession
