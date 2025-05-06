"""Ingest module."""
import json
from datetime import datetime
import logging
from typing import Any, Literal
from uuid import UUID

from globus_sdk import GlobusHTTPResponse, SearchClient
from pydantic import (
    BaseModel,
    validate_call,
)
from requests import Response
from sqlalchemy.orm import object_session

from metadata_migrate_sync.database import Datasets, Files, Ingest, MigrationDB, Query
from metadata_migrate_sync.globus import GlobusClient, GlobusIngestModel
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite


logger = logging.getLogger(__name__)


class BaseIngest(BaseModel):
    """ingestion base model."""

    end_point: str | UUID | None = None
    ep_type: Literal["solr", "globus"] = "globus"
    ep_name: str
    project: ProjectReadOnly | ProjectReadWrite


class GlobusIngest(BaseIngest):
    """Globus ingestion model."""

    _submitted: bool = False
    _response_data: dict[Any, Any] = {}

    # from globus2solr
    def ingest(self, gingest: dict[str, Any], dry_run: bool = False) -> None:
        """Ingest documents to a globus index using globus search client."""

        current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("start the inject now at " + current_timestr)

        GlobusIngestModel.model_validate(gingest)

        gc = GlobusClient.get_client(name=self.ep_name)
        sc = gc.search_client

        if self.ep_name == "stage":
             _globus_index_id = gc.indexes[self.project.value]
        else:
             _globus_index_id = gc.indexes[self.ep_name]

        if str(self.end_point) != str(_globus_index_id):
            logger.error("end_point is not consistent with ep_name")
            raise ValueError("end_point is not consistent with ep_name")

        if dry_run:
            r = Response()
            r._content = b"{'acknowledged': True, 'success': True, 'task_id': '1234567890'}"
            response = GlobusHTTPResponse(r)
        else:
            response = sc.ingest(_globus_index_id, gingest)

        self._response_data = response.data

        if response.data["acknowledged"] and response.data["success"]:
            self._submitted = True

            current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info("the ingestion submitted successfully at " + current_timestr)
        else:
            logger.info("the ingestion submission failed at " + current_timestr)

    def prov_collect(
        self,
        docs: list[dict[str, Any]],
        review: bool,
        current_query: Any,  # noqa ANN401
        metatype: Literal["files", "datasets"],
        batch_num: int = -1,
    ) -> None:
        """Provenance collection and database updation."""

        if not self._submitted:

            logger.info(
                "the submission failed, so no need to write ingest/datafiles/files tabs"
            )
            return None

        if review:

            logger.info(
                "in review mode, the failed ingest record will be updated by the new task_id"
            )
            logger.info(
                "the succuss record in datasets/files tabs will be updated by check_ingest"
            )
            DBsession = MigrationDB.get_session()
            with DBsession() as session:
                current_ingest: Ingest = session.query(Ingest).filter_by(
                    pages=current_query.pages
                ).first()

                # for batched ingestion, it will be array. how to do?

                current_ingest.task_id = self._response_data.get("task_id")
                current_ingest.ingest_response = json.dumps(self._response_data)
                current_ingest.submitted = current_ingest.submitted + 1

            return

        # write to db

        logger.info("ingest sumbmitted, so add the files/datasets to the tabs")

        DBsession = MigrationDB.get_session()
        with DBsession() as session:

            if object_session(current_query) is None:
                last_query = session.query(Query).order_by(Query.id.desc()).first()
            else:
                last_query = current_query

            n_datasets = 0
            n_files = 0
            for doc in docs:
                if metatype == "files":
                    urls = ",".join(doc.get("url")) if "url" in doc else "NoURL"
                    files_obj = Files(
                        query=last_query,
                        source_index=last_query.index_id,
                        target_index=str(self.end_point),
                        files_id=doc.get("id"),
                        size=(doc.get("size") if "size" in doc else -1),
                        uri=urls,
                        success=-9 if "skip_ingest" in doc else 0,
                    )
                    n_files += 1

                    session.add(files_obj)

                elif metatype == "datasets":
                    datasets_obj = Datasets(
                        query=last_query,
                        source_index=last_query.index_id,
                        target_index=str(self.end_point),
                        datasets_id=doc.get("id"),
                        success=-9 if "skip_ingest" in doc else 0,
                    )
                    session.add(datasets_obj)
                    n_datasets += 1

            if self._response_data:
                task_id = self._response_data.get("task_id")
                ingest_response = json.dumps(self._response_data)
            else:
                task_id = "skip"
                ingest_response = "skip"

            ingest_obj = Ingest(
                n_ingested=len(docs),
                n_datasets=n_datasets if batch_num == -1 or n_datasets > 0 else batch_num,
                n_files=n_files,
                index_id=str(self.end_point),
                task_id=task_id,
                ingest_response=ingest_response,
                query=last_query,
                submitted=1,
            )

            session.add(ingest_obj)
            session.commit()
        logger.info("add records to the files/datasets to the tabs successfully")


@validate_call
def generate_gmeta_list(
    docs: list[dict[str, Any]], metatype: Literal["files", "datasets"]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Generate gmeta list for ingestion from solr documents."""
    from metadata_migrate_sync.convert import convert_to_esgf_1_5

    gmeta_entries = []
    all_entries = []
    for doc in docs:
        converted_doc = convert_to_esgf_1_5(doc, metatype)
        if converted_doc is None:
            doc["skip_ingest"] = True
            all_entries.append(doc)
            continue

        #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        #-converted_doc["_timestamp"] = datetime.now(
        #-     timezone.utc).isoformat().replace("+00:00", "Z")

        gmeta_dict = {
            "id": metatype[:-1],
            "subject": converted_doc.get("id"),
            "visible_to": ["public"],
            "content": converted_doc,
        }
        gmeta_entries.append(gmeta_dict)
        all_entries.append(doc)

    gmeta_list = {
        "ingest_type": "GMetaList",
        "ingest_data": {"gmeta": gmeta_entries},
    }

    return gmeta_list, all_entries


@validate_call
def generate_gmeta_list_globus(
    gdoc: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Generate gmeta list for ingestion from globus documents."""
    gmeta_entries = []

    # need to add the "skip_ingest: True"
    # in the content if there are skips
    gmeta_entries_skipped: list[dict[str, Any]] = []

    for g in gdoc["gmeta"]:

        #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        #g["entries"][0]["content"]["_timestamp"] = datetime.now(
        #     timezone.utc).isoformat().replace("+00:00", "Z")

        gmeta_dict = {
            "id": g["entries"][0]["entry_id"],
            "subject": g["subject"],
            "visible_to": ["public"],
            "content":g["entries"][0]["content"],
        }
        gmeta_entries.append(gmeta_dict)

    gmeta_ingest = {
        "ingest_type": "GMetaList",
        "ingest_data": {"gmeta": gmeta_entries},
    }

    gmeta_ingest_skipped = {
        "ingest_type": "GMetaList",
        "ingest_data": {"gmeta": gmeta_entries_skipped},
    }

    return gmeta_ingest, gmeta_ingest_skipped
