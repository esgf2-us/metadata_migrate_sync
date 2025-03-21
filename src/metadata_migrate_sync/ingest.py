from pydantic import (
    BaseModel,
    field_validator,
    validate_call,
)
from typing import Any, Literal
from uuid import UUID
import json
from globus_sdk import SearchClient, GlobusError
from datetime import datetime

from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.database import MigrationDB, Query, Ingest, Datasets, Files
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.esgf_index_schema.schema_solr import DatasetDocs, FileDocs

from metadata_migrate_sync.provenance import provenance


class GlobusMeta(BaseModel):
    id: Literal["file", "dataset"]  # files or datasets
    subject: str
    visible_to: list[str] | None = ["public"]
    content: DatasetDocs | FileDocs | dict[str, Any]


class GlobusIngestModel(BaseModel):
    ingest_type: Literal["GMetaList"] | None = "GMetaList"
    ingest_data: dict[str, list[GlobusMeta]]

    @field_validator("ingest_data")
    @classmethod
    def check_gmeta(cls, data: dict[Any, Any]) -> dict[Any, Any]:
        logger = provenance._instance.get_logger(__name__)
        if len(data.keys()) != 1 or "gmeta" not in data:
            logger.error("no gmeta in the dict")
            raise ValueError("no gmeta in the dict")
        return data


class BaseIngest(BaseModel):
    end_point: str | UUID | None = None
    ep_type: Literal["solr", "globus"] = "globus"
    ep_name: str
    project: ProjectReadOnly | ProjectReadWrite


class GlobusIngest(BaseIngest):

    _submitted: bool = False
    _response_data: dict[Any, Any]

    # from globus2solr
    def ingest(self, gingest: dict[str, Any]) -> None:

        logger = provenance._instance.get_logger(__name__)

        current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        logger.info("start the inject now at " + current_timestr)

        GlobusIngestModel.model_validate(gingest)

        gc = GlobusClient.get_client(name=self.ep_name)
        sc = gc.search_client

        if self.ep_name == "test":
            _globus_index_id = gc.indexes[self.ep_name]
        else:
            _globus_index_id = gc.indexes[self.project.value]

        if str(self.end_point) != str(_globus_index_id):
            logger.error("end_point is not consistent with ep_name")
            raise ValueError("end_point is not consistent with ep_name")

        if isinstance(sc, SearchClient):
            response = sc.ingest(_globus_index_id, gingest)
        else:
            logger.error("not a search client")
            raise ValueError("not a search client")

        self._response_data = response.data

        if response.data["acknowledged"] and response.data["success"]:
            self._submitted = True
        
            current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
            logger.info("the ingestion submitted successfully at " + current_timestr)
        else:
            logger.info("the ingestion submission failed at " + current_timestr)


    def prov_collect(
        self, docs: list[dict[str, Any]], review: bool, current_query: Any, metatype: Literal["files", "datasets"]
    ) -> None:

        logger = provenance._instance.get_logger(__name__)

        if not self._submitted:

            logger.info("the submission failed, so no need to write ingest/datafiles/files tabs")
            return None

        if review:

            logger.info("in review mode, the failed ingest record will be updated by the new task_id")
            logger.info("the succuss record in datasets/files tabs will be updated by check_ingest")
            with MigrationDB.get_session() as session:
                current_ingest = session.query(Ingest).filterby(
                    pages=current_query.pages
                )

                current_ingest.task_id = self._response_data.get("task_id")
                current_ingest.ingest_response = json.dumps(self._response_data)
                current_ingest.submitted = current_ingest.submitted + 1

            return

        # write to db

        logger.info("ingest sumbmitted, so add the files/datasets to the tabs")
 

        with MigrationDB.get_session() as session:
            # last_query = session.query(Query).order_by(Query.id.desc()).first()
            last_query = current_query

            n_datasets = 0
            n_files = 0
            for doc in docs:
                if metatype == "files":

                    if 'url' in doc:
                        urls = ",".join(doc.get("url"))
                    else:
                        urls = "NoURL"
                    files_obj = Files(
                        query=last_query,
                        source_index=last_query.index_id,
                        target_index=str(self.end_point),
                        files_id=doc.get("id"),
                        size=(
                            doc.get("size")
                            if "size" in doc
                            else -1
                        ),
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
                        #uri=",".join(doc.get("url")),
                        success=-9 if "skip_ingest" in doc else 0,
                    )
                    session.add(datasets_obj)
                    n_datasets += 1

            if self._response_data:
                task_id = self._response_data.get("task_id")
                ingest_response = json.dumps(self._response_data)
            else:
                task_id="skip"
                ingest_response="skip"

            ingest_obj = Ingest(
                n_ingested=len(docs),
                n_datasets=n_datasets,
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
    """generate gmeta list for ingestion"""

    from metadata_migrate_sync.convert import convert_to_esgf_1_5

    gmeta_entries = []
    all_entries = []
    for doc in docs:
        converted_doc = convert_to_esgf_1_5(doc, metatype)
        if converted_doc is None:
            doc["skip_ingest"] = True
            all_entries.append(doc)
            continue
        
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
