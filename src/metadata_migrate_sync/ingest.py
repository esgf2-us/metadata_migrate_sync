from pydantic import (
    BaseModel,
    field_validator,
    validate_call,
)
from typing import Any, Literal
from uuid import UUID
import json
from globus_sdk import SearchClient, GlobusError

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

    def prov_collect(
        self, gingest: dict[str, Any], review: bool, current_query: Any
    ) -> None:

        if not self._submitted:

            # log
            return None

        if review:
            with MigrationDB.get_session() as session:
                current_ingest = session.query(Ingest).filterby(
                    pages=current_query.pages
                )

                current_ingest.task_id = self._response_data.get("task_id")
                current_ingest.ingest_response = json.dumps(self._response_data)
                current_ingest.submitted = current_ingest.submitted + 1

            return

        # write to db

        with MigrationDB.get_session() as session:
            # last_query = session.query(Query).order_by(Query.id.desc()).first()
            last_query = current_query

            n_datasets = 0
            n_files = 0
            for gmeta in gingest["ingest_data"]["gmeta"]:

                if gmeta.get("id") == "file":
                    files_obj = Files(
                        query=last_query,
                        source_index=last_query.index_id,
                        target_index=str(self.end_point),
                        files_id=gmeta.get("subject"),
                        size=(
                            gmeta.get("content").get("size")
                            if "size" in gmeta.get("content")
                            else -1
                        ),
                        uri=",".join(gmeta.get("content").get("url")),
                        success=0,
                    )
                    n_files += 1

                    session.add(files_obj)

                elif gmeta.get("id") == "dataset":
                    datasets_obj = Datasets(
                        query=last_query,
                        source_index=last_query.index_id,
                        target_index=str(self.end_point),
                        datasets_id=gmeta.get("subject"),
                        uri=",".join(gmeta.get("content").get("url")),
                        success=0,
                    )
                    session.add(datasets_obj)
                    n_datasets += 1

            ingest_obj = Ingest(
                n_ingested=len(gingest["ingest_data"]["gmeta"]),
                n_datasets=n_datasets,
                n_files=n_files,
                index_id=str(self.end_point),
                task_id=self._response_data.get("task_id"),
                ingest_response=json.dumps(self._response_data),
                query=last_query,
                submitted=1,
            )

            session.add(ingest_obj)
            session.commit()


@validate_call
def generate_gmeta_list(
    docs: list[dict[str, Any]], metatype: Literal["files", "datasets"]
) -> dict[str, Any]:
    """generate gmeta list for ingestion"""

    from metadata_migrate_sync.convert import convert_to_esgf_1_5

    gmeta_entries = []
    for doc in docs:
        converted_doc = convert_to_esgf_1_5(doc)
        gmeta_dict = {
            "id": metatype[:-1],
            "subject": doc.get("id"),
            "visible_to": ["public"],
            "content": doc,
        }
        gmeta_entries.append(gmeta_dict)

    gmeta_list = {
        "ingest_type": "GMetaList",
        "ingest_data": {"gmeta": gmeta_entries},
    }

    return gmeta_list
