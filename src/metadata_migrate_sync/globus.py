import pathlib
from typing import Any
from uuid import UUID

from globus_sdk import (
    NativeAppAuthClient,
    RefreshTokenAuthorizer,
    SearchClient,
    SearchQuery,
)
from globus_sdk.tokenstorage import SimpleJSONFileAdapter
from pydantic import BaseModel, ConfigDict

from metadata_migrate_sync.project import ProjectReadWrite
from metadata_migrate_sync.provenance import provenance


# from Lucasz and Nate code with some minor changes
def get_authorized_search_client(
    app_client_id: UUID | str, token_name: str = "token.json"
) -> SearchClient:
    """Return a transfer client authorized to make transfers."""
    config_path = pathlib.Path.home() / ".ssh"
    config_path.mkdir(parents=True, exist_ok=True)
    token_adapter = SimpleJSONFileAdapter(config_path / token_name)
    app_client = NativeAppAuthClient(app_client_id)

    if token_adapter.file_exists():
        tokens = token_adapter.get_token_data("search.api.globus.org")
    else:
        app_client.oauth2_start_flow(
            requested_scopes=["urn:globus:auth:scope:search.api.globus.org:all"],
            refresh_tokens=True,
        )
        authorize_url = app_client.oauth2_get_authorize_url()
        print(
            f"""
All interactions with Globus must be authorized. To ensure that we have permission to faciliate your transfer, please open the following link in your browser.

{authorize_url}

You will have to login (or be logged in) to your Globus account. Globus will also request that you give a label for this authorization. You may pick anything of your choosing. After following the instructions in your browser, Globus will generate a code which you must copy and paste here and then hit <enter>.\n"""
        )
        auth_code = input("> ").strip()
        token_response = app_client.oauth2_exchange_code_for_tokens(auth_code)
        token_adapter.store(token_response)
        tokens = token_response.by_resource_server["search.api.globus.org"]

    authorizer = RefreshTokenAuthorizer(
        tokens["refresh_token"],
        app_client,
        access_token=tokens["access_token"],
        expires_at=tokens["expires_at_seconds"],
        on_refresh=token_adapter.on_refresh,
    )
    search_client = SearchClient(authorizer=authorizer)
    return search_client


class ClientModel(BaseModel):
    """a client model includes many aspects and indexes"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    app_client_id: UUID
    token_name: str
    search_client: SearchClient | None
    search_query: SearchQuery
    indexes: dict[str, UUID]

    def list_index(self) -> dict[str, Any]:
        index_dict = {}
        for name, index in self.indexes.items():
            r = self.search_client.get_index(index)
            index_dict[name] = r.data

        return index_dict


class GlobusClient:
    """a class to hold ESGF1.5 indexes and methods
    indexes are from https://github.com/esgf2-us/esgf-1.5-design/blob/main/indexes.md
    commit: 8630c26
    """

    globus_clients: dict[str, ClientModel] = {}

    _client_test = {
        "app_client_id": "fe862e63-f3bb-457a-9662-995832bb692f",
        "token_name": "test_index.json",
        "search_client": None,
        "search_query": SearchQuery(""),
        "indexes": {
            "test_1": "52eff156-6141-4fde-9efe-c08c92f3a706",
            "test": "a2f1ac3a-bb7c-4be2-b3f5-cbd2b6a3e17b",
        },
    }

    _client_prod_migration = {
        "app_client_id": "bb163ebc-7feb-490e-8296-e572a0622e3c",
        "token_name": "metadata_migration_sync_tokens.json",
        "search_client": None,
        "search_query": SearchQuery(""),
        "indexes": {
            "public": "c0173b0c-5587-437a-a912-ef09b6d14e9c",
        },
    }

    _client_prod_sync = {
        "app_client_id": "bb163ebc-7feb-490e-8296-e572a0622e3c",
        "token_name": "metadata_migration_sync_tokens.json",
        "search_client": None,
        "search_query": SearchQuery(""),
        "indexes": {
            "backup": "a37bc34d-de15-493b-9221-b95b13114fd8",
            ProjectReadWrite.CMIP6PLUS.value: "1f385759-596e-4085-8d79-5b1dfedd1ca2",
            ProjectReadWrite.DRCDP.value: "c7cc5d1e-5740-49c2-aa10-fe31f3bcb035",
            ProjectReadWrite.E3SM.value: "f5a2d874-30ef-40a0-8c8d-e2498f3bd026",
            ProjectReadWrite.INPUT4MIPS.value: "3c71c174-c8c8-43e5-994c-10dd4251579a",
            ProjectReadWrite.OBS4MIPS.value: "3cfaa44b-e549-487e-8058-5923aaf095b4",
        },
    }
    globus_clients["test"] = ClientModel(**_client_test)
    globus_clients["prod-migration"] = ClientModel(**_client_prod_migration)
    globus_clients["prod-sync"] = ClientModel(**_client_prod_sync)

    def __init__(
        self, client_name: str | None = None, client_model: ClientModel | None = None
    ):

        if client_name == None or client_model == None:
            return None
        else:
            if client_name not in ["test", "prod-migration", "prod-sync"]:
                self.globus_clients[client_name] = client_model
            else:
                raise ValueError(f"{client_name} has been used")

    @classmethod
    def get_client(cls, name: str = "test") -> ClientModel:

        logger = provenance.get_logger(__name__)

        if name == "public":
            index_name = "prod-migration"
        elif name == "stage":
            index_name = "prod-sync"
        elif name == "test":
            index_name = "test"
        elif name == "all-prod":
            index_name = "prod-all"

        if index_name == "prod-all":
            client_prod_all = {}

            client_prod_all = dict(cls._client_prod_migration)
            client_prod_all["indexes"] = dict(cls._client_prod_migration["indexes"])
            client_prod_all["indexes"].update(cls._client_prod_sync["indexes"])

            if client_prod_all["search_client"] is None:

                client_prod_all["search_client"] = get_authorized_search_client(
                    client_prod_all["app_client_id"],
                    client_prod_all["token_name"],
                )


            return ClientModel(**client_prod_all)


        else:
            if cls.globus_clients[index_name].search_client == None:
                logger.info(f"no search client and request for the index {index_name}")

                cls.globus_clients[index_name].search_client = get_authorized_search_client(
                    cls.globus_clients[index_name].app_client_id,
                    cls.globus_clients[index_name].token_name,
                )

            logger.info(f"return the search client with the name {name}.")

            return cls.globus_clients[index_name]

