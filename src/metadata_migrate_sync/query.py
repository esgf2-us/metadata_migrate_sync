"""query for solr and globus both"""

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, HTTPError, ConnectionError, RetryError
from urllib3 import Retry
from uuid import UUID
from pydantic import BaseModel
from typing import Literal, Any, Generator
import sys
import json

from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.database import MigrationDB, Index, Query, Ingest
from metadata_migrate_sync.provenance import provenance

from metadata_migrate_sync.globus import GlobusClient

params_search = {
    "sort": "id asc",
    "rows": 2,
    "cursorMark": "*",
    "wt": "json",
}


class BaseQuery(BaseModel):
    end_point: str | UUID
    ep_type: Literal["solr", "globus"]
    ep_name: str
    project: ProjectReadOnly | ProjectReadWrite


class SolrQuery(BaseQuery):
    """query solr index"""

    query: dict[str, Any]

    _restart: bool = False
    _review: bool = False
    _review_list: list[Any]

    _current_query: Any | None = None
   
    #_numFound: int

    def get_cursormark(self, review: bool = False) -> None:
        """get the cursormark from the database file"""


        logger = provenance._instance.get_logger(__name__)
        if review:
            # get all the failed cases in the database, re-query and re-ingest

            self._veview = True
            with MigrationDB.get_session() as session:
                 failed_ingests = session.query(Ingest).filter_by(succeeded = 0).all()

                 failed_querys = failed_ingests.query

                 self._review_list = []
                 for ingest in failed_ingests:
                     failed_query = session.query(Query).filter_by(pages = ingest.pages).first()
                     self._review_list.append(failed_query)

            # set the query
            if len(self._review_list) == 0:
                logger.info("there are no failed ingest")
                sys.exit()
            else:
                self._current_query = self._review_list.pop()
                self.query["cursorMark"] = self._current_query.cursorMark


        else:
            # determine the cursorMark
            with MigrationDB.get_session() as session:

                last_query = session.query(Query).order_by(Query.id.desc()).first()
                if last_query is None:  # new start
                    self.query["cursorMark"] = "*"
                    self._current_query = None

                    logger.info("The query is new start")
                else:

                    self._current_query = last_query
                    ingest_obj = last_query.ingest

                    if len(ingest_obj) == 0:
                        self.query["cursorMark"] = last_query.cursorMark
                        self._restart = True

                        logger.info("The query is restart with no corresponding ingest")
                    else:
                        filter_ingest = [
                            ing for ing in ingest_obj if ing.pages == last_query.pages
                        ]

                        if len(filter_ingest) == 1: 
                            if filter_ingest[0].submitted == 0:
                                self.query["cursorMark"] = last_query.cursorMark
                                self._restart = True

                                logger.info("The query is restart with failure ingest")
                            else:
                                self.query["cursorMark"] = last_query.cursorMark_next
                                self._restart = False

                                logger.info("The query is restart in the next cursormark")
                        else:
                            self.query["cursorMark"] = last_query.cursorMark
                            self._restart = True

                            logger.info("The query should not happened")


    @staticmethod
    def _make_request(url: str, params: dict[str, Any], is_test: bool = False) -> tuple[Any, Any, Any] | None | int:
        """
        Make an HTTP GET request with retry logic.

        Args:
            url (str): The URL to make the request to.
            params (dict[str, Any]): Query parameters for the request.

        Returns:
            dict[str, Any] | None: The JSON response if successful, None otherwise.
        """
        logger = provenance.get_logger(__name__)

        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        try:
            response = http.get(url, params=params)
            response.raise_for_status()
            response_time = response.elapsed.total_seconds()

            if is_test:
                return response.status_code
            else:
                return response.json(), response_time, response.url
        except ConnectionError as e:
            logger.error(f"Failed to connect to {url}: {e}")
            raise ConnectionError(f"Failed to connect to {url}: {e}") from e

        except RetryError as e:
            logger.error(f"maximum fails to {url}: {e}")
            raise RetryError(f"maximum fails to {url}: {e}") from e

        except RequestException as e:
            logger.error(f"Request failed at {url}: {e}")
            return None

    def _process_response(self, response_json: dict[str, Any]) -> Generator[Any, None, None]:
        """
        Process the JSON response and yield the results.

        Args:
            response_json (dict[str, Any]): The JSON response from the API.

        Yields:
            Generator[Any, None, None]: The documents from the response.
        """
        logger = provenance.get_logger(__name__)

        docs = response_json.get("response", {}).get("docs", [])
        yield docs

        # Check if this is the last page
        if self.query["cursorMark"] == response_json.get("nextCursorMark"):
            logger.info("Reached the last page.")
            return

        # Get the next page in the review mode
        if self._review:
            if not self._review_list:
                logger.info("No more pages to review.")
                return
            self._review_page, self._review_cursor = self._review_list.pop()
            self.query["cursorMark"] = self._review_cursor
        else:
            self.query["cursorMark"] = response_json.get("nextCursorMark")

        # Continue processing the next page
        yield from self.run()


    def run(self) -> Generator[Any, None, None]:
        """
        Query solr index in a paginated manner.

        Yields:
            Generator[Any, None, None]: The docs from each page.
        """

        result = self._make_request(self.end_point, self.query)


        if result:
            response_json, response_time, response_url = result
            self.prov_collect(response_url, response_time, response_json) 
            yield from self._process_response(response_json)
       



    def prov_collect(
        self, req_url: str, req_time: float, response: dict[Any, Any]
    ) -> None:
        """collect prov and db"""

        self._numFound = response.get("response").get("numFound")

        with MigrationDB.get_session() as session:

            if self._review:
                pass

            elif self._restart:
                prepage = session.query(Query).order_by(Query.id.desc()).first()
                prepage.n_failed = prepage.n_failed + 1
                prepage.query_time = req_time
                session.commit()
                self._restart = False

            else:
                ind = (
                    session.query(Index)
                    .filter(Index.index_name == self.ep_name)
                    .first()
                )
                prepage = session.query(Query).order_by(Query.id.desc()).first()

                query_obj = Query(
                    project=self.project,
                    project_type=(
                        "readonly"
                        if isinstance(self.project, ProjectReadOnly)
                        else "readwrite"
                    ),
                    query_str=req_url.split("?")[1],
                    query_type="solr",
                    query_time=req_time,
                    date_range="[* To *]" if not self.query.get("fq") else self.query.get("fq"),
                    numFound=response.get("response").get("numFound"),
                    n_datasets=(
                        0
                        if "solr/files" in self.end_point
                        else len(response.get("response").get("docs"))
                    ),
                    n_files=(
                        0
                        if "solr/datasets" in self.end_point
                        else len(response.get("response").get("docs"))
                    ),
                    pages=prepage.pages + 1 if prepage != None else 1,
                    rows=self.query.get("rows"),
                    cursorMark=self.query.get("cursorMark"),
                    cursorMark_next=response.get("nextCursorMark"),
                    n_failed=0,
                    index=ind,
                )

                session.add(query_obj)
                session.commit()
                curpage = session.query(Query).order_by(Query.id.desc()).first()
                self._current_query = curpage


class GlobusQuery(BaseQuery):
    """query globus index"""

    def run(self):

        gc = GlobusClient()
        cm = gc.get_client(name = "test")
        sc = cm.search_client
        sq = cm.search_query

        sq.set_query("us-index")


        if self.ep_name == "test":
            _globus_index_id = cm.indexes[self.ep_name]
        else:
            _globus_index_id = cm.indexes[self.project.value]

        r = sc.post_search(_globus_index_id, sq)
        with open('data.json', 'w') as f:
            json.dump(r.data, f)
