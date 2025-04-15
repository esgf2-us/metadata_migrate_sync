"""query for solr and globus both."""

import json
import logging
import sys
import time
from collections.abc import Generator
from typing import Any, Literal
from uuid import UUID

import requests
from globus_sdk import GlobusAPIError, SearchQuery
from pydantic import AnyUrl, BaseModel
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, RequestException, RetryError
from urllib3 import Retry

from metadata_migrate_sync.database import Files, Index, Ingest, MigrationDB, Query
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.provenance import provenance

params_search = {
    "sort": "id asc",
    "rows": 2,
    "cursorMark": "*",
    "wt": "json",
}


class BaseQuery(BaseModel):
    """Query base model."""

    end_point: str | UUID | AnyUrl
    ep_type: Literal["solr", "globus"]
    ep_name: str
    project: ProjectReadOnly | ProjectReadWrite


class SolrQuery(BaseQuery):
    """query solr index."""

    query: dict[str, Any]

    _restart: bool = False
    _review: bool = False
    _review_list: list[Any] = []

    _current_query: Any | None = None

    def get_cursormark(self, review: bool = False) -> None:
        """Get the cursormark from the database file."""
        logger = provenance._instance.get_logger(__name__) if provenance is not None else logging.getLogger()
        if review:
            # get all the failed cases in the database, re-query and re-ingest

            self._veview = True
            DBsession = MigrationDB.get_session()
            with DBsession() as session:
                failed_ingests = session.query(Ingest).filter_by(succeeded=0).all()

                self._review_list = []
                for ingest in failed_ingests:
                    failed_query = (
                        session.query(Query).filter_by(pages=ingest.pages).first()
                    )
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

            DBsession = MigrationDB.get_session()
            with DBsession() as session:

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

                                logger.info(
                                    "The query is restart in the next cursormark"
                                )
                        else:
                            self.query["cursorMark"] = last_query.cursorMark
                            self._restart = True

                            logger.info("The query should not happened")

    @staticmethod
    def _make_request(
        url: str, params: dict[str, Any], is_test: bool = False
    ) -> tuple[Any, Any, Any] | None | int:
        """Make an HTTP GET request with retry logic.

        Args:
            url (str): The URL to make the request to.
            params (dict[str, Any]): Query parameters for the request.
            is_test (bool): If it is a test

        Returns:
            dict[str, Any] | None: The JSON response if successful, None otherwise.

        """
        logger = provenance.get_logger(__name__)

        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=["GET"],
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

    def _process_response(
        self, response_json: dict[str, Any]
    ) -> Generator[Any, None, None]:
        """Process the JSON response and yield the results.

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
        """Query solr index in a paginated manner.

        Yields:
            Generator[Any, None, None]: The docs from each page.

        """
        logger = provenance.get_logger(__name__)

        while True:
            result = self._make_request(self.end_point, self.query)

            if not result:
                break

            response_json, response_time, response_url = result
            self.prov_collect(response_url, response_time, response_json)

            docs = response_json.get("response", {}).get("docs", [])
            yield docs
            #yield from self._process_response(response_json)

            # Check if this is the last page
            if self.query["cursorMark"] == response_json.get("nextCursorMark"):
                logger.info("Reached the last page.")
                break


            # Get the next page in the review mode
            if self._review:
                if not self._review_list:
                    logger.info("No more pages to review.")
                    break
                self._review_page, self._review_cursor = self._review_list.pop()
                self.query["cursorMark"] = self._review_cursor
            else:
                self.query["cursorMark"] = response_json.get("nextCursorMark")

    def prov_collect(
        self,
        req_url: str,
        req_time: float,
        response: dict[Any, Any],
    ) -> None:
        """Collect prov and db."""
        self._numFound = response.get("response").get("numFound")

        DBsession = MigrationDB.get_session()
        with DBsession() as session:

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
                    date_range=(
                        "[* To *]" if not self.query.get("fq") else self.query.get("fq")
                    ),
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
                    pages=prepage.pages + 1 if prepage is not None else 1,
                    rows=self.query.get("rows"),
                    cursorMark=self.query.get("cursorMark"),
                    cursorMark_next=response.get("nextCursorMark"),
                    n_failed=0,
                    index=ind,
                    doc_size = len(json.dumps(response.get("response").get("docs"))),
                )

                session.add(query_obj)
                session.commit()
                curpage = session.query(Query).order_by(Query.id.desc()).first()
                self._current_query = curpage


class GlobusQuery(BaseQuery):
    """query globus index."""

    query: dict[Any, Any]
    generator: bool = False
    paginator: Literal["post", "scroll"]
    skip_prov: bool = False

    _current_query: Any | None = None
    _total_returned: int = 0
    _review: bool = False
    _restart: bool = False

    _n_batch: int = 0

    def get_offset(self, review:bool = False) -> None:
        """Find the offset of previous sync."""
        logger = provenance._instance.get_logger(__name__)

        if review:
            pass

        else:
            # determine the offset
            DBsession = MigrationDB.get_session()
            with DBsession() as session:

                last_query = session.query(Query).order_by(Query.id.desc()).first()


                if last_query:
                    query_dict= json.loads(last_query.query_str)

                if last_query is None or query_dict["filters"] != self.query["filters"]:  # new start
                    self._current_query = None

                    if self.paginator == "scroll":
                        self.query.pop("marker", None)
                        self.query.pop("premarker", None)

                    else:
                        self.query["offset"] = 0

                    logger.info("The query is new start")
                else:

                    self._current_query = last_query
                    ingest_obj = last_query.ingest

                    if len(ingest_obj) == 0:
                        self._restart = True

                        if self.paginator == "scroll":
                            self.query["marker"] = last_query.cursorMark
                        else:
                            self.query["offset"] = int(last_query.cursorMark)

                        logger.info("The query is restart with no corresponding ingest")
                    else:
                        filter_ingest = [
                            ing for ing in ingest_obj if ing.pages == last_query.pages
                        ]

                        #any failed one of the batch, will restart it and delete the record
                        #n_datasets is n_batch in the sync program
                        if len(filter_ingest) != last_query.n_datasets or last_query.n_datasets == 0:

                            # delete all the records in files and ingest, we do not use datasets anymore

                            # Delete matching Ingest records
                            deleted_count = session.query(Ingest)  \
                                .filter(Ingest.pages == last_query.pages)\
                                .delete(synchronize_session=False)

                            logger.info(f"Delete failed ingest records {deleted_count}")

                            deleted_count = session.query(Files)  \
                                .filter(Files.pages == last_query.pages)\
                                .delete(synchronize_session=False)

                            logger.info(f"Delete failed file records {deleted_count}")

                            self._restart = True

                            if self.paginator == "scroll":
                                self.query["marker"] = last_query.cursorMark
                            else:
                                self.query["offset"] = int(last_query.cursorMark)

                            logger.info("The query is restarted with cleaning recoreds")

                            session.commit()
                        else:

                            cleaned = False
                            for ing in filter_ingest:
                                 if ing.submitted == 0:
                                     cleaned = True
                                     break

                            if cleaned:
                            # Delete matching Ingest records
                                deleted_count = session.query(Ingest)  \
                                    .filter(Ingest.pages == last_query.pages)\
                                    .delete(synchronize_session=False)

                                logger.info(f"Delete failed ingest records {deleted_count}")

                                deleted_count = session.query(Files)  \
                                    .filter(Files.pages == last_query.pages)\
                                    .delete(synchronize_session=False)

                                logger.info(f"Delete failed file records {deleted_count}")

                                self._restart = True

                                if self.paginator == "scroll":
                                    self.query["marker"] = last_query.cursorMark
                                else:
                                    self.query["offset"] = int(last_query.cursorMark)

                                logger.info("The query is restarted with cleaning recoreds (no possible)")

                                session.commit()
                            else:
                                self.query["offset"] = int(last_query.cursorMark_next)
                                self._restart = False

                                if self.paginator == "scroll":
                                    self.query["marker"] = last_query.cursorMark_next
                                else:
                                    self.query["offset"] = int(last_query.cursorMark_next)

                                logger.info(
                                    "The query is restart in the next cursormark"
                                )


    def run(self) -> Generator[Any, None, None]:
        """Query the globus index in a pagination way."""
        logger = (
            provenance._instance.get_logger(__name__)
            if provenance._instance is not None else logging.getLogger(__name__)
        )

        client_name, index_name = GlobusClient.get_client_index_names(self.ep_name, self.project.value)

        gc = GlobusClient()
        cm = gc.get_client(self.ep_name)
        sc = cm.search_client
        sq = cm.search_query


        _globus_index_id = cm.indexes[index_name]
        if str(_globus_index_id) != str(self.end_point):
            raise ValueError("please give a right end point")

        for filter in self.query["filters"]:
            sq.add_filter(filter["field_name"], filter["values"], type=filter["type"])

        page_size = self.query["limit"]
        offset = self.query["offset"]

        total_returned = 0


        if self.paginator == "scroll":
            sq.set_query("*").set_limit(page_size)
            self.query["premarker"] = "*"
            if "marker" in self.query and self.query["marker"] is not None:
                sq["marker"] = self.query["marker"]
                self.query["premarker"] = self.query["marker"]

            start = time.time()
            for batch in sc.paginated.scroll(_globus_index_id, sq):
                elapsed_time = time.time() - start
                start = time.time()

                entries = batch.data
                total_returned += len(entries)
                self._total_returned = total_returned

                if self.skip_prov:
                    logger.info("skip the provenance and database update")
                else:
                    self.prov_collect(entries, elapsed_time, sq)
                yield entries

        if self.paginator == "post":

            max_retries = 3
            while True:
                retries = 0
                r = None
                while retries < max_retries:
                    try:
                        start = time.time()
                        sq.set_query("*").set_limit(page_size).set_offset(offset)
                        sq.add_sort(self.query.get("sort_field"), order=self.query.get("sort"))

                        r = sc.post_search(_globus_index_id, sq)
                        elapsed_time = time.time() - start
                        break

                    except GlobusAPIError as e:
                        if e.http_status == 429:  # Rate limited
                            retries += 1
                            time.sleep(2 ** retries)  # Exponential backoff
                            continue
                        logger.error(f"Error happened in globus query: {e}")
                        raise  # Re-raise other errors

                if not r:
                    break

                entries = r.data

                offset += page_size
                total_returned += len(entries)
                self._total_returned = total_returned
                if self.skip_prov:
                    logger.info("skip the provenance and database update")
                else:
                    self.prov_collect(entries, elapsed_time, sq)

                yield entries

                self.query["offset"] = offset
                if not r.data["has_next_page"]:
                    break

    def prov_collect(
        self,
        entries: dict[Any, Any],
        req_time: float,
        sq: SearchQuery
    ) -> None:
        """Collect provenance and update database from a globus query."""
        logger = provenance._instance.get_logger(__name__)
        self._numFound = entries.get("total")


        DBsession = MigrationDB.get_session()
        with DBsession() as session:

            if self._review:
                pass

            elif self._restart:
                prepage = session.query(Query).order_by(Query.id.desc()).first()
                prepage.n_failed = prepage.n_failed + 1
                prepage.query_time = req_time
                session.commit()
                self._restart = False

            else:

                my_index_name = self.project.value if self.ep_name == "stage" else self.ep_name
                ind = (
                    session.query(Index)
                    .filter(Index.index_name == my_index_name)
                    .first()
                )
                prepage = session.query(Query).order_by(Query.id.desc()).first()

                for f in self.query["filters"]:
                    if f["field_name"] == "_timestamp":
                        date_range = json.dumps(f["values"])

                query_obj = Query(
                    project=self.project,
                    project_type=(
                        "readonly"
                        if isinstance(self.project, ProjectReadOnly)
                        else "readwrite"
                    ),
                    query_str=json.dumps(sq.__dict__["data"]),
                    query_type="globus",
                    query_time=req_time,
                    date_range=date_range,
                    numFound=entries.get("total"),
                    n_datasets=0,             #store the n_batch in the sync mode
                    n_files=len(entries.get("gmeta")),
                    pages=prepage.pages + 1 if prepage is not None else 1,
                    rows=self.query.get("limit"),
                    cursorMark=self.query.get("premarker") if "marker" in entries else str(
                        self.query.get("offset")),
                    cursorMark_next=entries.get("marker") if "marker" in entries else str(
                        self.query.get("offset")+self.query.get("limit")),
                    n_failed=0,
                    index=ind,
                    doc_size = len(json.dumps(entries)),
                )

                session.add(query_obj)
                session.commit()
                curpage = session.query(Query).order_by(Query.id.desc()).first()
                self._current_query = curpage

                if "marker" in entries:
                    self.query["premarker"] = entries.get("marker")

            logger.info("Sucessfully update query table in the database")
