"""query for solr and globus both"""

import requests
from uuid import UUID
from pydantic import BaseModel
from typing import Literal, Any, Generator
import sys

from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.database import MigrationDB, Index, Query, Ingest
from metadata_migrate_sync.provenance import provenance


params_search = {
    "sort": "id asc",
    "rows": 2,
    "cursorMark": "*",
    "wt": "json",
}

logger = provenance.get_logger()

class BaseQuery(BaseModel):
    end_point: str | UUID
    ep_type: Literal["solr", "gloubs"]
    ep_name: str
    project: ProjectReadOnly | ProjectReadWrite


class SolrQuery(BaseQuery):
    """query solr index"""

    query: dict[str, Any]

    _restart: bool = False
    _review: bool = False
    _review_list: list[Any]

    _current_query: Any | None = None
   

    def get_cursormark(self, review: bool = False) -> None:
        """get the cursormark from the database file"""

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
                else:  # dirty start
                    # get ingest
                    self._current_query = last_query
                    ingest_obj = last_query.ingest

                    assert len(ingest_obj) == 0

                    # if ingest_obj is None:   # never be a none
                    if len(ingest_obj) == 0:
                        self.query["cursorMark"] = last_query.cursorMark
                        self._restart = True
                    else:
                        filter_ingest = [
                            ing for ing in ingest_obj if ing.pages == last_query.pages
                        ]

                        if len(filter_ingest) == 1:
                            if filter_ingest[0].submitted == 0:
                                self.query["cursorMark"] = last_query.cursorMark
                                self._restart = True
                            else:
                                self.query["cursorMark"] = last_query.cursorMark_next
                                self._restart = False
                        else:
                            self.query["cursorMark"] = last_query.cursorMark
                            self._restart = True

    def run(self) -> Generator[Any, None, None]:

        index_url = self.end_point

        response = requests.get(index_url, params=self.query)
        if response.status_code == requests.codes.ok:
            res_json = response.json()
            self.prov_collect(response.url, response.elapsed.total_seconds(), res_json)

            yield res_json.get("response").get("docs")

            if self.query["cursorMark"] == res_json.get("nextCursorMark"):  # last page
                pass

            else:
                # get next page
                if self._review:
                    if len(self._review_list) == 0: # last 
                        pass
                    else:
                        self._review_page, self._review_cursor = self._review_list.pop()
                        self.query["cursorMark"] = self._review_cursor
                else:
                    self.query["cursorMark"] = res_json.get("nextCursorMark")
                yield from self.run()

    def prov_collect(
        self, req_url: str, req_time: float, response: dict[Any, Any]
    ) -> None:
        """collect prov and db"""

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
                    date_range="[* To *]" if isinstance(self.project, ProjectReadOnly) else self.query["q"],
                    numFound=response.get("response").get("numFound"),
                    n_datasets=(
                        0
                        if "solr\/files" in self.end_point
                        else len(response.get("response").get("docs"))
                    ),
                    n_files=(
                        0
                        if "solr\/datasets" in self.end_point
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


class GlobusQuey(BaseQuery):
    """query globus index"""

    pass
