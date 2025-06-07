"""syncing the indexes from the staged ones to the public (one way)."""
import json
import logging
import math
import pathlib
import sys
from datetime import datetime, timedelta
from typing import Any, Literal

from pydantic import validate_call
from tqdm import tqdm

from metadata_migrate_sync.database import MigrationDB, Query
from metadata_migrate_sync.globus import GlobusClient, GlobusCV
from metadata_migrate_sync.ingest import GlobusIngest, generate_gmeta_list_globus
from metadata_migrate_sync.project import ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.query import GlobusQuery
from metadata_migrate_sync.util import get_last_value, get_utc_time_from_server


class SyncConfig:
    """config class for sync."""

    PROD_MAX_INGEST_SIZE = 10 * 1000 * 1000 - 1000  # 10MB with buffer
    TEST_MAX_INGEST_SIZE = 20000
    TEST_MAX_PAGES = 2



@validate_call
def _process_batches(
    gmeta_list: list[dict[str, Any]],
    max_size_bytes: int,
) -> list[list[dict[str, Any]]]:
    """Process a batch of GMeta entries with size limits."""
    batches = []
    current_batch: list[dict[str,Any]] = []
    current_size = 0

    for gmeta in gmeta_list:
        gmeta_size = len(json.dumps(gmeta))
        if current_batch and (current_size + gmeta_size) > max_size_bytes:
            batches.append(current_batch)
            current_batch = []
            current_size = 0
        current_batch.append(gmeta)
        current_size += gmeta_size

    if current_batch:
        batches.append(current_batch)

    return batches


def _get_time_range_filter(*, time_from: str, time_to: str) -> dict[str, Any]:

    return {
        "type": "range",
        "field_name": "_timestamp",
        "values": [{
            "from": time_from,
            "to": time_to,
        }],
    }

def _setup_time_range_filter(
    path_db_base: str,
    production: bool,
    sync_freq: int | None,
    start_time: datetime | None,
    logger: logging.Logger,
    data_dir: str = "./",
) -> dict[str, dict[str, Any] | None]:
    """Set up the time range filter for the query."""
    if not production or sync_freq is None:
        return {
            "restart": None,
            "normal": _get_time_range_filter(
                          time_from = get_utc_time_from_server(ahead_minutes=20),
                          time_to   = get_utc_time_from_server(ahead_minutes=15),
                      )
        }

    # Production mode with sync frequency

    time_range: dict[str, dict[str, Any] | None] = {"restart": None, "normal": {}}
    prod_start = None
    for day in [0, 1, 2]:
        time_str = (datetime.now() - timedelta(days=day)).strftime("%Y-%m-%d")
        path_db = f"{path_db_base}_{time_str}.sqlite"
        prev_db = pathlib.Path(data_dir) / path_db

        logger.info(f"Looking for previous database file {prev_db}")

        if pathlib.Path(prev_db).is_file():
            query_str = get_last_value('query_str', "query", db_path=prev_db)

            cursorMark_next = get_last_value('cursorMark_next', "query", db_path=prev_db)


            if query_str:
                query_json = json.loads(query_str)
                for fi in query_json["filters"]:
                    if (fi.get("type") == 'range' and fi.get("field_name") == "_timestamp"):
                        time_range["restart"] = fi
                        prod_start = fi["values"][0]['to']
                        break

            if cursorMark_next and cursorMark_next == "end of this query":
                time_range["restart"] = None
            if prod_start is not None:
                break

    if prod_start is None:
        time_range["restart"] = None
        if start_time is not None:
            prod_start = start_time.isoformat() + 'Z'
        else:
            raise ValueError(
                "Cannot find previous 3-day time filters, please provide it by --start-time"
            )

    prod_end = get_utc_time_from_server(ahead_minutes=15)

    time_range["normal"] =  _get_time_range_filter(
                                time_from = prod_start,
                                time_to   = prod_end,
    )
    return time_range


@validate_call
def metadata_sync(
    *,
    source_epname: Literal["stage", "test", "test_1"],
    target_epname: Literal["public", "test", "test_1", "backup"],
    project: ProjectReadWrite,
    production: bool,
    sync_freq: int | None = None,
    start_time: datetime | None = None,
) -> None:
    """Sync the metadata between two Globus Indexes."""
    target_client, target_index = GlobusClient.get_client_index_names(target_epname, target_epname)

    source_client, source_index = GlobusClient.get_client_index_names(source_epname, project.value)

    current_timestr = datetime.now().strftime("%Y-%m-%d")

    file_base = f"synchronization_{source_epname}_{target_epname}_{project.value}_{current_timestr}"

    prov = provenance(
        task_name="sync",
        source_index_id=GlobusClient.globus_clients[source_client].indexes[source_index],
        source_index_type="globus",
        source_index_name=source_epname,
        source_index_schema="ESGF1.5",
        ingest_index_id=GlobusClient.globus_clients[target_client].indexes[target_index],
        ingest_index_type="globus",
        ingest_index_name=target_epname,
        ingest_index_schema="ESGF1.5",
        log_file=f"{file_base}.log",
        prov_file=f"{file_base}.json",
        db_file=f"{file_base}.sqlite",
        type_query="mixed (datasets and files)",
        cmd_line=" ".join(sys.argv),
    )


    pathlib.Path(prov.prov_file).write_text(prov.model_dump_json(indent=2))

    logger = (
        provenance._instance.get_logger(__name__)
        if provenance._instance is not None else logging.getLogger()
    )

    logger.info(f"set up the provenance and save it to {prov.prov_file}")
    logger.info(f"log file is at {prov.log_file}")

    # database
    _ = MigrationDB(prov.db_file, True)
    logger.info(f"initialized the sqlite database at {prov.db_file}")

    # query generator
    # for e3sm, "values": ["CMIP6-E3SM-Ext"] if project.value == "e3sm" else [project.value]
    # but change to e3sm as it is needed by Metagrid and confirmed by Sasha
    search_dict = {
        "filters":[
            {
                "type": "match_all",
                "field_name": "project",
                "values": [project.value],
            },
        ],
        "sort_field": "id",
        "sort": "asc",
        "limit": 10,
        "offset": 0,
    }


    path_db_base = f"synchronization_{source_epname}_{target_epname}_{project.value}"   #_{time_str}.sqlite"
    time_range_filter = _setup_time_range_filter(
        path_db_base,
        production,
        sync_freq,
        start_time,
        logger,
    )


    if production:
        search_dict["limit"] = 1000
        maxpage = None
    else:
        search_dict["limit"] = 20
        maxpage = 2

    gq = GlobusQuery(
        end_point=prov.source_index_id,
        ep_type="globus",
        ep_name=source_epname,
        project=project,
        query=search_dict,
        generator=True,
        paginator="scroll",
    )

    # ingest
    ig = GlobusIngest(
        end_point=prov.ingest_index_id,
        ep_name=target_epname,
        project=project,
    )

    logger.info("instantiate query and ingest classes")


    for step in ["restart", "normal"]:

        if step == "restart" and (not production or time_range_filter[step] is None):
            continue

        search_dict["filters"].append(time_range_filter[step])

        # set the initial cursormark
        gq.get_offset_marker(review=False)
        logger.info("find the offset at " + str(gq.query["offset"]))

        current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("query-ingest start at " + current_timestr + f"step: {step} {time_range_filter[step]}")

        with tqdm(
            gq.run(),
            desc="Processing",
            unit="page",
            colour="blue",
            bar_format="{l_bar}{bar:50}{r_bar}",
            ncols=100,
            ascii=" ░▒▓█",
        ) as pbar:

            for page_num, page in enumerate(pbar):
                if not pbar.total and hasattr(gq, "_numFound") and gq._numFound:
                    pbar.total = math.ceil(gq._numFound / gq.query["limit"])

                if len(page) == 0:
                    logger.info(f"Empty page {page_num}. stop sync!")
                    break

                gmeta_ingest, gmeta_ingest_skipped = generate_gmeta_list_globus(page)

                gq._n_batch = 0
                # record the skipped entries
                if len(
                       gmeta_ingest_skipped[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]
                   ) > 0:

                    ig._response_data = {}
                    ig._submitted = True

                    ig.prov_collect(
                        [
                            g[GlobusCV.CONTENT.value]
                            for g in gmeta_ingest_skipped[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]
                        ],
                        review=False,
                        current_query=gq._current_query,
                        metatype="files",
                        batch_num=gq._n_batch,
                    )

                    skip_size = len(gmeta_ingest_skipped[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value])
                    logger.info(f"Skipped {skip_size}")

                if len(gmeta_ingest[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]) == 0:
                    break

                gmeta_list = gmeta_ingest[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]

                if production:
                    batches = _process_batches(gmeta_list, SyncConfig.PROD_MAX_INGEST_SIZE)
                else:
                    batches = _process_batches(gmeta_list, SyncConfig.TEST_MAX_INGEST_SIZE)

                for n_batch, batch in enumerate(batches, start=1):

                    gq._n_batch = n_batch
                    ig._submitted = False
                    logger.debug(f"Processing batch {gq._n_batch}")

                    ig.ingest(
                        {
                            GlobusCV.INGEST_TYPE.value: GlobusCV.GMETALIST.value,
                            GlobusCV.INGEST_DATA.value: {
                                GlobusCV.GMETA.value: batch,
                            }
                        }
                    )

                    ig.prov_collect(
                        [g[GlobusCV.CONTENT.value] for g in batch],
                        review=False,
                        current_query=gq._current_query,
                        metatype="files",
                        batch_num=gq._n_batch,
                    )

                # update the n_batch in the query table
                DBsession = MigrationDB.get_session()
                with DBsession() as session, session.begin():
                    prepage = session.query(Query).order_by(Query.id.desc()).first()
                    if prepage is not None:
                        prepage.n_datasets = gq._n_batch  # type: ignore[assignment]
                        gq._n_batch = 0
                    else:
                        raise ValueError("cannot find the previous page in the query table")

                logger.info(f"Batch {gq._n_batch} ingested successfully for the page{page_num}")

                if not production and (maxpage is not None) and page_num > maxpage:
                    break

        # set the marker of the end of this query/search
        DBsession = MigrationDB.get_session()
        with DBsession() as session, session.begin():
            prepage = session.query(Query).order_by(Query.id.desc()).first()
            if prepage is not None:
                prepage.cursorMark_next = "end of this query"  # type: ignore[assignment]
            else:
                raise ValueError("cannot find the previous page in the query table")


    current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Synchronization stop at {current_timestr}")
    logger.info(f"Processed total pages: {page_num}")

    # clean up
    logging.shutdown()
    prov.successful = True
    pathlib.Path(prov.prov_file).write_text(prov.model_dump_json(indent=2))


if __name__ == "__main__":

    metadata_sync(
        source_epname="stage",
        target_epname="test",
        project=ProjectReadWrite.INPUT4MIPS,
        production=False,
    )

