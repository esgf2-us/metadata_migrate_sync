"""syncing the indexes from the staged ones to the public (one way)."""
import logging
import math
import pathlib
import sys
from datetime import datetime
from typing import Literal

from pydantic import validate_call
from tqdm import tqdm

from metadata_migrate_sync.convert import fix_dtype_gmeta
from metadata_migrate_sync.database import MigrationDB, Query
from metadata_migrate_sync.globus import GlobusClient, GlobusCV
from metadata_migrate_sync.gmeta import ModifiedGmetaGenerator
from metadata_migrate_sync.ingest import GlobusIngest
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.query import GlobusQuery
from metadata_migrate_sync.sync import _process_batches


class FixesConfig:
    """config class for fixes."""

    PROD_MAX_INGEST_SIZE = 10 * 1000 * 1000 - 1000  # 10MB with buffer
    TEST_MAX_INGEST_SIZE = 20000
    TEST_MAX_PAGES = 2

@validate_call
def metadata_fixes(
    *,
    globus_epname: Literal["public", "backup"],
    project: ProjectReadWrite | ProjectReadOnly,
    production: bool,
    start_time: datetime | None = None,
    dry_run: bool = True
) -> None:
    """Sync the metadata between two Globus Indexes."""

    globus_client, globus_index = GlobusClient.get_client_index_names(globus_epname, project.value)

    file_base = f"fixation_{globus_epname}_{globus_epname}_{project.value}"

    prov = provenance(
        task_name="fixes",
        source_index_id=GlobusClient.globus_clients[globus_client].indexes[globus_index],
        source_index_type="globus",
        source_index_name=globus_epname,
        source_index_schema="ESGF1.5",
        ingest_index_id=GlobusClient.globus_clients[globus_client].indexes[globus_index],
        ingest_index_type="globus",
        ingest_index_name=globus_epname,
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

    if production:
        search_dict["limit"] = 3000
        maxpage = None
    else:
        search_dict["limit"] = 2
        maxpage = 2

    gq = GlobusQuery(
        end_point=prov.source_index_id,
        ep_type="globus",
        ep_name=globus_epname,
        project=project,
        query=search_dict,
        generator=True,
        paginator="scroll",
    )

    # ingest
    ig = GlobusIngest(
        end_point=prov.ingest_index_id,
        ep_name=globus_epname,
        project=project,
    )

    logger.info("instantiate query and ingest classes")

    for step in ["normal"]:
        # set the initial cursormark
        gq.get_offset_marker(review=False)

        logger.info("find the offset at " + str(gq.query["offset"]))

        current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info("query-ingest start at " + current_timestr)

        skipped_gmeta_no = 0
        ingested_gmeta_no = 0
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
                    logger.info(f"Empty page {page_num}. stop fixes!")
                    break

                gm =  ModifiedGmetaGenerator(
                    fix_dtype_gmeta
                )

                gmeta_ingest, gmeta_ingest_skipped = gm.generate(page)

                skipped_gmeta_no = skipped_gmeta_no + len(
                           gmeta_ingest_skipped[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]
                       )
                ingested_gmeta_no = ingested_gmeta_no + len(
                           gmeta_ingest[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]
                       )

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
                    continue

                gmeta_list = gmeta_ingest[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]

                if production:
                    batches = _process_batches(gmeta_list, FixesConfig.PROD_MAX_INGEST_SIZE)
                else:
                    batches = _process_batches(gmeta_list, FixesConfig.TEST_MAX_INGEST_SIZE)

                for n_batch, batch in enumerate(batches, start=1):

                    gq._n_batch = n_batch
                    ig._submitted = False
                    logger.debug(f"Processing batch {gq._n_batch}")

                    if dry_run:
                        ig._submitted = True
                    else:
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
        #-DBsession = MigrationDB.get_session()
        #-with DBsession() as session, session.begin():
        #-    prepage = session.query(Query).order_by(Query.id.desc()).first()
        #-    if prepage is not None:
        #-        prepage.cursorMark_next = "end of this query"  # type: ignore[assignment]
        #-    else:
        #-        raise ValueError("cannot find the previous page in the query table")


    current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Fixes stop at {current_timestr}")
    logger.info(f"Processed total pages: {page_num}")
    logger.info(f"Total skipped: {skipped_gmeta_no}")
    logger.info(f"Total ingested: {ingested_gmeta_no}")

    # clean up
    logging.shutdown()
    prov.successful = True
    pathlib.Path(prov.prov_file).write_text(prov.model_dump_json(indent=2))
