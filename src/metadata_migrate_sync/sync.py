"""syncing the indexes from the staged ones to the public (one way)."""
import json
import logging
import math
import pathlib
import sys
from datetime import datetime
from typing import Literal

from pydantic import validate_call
from tqdm import tqdm

from metadata_migrate_sync.database import MigrationDB, Query
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.ingest import GlobusIngest, generate_gmeta_list_globus
from metadata_migrate_sync.project import ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.query import GlobusQuery


@validate_call
def metadata_sync(
    *,
    source_epname: Literal["stage", "test"],
    target_epname: Literal["public", "test"],
    project: ProjectReadWrite,
    production: bool,
) -> None:
    """Sync the metadata between two Globus Indexes."""
    if target_epname == "public" and production:
        target_client = "prod-migration"
        target_index = target_epname
    else:
        target_client = "test"
        target_index = "test"


    if source_epname == "stage":
        source_client = "prod-sync"
        source_index = project.value
    else:
        source_client = "test"
        source_index = "test"


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

    logger = provenance._instance.get_logger(__name__)

    logger.info(f"set up the provenance and save it to {prov.prov_file}")
    logger.info(f"log file is at {prov.log_file}")

    # database
    mdb = MigrationDB(prov.db_file, True)
    logger.info(f"initialed the sqllite database at {prov.db_file}")

    # query generator

    search_dict = {
        "filters":[
            {
                "type": "match_all",
                "field_name": "project",
                "values": [project.value],
            },
            {
                "type": "range",
                "field_name": "_timestamp",
                "values": [{
                    "from": "*",
                    "to": datetime.fromisoformat("2025-03-16").isoformat() + "Z",
                }],
            },
        ],
        "sort_field": "id",
        "sort": "asc",
        "limit": 10,
        "offset": 0,
    }

    if production:
        search_dict["limit"] = 1000
        maxpage = None
    else:
        search_dict["limit"] = 2
        maxpage = 2

    gq = GlobusQuery(
        end_point=prov.source_index_id,
        ep_type="globus",
        ep_name=source_epname,
        project=project,
        query=search_dict,
        generate=True,
    )


    # ingest
    ig = GlobusIngest(
        end_point=prov.ingest_index_id,
        ep_name=target_epname,
        project=project,
    )

    logger.info("instantiate query and ingest classes")

    # set the initial cursormark
    gq.get_offset(review=False)
    logger.info("find the offset at " + str(gq.query["offset"]))

    current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("query-ingest start at " + current_timestr)

    n = 0
    with tqdm(
        gq.run(),
        desc="Processing",
        unit="page",
        colour="blue",
        bar_format="{l_bar}{bar:50}{r_bar}",
        ncols=100,
        ascii=" ░▒▓█",
    ) as pbar:

        for page in pbar:
            if not pbar.total and hasattr(gq, "_numFound") and gq._numFound:
                pbar.total = math.ceil(gq._numFound / gq.query["limit"])

            if len(page) == 0:
                logger.info(f"no data in this page {n}. stop the ingestion")
                break

            n = n + 1
            gmeta_ingest = generate_gmeta_list_globus(page)

            max_size_bytes = 10 * 1000 * 1000 - 1000

            if len(gmeta_ingest["ingest_data"]["gmeta"]) > 0:

                gq._n_batch = 0
                ig._submitted = False

                #batch ingestion
                gmeta_ingest_batch={}
                gmeta_ingest_batch["ingest_type"] = "GMetaList"
                gmeta_ingest_batch["ingest_data"] = {"gmeta":[]}
                base_size = len(json.dumps(gmeta_ingest_batch))
                current_batch_size = base_size
                new_page = []

                for gmeta in gmeta_ingest["ingest_data"]["gmeta"]:
                    gmeta_size = len(json.dumps(gmeta))
                    # Check if adding this item would exceed the size limit
                    if (
                           gmeta_ingest_batch["ingest_data"]["gmeta"] and
                           (current_batch_size + gmeta_size) > max_size_bytes
                       ):
                        # Process the current batch
                        gq._n_batch += 1
                        ig._submitted = False
                        logger.debug(f"Processing batch {gq._n_batch}")
                        ig.ingest(gmeta_ingest_batch)

                        ig.prov_collect(
                            new_page,
                            review=False,
                            current_query=gq._current_query,
                            metatype="files",
                        )
                        # Start a new batch
                        new_page = []
                        gmeta_ingest_batch["ingest_data"]["gmeta"] =  []
                        current_batch_size = base_size
                    # Add item to current batch
                    new_page.append(gmeta["content"])
                    gmeta_ingest_batch["ingest_data"]["gmeta"].append(gmeta)
                    current_batch_size += gmeta_size

                if len(gmeta_ingest_batch["ingest_data"]["gmeta"]) > 0:
                    gq._n_batch += 1
                    ig._submitted = False
                    logger.debug(f"Processing batch {gq._n_batch}")
                    ig.ingest(gmeta_ingest_batch)

                    ig.prov_collect(
                        new_page,
                        review=False,
                        current_query=gq._current_query,
                        metatype="files",
                    )
                logger.info(f"Batch {gq._n_batch} ingested successfully")
                # update the n_batch in the query table
                DBsession = MigrationDB.get_session()
                with DBsession() as session, session.begin():
                    prepage = session.query(Query).order_by(Query.id.desc()).first()
                    prepage.n_datasets = gq._n_batch
                    gq._n_batch = 0

            else:
                ig._response_data = None
                ig._submitted = True

                ig.prov_collect(
                    new_page,
                    review=False,
                    current_query=gq._current_query,
                    metatype="files",
                )

            if not production and (maxpage is not None) and n > maxpage:
                break

    current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("query-ingest stop at " + current_timestr)
    logger.info(f"Processing total pages {n}")
    # clean up
    logging.shutdown()
    prov.successful = True
    pathlib.Path(prov.prov_file).write_text(prov.model_dump_json(indent=2))


if __name__ == "__main__":

    metadata_sync(
        source_epname="stage",
        target_epname="test",
        project=ProjectReadWrite.CMIP6PLUS,
        production=True,
    )

