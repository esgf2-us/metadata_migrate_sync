"""migrate the solr indexes in ORNL/LLNL/ANL to globus indexes (public and staged ones)."""

import logging
import math
import pathlib
import sys
from datetime import datetime
from typing import Literal

from pydantic import validate_call
from tqdm import tqdm

from metadata_migrate_sync.database import MigrationDB
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.ingest import GlobusIngest, generate_gmeta_list
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.query import SolrQuery, params_search
from metadata_migrate_sync.solr import SolrIndexes


@validate_call
def metadata_migrate(
    *,
    source_epname: Literal["llnl", "ornl", "anl"],
    target_epname: Literal["test", "public", "stage"],
    metatype: Literal["files", "datasets"],
    project: ProjectReadOnly | ProjectReadWrite,
    production: bool,
) -> None:
    """Migrate metadata/documents from solr indexes to the globus indexes."""
    # setup the provenance

    client_name, index_name = GlobusClient.get_client_index_names(target_epname, project.value)

    prov = provenance(
        task_name="migrate",
        source_index_id=SolrIndexes.indexes[source_epname].index_id,
        source_index_type=SolrIndexes.indexes[source_epname].index_type,
        source_index_name=source_epname,
        source_index_schema=SolrIndexes.indexes[source_epname].index_type,
        ingest_index_id=GlobusClient.globus_clients[client_name].indexes[index_name],
        ingest_index_type="globus",
        ingest_index_name=target_epname,
        ingest_index_schema="ESGF1.5",
        log_file=f"migration_{source_epname}_{target_epname}_{project.value}_{metatype}.log",
        prov_file=f"migration_{source_epname}_{target_epname}_{project.value}_{metatype}.json",
        db_file=f"migration_{source_epname}_{target_epname}_{project.value}_{metatype}.sqlite",
        type_query=metatype.capitalize(),
        cmd_line=" ".join(sys.argv),
    )

    pathlib.Path(prov.prov_file).write_text(prov.model_dump_json(indent=2))

    logger = (
        provenance._instance.get_logger(__name__)
        if provenance._instance is not None else logging.getLogger(__name__)
    )

    logger.info(f"set up the provenance and save it to {prov.prov_file}")
    logger.info(f"log file is at {prov.log_file}")

    # database
    _ = MigrationDB(prov.db_file, True)
    logger.info(f"initialed the sqllite database at {prov.db_file}")

    # query generator
    # for ReadWirte projects, need a cut-off date.
    if project in ProjectReadWrite:
        search_dict = {
            **params_search,
            "q": "project:" + project.value,
            "fq": "_timestamp:[* TO 2025-03-16T00:00:00Z]",
        }
    else:
        search_dict = {
            **params_search,
            "q": "project:" + project.value,
            "fq": "_timestamp:[* TO 2025-03-16T00:00:00Z]",
        }

    if production:
        search_dict["rows"] = 1500

        if project.value == "CMIP6":
            search_dict["rows"] = 1500

        if project.value == "e3sm" or project.value == "e3sm-supplement":
            search_dict["rows"] = 100

        if project.value == "GFDL-CMIP6":
            if source_epname != "llnl":
                print (f"for the project {project.value}, only llnl is allowed")
                sys.exit()
            search_dict["shards"] = f"localhost:8995/solr/{metatype}"
            search_dict["q"] = "project: CMIP6"

        if project.value == "GFDL-CMIP5":
            if source_epname != "llnl":
                print (f"for the project {project.value}, only llnl is allowed")
                sys.exit()
            search_dict["shards"] = f"localhost:8995/solr/{metatype}"
            search_dict["q"] = "project: CMIP5"

        if project.value == "GFDL-LLNL-CMIP6":
            if source_epname != "llnl":
                print (f"for the project {project.value}, only llnl is allowed")
                sys.exit()
            search_dict["fq"] = "data_node:esgdata.gfdl.noaa.gov"
            search_dict["q"] = "project: CMIP6"

        if project.value == "GFDL-LLNL-CMIP5":
            if source_epname != "llnl":
                print (f"for the project {project.value}, only llnl is allowed")
                sys.exit()
            search_dict["fq"] = "data_node:esgdata.gfdl.noaa.gov"
            search_dict["q"] = "project: CMIP5"

        maxpage = None
        if target_epname == "test":
            logger.warning("production run generaly does not ingest to test index")

    else:
        search_dict["rows"] = 2
        maxpage = 2
        if target_epname != "test":
            logger.warning("test run generaly does not ingest to production indexes")

    logger.info("finish the query setting")

    sq = SolrQuery(
        end_point=f"{prov.source_index_id}/{prov.source_index_type}/{metatype}/select",
        ep_type=prov.source_index_type,
        ep_name=source_epname,
        project=project,
        query=search_dict,
    )

    # ingest
    ig = GlobusIngest(
        end_point=prov.ingest_index_id,
        ep_name=target_epname,
        project=project,
    )

    logger.info("instantiate query and ingest classes")

    # set the initial cursormark
    sq.get_cursormark(review=False)
    logger.info("find the cursormark at " + sq.query["cursorMark"])

    current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("query-ingest start at " + current_timestr)

    n = 0
    with tqdm(
        sq.run(),
        desc="Processing",
        unit="page",
        colour="blue",
        bar_format="{l_bar}{bar:50}{r_bar}",
        ncols=100,
        ascii=" ░▒▓█",
    ) as pbar:

        for page in pbar:
            if not pbar.total and hasattr(sq, "_numFound") and sq._numFound:
                pbar.total = math.ceil(sq._numFound / search_dict["rows"])

            if len(page) == 0:
                logger.info(f"no data in this page {n}. stop the ingestion")
                break

            n = n + 1
            ig._submitted = False
            gmeta_ingest, new_page = generate_gmeta_list(page, metatype)

            if len(gmeta_ingest["ingest_data"]["gmeta"]) > 0:
                ig.ingest(gmeta_ingest)
            else:
                ig._response_data = {}
                ig._submitted = True

            ig.prov_collect(
                new_page,
                review=False,
                current_query=sq._current_query,
                metatype=metatype,
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
