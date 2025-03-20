""" 
migrating the solr indexes in ORNL/LLNL/ANL to globus indexes (public and staged ones)
"""

from pydantic import validate_call
from typing import Literal
import pathlib
from tqdm import tqdm
import math

from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.solr import SolrIndexes
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.database import MigrationDB
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.query import SolrQuery, params_search
from metadata_migrate_sync.ingest import GlobusIngest, generate_gmeta_list


@validate_call
def metadata_migrate(
    *,
    source_epname: Literal["llnl", "ornl", "anl"],
    target_epname: Literal["test", "public", "stage"],
    metatype: Literal["files", "datasets"],
    project: ProjectReadOnly | ProjectReadWrite,
) -> None:

    # setup the provenance
    prov = provenance(
        task_name="migrate",
        source_index_id=SolrIndexes.indexes[source_epname].index_id,
        source_index_type=SolrIndexes.indexes[source_epname].index_type,
        source_index_schema=SolrIndexes.indexes[source_epname].index_type,
        # ingest_index_id = GlobusClient.globus_clients["prod-migration"].indexes[target_epname],
        ingest_index_id=GlobusClient.globus_clients["test"].indexes["test"],
        ingest_index_type="globus",
        ingest_index_schema="ESGF1.5",
        log_file=f"migration_{source_epname}_{target_epname}_{project}_{metatype}.log",
        prov_file=f"migration_{source_epname}_{target_epname}_{project}_{metatype}.json",
        db_file=f"migration_{source_epname}_{target_epname}_{project}_{metatype}.sqlite",
        type_query=metatype.capitalize(),
    )

    logger = provenance._instance.get_logger(__name__)

    # database
    mdb = MigrationDB(prov.db_file, True)

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

    # set the initial cursormark
    sq.get_cursormark(review=False)

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
                pbar.total = math.ceil(sq._numFound / 1000.0)

            n = n + 1
            ig._submitted = False
            gmeta_ingest = generate_gmeta_list(page, metatype)

            ig.ingest(gmeta_ingest)
            ig.prov_collect(gmeta_ingest, review=False, current_query=sq._current_query)

            # for test purpose
            if n > 5:
                break

    # clean up
    pathlib.Path(prov.prov_file).write_text(prov.model_dump_json(indent=2))


if __name__ == "__main__":
    metadata_migrate(
        source_epname="ornl",
        target_epname="test",
        metatype="files",
        project=ProjectReadWrite.CMIP6,
    )
