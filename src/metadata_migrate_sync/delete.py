

from datetime import datetime
from typing import Literal, Any
from pydantic import validate_call
from tqdm import tqdm
import sys
import pathlib
import math

from metadata_migrate_sync.database import MigrationDB, Query
from metadata_migrate_sync.globus import GlobusClient, GlobusCV
from metadata_migrate_sync.ingest import GlobusIngest, generate_gmeta_list_globus
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.query import GlobusQuery



def _get_subjects(page: dict[str, Any]) -> list[str]:

    subjects_list=[]
    for gmeta in page["gmeta"]:
        subjects_list.append(gmeta["subject"])
    return subjects_list
    

@validate_call
def metadata_delete_llnl(
    *,
    globus_epname: Literal["stage", "test", "test_1", "public"],
    project: ProjectReadOnly | ProjectReadWrite,
    production: bool,
    dryrun: bool,
) -> None:

    globus_client, globus_index = GlobusClient.get_client_index_names(globus_epname, project.value)

    current_timestr = datetime.now().strftime("%Y-%m-%d")

    file_base = f"Deletion_{globus_epname}_{project.value}_{current_timestr}"

    prov = provenance(
        task_name="delete",
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

    _globus_index_id = GlobusClient.globus_clients[globus_client].indexes[globus_index]

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
            {
                "type": "match_any",
                "field_name": "data_node",
                "values": ["esgf-data1.llnl.gov", "esgf-data2.llnl.gov", "aims3.llnl.gov"],
            },
        ],
        "sort_field": "id",
        "sort": "asc",
        "limit": 10,
        "offset": 0,
    }

    if production:
        search_dict["limit"] = 2000
        maxpage = None
    else:
        search_dict["limit"] = 1
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

    sc = GlobusClient().get_client(globus_epname).search_client

    logger.info("instantiate query classes")
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
                 logger.info(f"Empty page {page_num}. stop deleting!")
                 break


             del_subjects_list=_get_subjects(page)

             if len(del_subjects_list) == 0:
                 continue

             ig._submitted = False

             if dryrun:
                 ig._submitted = True
             else:
                 response = sc.batch_delete_by_subject(
                     _globus_index_id,
                     subjects=del_subjects_list,
                 )
                 ig._response_data = response.data
                 ig._submitted = True

             ig.prov_collect(
                [
                    gmeta["entries"][0]["content"]
                    for gmeta in page["gmeta"]
                ],
                review=False,
                current_query=gq._current_query,
                metatype="files",
             )
             
             if not production and (maxpage is not None) and page_num >= maxpage:
                 break

    current_timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Deletion stop at {current_timestr}")
    logger.info(f"Processed total pages: {page_num}")
