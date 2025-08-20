"""Data revision"""
from metadata_migrate_sync.database import MigrationDB, Query
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.ingest import GlobusIngest
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.query import GlobusQuery
from metadata_migrate_sync.transfer import paginate_json
from metadata_migrate_sync.convert import revise_gmeta
from metadata_migrate_sync.gmeta import ModifiedGmetaGenerator

from pydantic import validate_call
from tqdm import tqdm
from typing import Literal
from datetime import datetime
import sys
import pathlib


@validate_call
def metadata_revise(
    *,
    globus_ep: Literal["backup"],
    project: ProjectReadOnly | ProjectReadWrite,
    meta: str=Literal["File", "Dataset"],
    revise_json: str,
    page_start: int = 0,
    per_page: int = 2000, # XXXXX
) -> None:
    """metadata revision"""

    client_name, index_name = GlobusClient.get_client_index_names(globus_ep, project.value)
    _globus_index_id = GlobusClient.globus_clients[client_name].indexes[index_name]


    current_timestr = datetime.now().strftime("%Y-%m-%d")
    file_base = f"revision_{globus_ep}_{globus_ep}_{project.value}_{meta}_{current_timestr}"

    prov = provenance(
        task_name="revise",
        source_index_id=GlobusClient.globus_clients[client_name].indexes[index_name],
        source_index_type="globus",
        source_index_name=globus_ep,
        source_index_schema="ESGF1.5",
        ingest_index_id=GlobusClient.globus_clients[client_name].indexes[index_name],
        ingest_index_type="globus",
        ingest_index_name=globus_ep,
        ingest_index_schema="ESGF1.5",
        log_file=f"{file_base}.log",
        prov_file=f"{file_base}.json",
        db_file=f"{file_base}.sqlite",
        type_query=meta,
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


    query_dict = {
        "@version": "query#1.0.0",
        "q": "",
        "filters": [
            {
                "type": "match_any",
                "field_name": "id",
                "values": [],
            },
            {
                "type": "match_all",
                "field_name": "type",
                "values": [meta],
            },
        ],
        "limit": per_page,
        "offset": 0,
        "sort_field": "_timestamp",
        "sort": "asc",
    }

    # query
    gq = GlobusQuery(
        end_point=_globus_index_id,
        ep_type="globus",
        ep_name=globus_ep,
        project=project,
        query=query_dict,
        generator=False,
        paginator="post",
        skip_prov=False,
    )

    # ingest
    ig = GlobusIngest(
        end_point=_globus_index_id,
        ep_name=globus_ep,
        project=project,
    )

    print ('xxxxxin revise', _globus_index_id)

    logger.info("instantiate query and ingest classes")

    page = page_start

    with tqdm(
        desc="Processing pages",
        initial=page_start,
        unit="page",
        colour="blue",
        bar_format="{l_bar}{bar:50}{r_bar}",
        ncols=100,
        ascii=" ░▒▓█",
    ) as pbar:
        while True:
            page = page + 1
            try:
                result = paginate_json(
                    revise_json,
                    page=page,
                    per_page=per_page,
                    json_type="RootList",
                    )

                if len(result["items"]) == 0:
                    break
                ids = result["items"]
                query_dict["filters"][0]["values"] = ids


                for gpage_num, gpage in enumerate(gq.run()):
                    gq._n_batch = -1

                    gm =  ModifiedGmetaGenerator(
                        modifier = revise_gmeta,
                        revised_by = "Min Xu",
                        #-revised_items = {"retracted":False, "latest":True},
                        #-revised_value = [True, False],
                        #revised_items = {"latest": True},
                        #revised_value = [0.0001],
                        revised_items = {"size": 35749987588},
                        revised_value = [0.0001],
                    )

                    gm_list, gm_list_skip = gm.generate(gpage)

                    print (gm_list)

                    #XXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                    ig._submitted = False
                    ig.ingest(gm_list)


                    ig.prov_collect(
                        [g["content"] for g in gm_list["ingest_data"]["gmeta"]],
                        review=False,
                        current_query=gq._current_query,
                        metatype=meta,
                        batch_num=gq._n_batch,
                    )

                # XXXXX
                #-if page > 1:
                #-    print ('exiting')
                #-    sys.exit()

                # Update progress bar
                pbar.update(1)
                pbar.set_description(f"Processing page {page}")

            except Exception as e:
                print (f"No more page left {e}")
                break
