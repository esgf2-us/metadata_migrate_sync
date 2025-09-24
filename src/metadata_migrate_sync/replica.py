"""Data replication"""
import pathlib
import sys
from datetime import datetime
from typing import Literal

from tqdm import tqdm

from metadata_migrate_sync.convert import replicate_gmeta
from metadata_migrate_sync.database import MigrationDB
from metadata_migrate_sync.globus import GlobusClient, GlobusCV
from metadata_migrate_sync.gmeta import ModifiedGmetaGenerator
from metadata_migrate_sync.ingest import GlobusIngest
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
from metadata_migrate_sync.query import GlobusQuery
from metadata_migrate_sync.transfer import paginate_json


def metadata_replica(*,
    source_ep: str,
    target_ep: str="public",
    project: ProjectReadOnly | ProjectReadWrite,
    replica_json: str,
    meta: str=Literal["File", "Dataset"],
    src_data_node: str=Literal["llnl", "nersc", "anl", "iap"],
    dst_data_node: str=Literal["ornl", "newiap"],
    page_start: int = 0,
    per_page: int = 2000,   #XXXXXX make sure it is less than 10000 the limit of post query
    has_globus: bool = True,
    is_replica: bool = True,
    dry_run: bool = False,
    output_path: str = './',
) -> None:
    """metadata replication."""


    if dry_run:
        per_page = 2


    src_client_name, src_index_name = GlobusClient.get_client_index_names(source_ep, project.value)
    _globus_index_id_src = GlobusClient.globus_clients[src_client_name].indexes[src_index_name]

    dst_client_name, dst_index_name = GlobusClient.get_client_index_names(target_ep, project.value)
    _globus_index_id_dst = GlobusClient.globus_clients[dst_client_name].indexes[dst_index_name]

    current_timestr = datetime.now().strftime("%Y-%m-%d")
    file_base = f"replication_{source_ep}_{target_ep}_{project.value}_{meta}_{current_timestr}"

    prov = provenance(
        task_name="replica",
        source_index_id=GlobusClient.globus_clients[src_client_name].indexes[src_index_name],
        source_index_type="globus",
        source_index_name=source_ep,
        source_index_schema="ESGF1.5",
        ingest_index_id=GlobusClient.globus_clients[dst_client_name].indexes[dst_index_name],
        ingest_index_type="globus",
        ingest_index_name=target_ep,
        ingest_index_schema="ESGF1.5",
        log_file=f"{output_path}/{file_base}.log",
        prov_file=f"{output_path}/{file_base}.json",
        db_file=f"{output_path}/{file_base}.sqlite",
        type_query="mixed (datasets and files)",
        cmd_line=" ".join(sys.argv),
    )

    pathlib.Path(prov.prov_file).write_text(prov.model_dump_json(indent=2))

    print ('xxxxxxx', prov.prov_file)

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
        end_point=_globus_index_id_src,
        ep_type="globus",
        ep_name=source_ep,
        project=project,
        query=query_dict,
        generator=False,
        paginator="post",
        skip_prov=False,
    )


    # ingest
    ig = GlobusIngest(
        end_point=_globus_index_id_dst,
        ep_name=target_ep,
        project=project,
    )

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
                    replica_json,
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

                    if dry_run:
                        print("gpage", gpage)
                    gm =  ModifiedGmetaGenerator(
                        modifier = replicate_gmeta,
                        metatype = meta,
                        source_data_node = src_data_node,
                        target_data_node = dst_data_node,
                        has_globus = has_globus,
                        is_replica = is_replica,
                    )

                    gm_list, gm_list_skip = gm.generate(gpage)

                    gq._n_batch = 0   # always be zero
                    # record the skipped entries
                    if len(
                        gm_list_skip[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]
                       ) > 0:

                        ig._response_data = {}
                        ig._submitted = True

                        ig.prov_collect(
                            [
                                g[GlobusCV.CONTENT.value]
                                for g in gm_list_skip[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]
                            ],
                            review=False,
                            current_query=gq._current_query,
                            metatype=meta,
                            batch_num=gq._n_batch,
                        )

                        skip_size = len(gm_list_skip[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value])
                        logger.info(f"Skipped {skip_size}")  


                    if len(gm_list[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]) == 0:
                        continue

                    #XXXXXXXXXXXXXXXXXXXXXXXXXXXXX

                    if dry_run:
                        print (gm_list)
                    else:
                        ig._submitted = False
                        ig.ingest(gm_list)

                        ig.prov_collect(
                            [
                                 g[GlobusCV.CONTENT.value] 
                                 for g in gm_list[GlobusCV.INGEST_DATA.value][GlobusCV.GMETA.value]
                            ],
                            review=False,
                            current_query=gq._current_query,
                            metatype=meta,
                            batch_num=gq._n_batch,
                        )

                # XXXXX
                if dry_run:
                    if page > 1:
                        sys.exit()

                # Update progress bar
                pbar.update(1)
                pbar.set_description(f"Processing page {page}")

            except Exception as e:
                print (f"No more page left {e}")
                break
