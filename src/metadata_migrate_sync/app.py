#!/usr/bin/env python
"""Main CLI interface to the ESGF-1.5 migration and synchronization tools.

the subcommands are:
  - migrate
  - sync
  - ...

"""



import datetime
import json
import pathlib
import sys
import time
from enum import Enum

import typer
#-from rich import print

from metadata_migrate_sync.check_ingest_tasks import check_ingest_tasks
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.migrate import metadata_migrate
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.query import GlobusQuery, SolrQuery
from metadata_migrate_sync.solr import SolrIndexes
from metadata_migrate_sync.sync import metadata_sync
from metadata_migrate_sync.transfer import globus_transfer, paginate_json
from metadata_migrate_sync.util import create_lock, release_lock
from metadata_migrate_sync.replica import metadata_replica

sys.setrecursionlimit(10000)

def _combine_enums(*enums: Enum, name:str="CombinedEnum") -> Enum:
    members = {}
    for enum in enums:
        for member in enum:
            # Ensure no duplicate names
            if member.name in members:
                raise ValueError(f"Duplicate member name: {member.name}")
            members[member.name] = member.value
    return Enum(name, members)


Project = _combine_enums(ProjectReadOnly, ProjectReadWrite)

app = typer.Typer()


def _validate_meta(meta: str) -> str:
    if meta not in ["files", "datasets"]:
        raise typer.BadParameter("meta must be 'files' or 'datasets'")
    return meta


def _validate_src_ep(ep: str) -> str:

    if ep not in ["ornl", "anl", "llnl", "stage", "test_1", "test"]:
        raise typer.BadParameter(f"{ep} is not a supported ep")
    return ep


def _validate_tgt_ep(ep: str) -> str:
    if ep not in ["test", "test_1", "public", "stage", "backup"]:
        raise typer.BadParameter(f"{ep} is not a supported ep ")
    return ep


def _validate_project(project: str) -> str:
    if project is not None:
        for p in ProjectReadOnly:
            if p.value == project:
                return p

        for p in ProjectReadWrite:
            if p.value == project:
                return p
        raise typer.BadParameter(f"project: {project} not supported")


@app.command()
def migrate(
    source_ep: str = typer.Argument(
        help="source end point name", callback=_validate_src_ep
    ),
    target_ep: str = typer.Argument(
        help="target end point name", callback=_validate_tgt_ep
    ),
    project: str = typer.Argument(help="project name", callback=_validate_project),
    meta: str = typer.Option(help="metadata type", callback=_validate_meta),
    prod: bool = typer.Option(help="production run", default=False),
    final: bool = typer.Option(help="final migration", default=False),
) -> None:
    """Migrate documents in solr index to the globus index.

    Following the ESGF-1.5 migration plan and desingation
    """
    metadata_migrate(
        source_epname=source_ep,
        target_epname=target_ep,
        metatype=meta,
        project=project,
        production=prod,
        final=final,
    )

def _validate_tgt_ep_all(ep: str) -> str:
    if ep not in ["test", "test_1", "public", "stage", "all-prod", "backup"]:
        raise typer.BadParameter(f"{ep} is not a supported ep ")
    return ep

@app.command()
def check_index(
    globus_ep: str = typer.Argument(
        help="globus end point name", callback=_validate_tgt_ep_all),
    project: str = typer.Option(None, help="project name", callback=_validate_project),
    save: bool = typer.Option(False, help="save to index.json"),
) -> None:
    """Check the globus index status."""
    gc = GlobusClient()
    cm = gc.get_client(name = globus_ep)

    sc = cm.search_client

    if project is None:

        tab_index = []
        for index_name in cm.indexes:
            index_id = cm.indexes.get(index_name)
            r = sc.get_index(index_id)
            print (r.data)
            tab_index.append(r.data)

    else:
        if project in ProjectReadOnly:
            index_id = cm.indexes.get("public")

        if project in ProjectReadWrite:
            index_id = cm.indexes.get(project.value)

        if index_id:
            print (sc.get_index(index_id).data)
            tab_index = sc.get_index(index_id).data
        else:
            print (f"Cannot find index for {project} in the {globus_ep} group, find it in public group")

    if save:
        pathlib.Path("index.json").write_text(json.dumps(tab_index))

@app.command()
def sync(
    source_ep: str = typer.Argument(
        help="source end point name", callback=_validate_src_ep
    ),
    target_ep: str = typer.Argument(
        help="target end point name", callback=_validate_tgt_ep
    ),
    project: str = typer.Argument(help="project name", callback=_validate_project),
    prod: bool = typer.Option(help="production run", default=False),
    start_time: datetime.datetime = typer.Option(help="start time", default=None),
) -> None:
    """Sync the ESGF-1.5 staged indexes to the public index.

    Details can be seen in the design.md
    """
    lock_file_path = f"/tmp/metadata_migrate_sync_{project.value}.lock"  # noqa S108

    try:
        lock_fd = create_lock(lock_file_path)

        metadata_sync(
            source_epname=source_ep,
            target_epname=target_ep,
            project=project,
            production=prod,
            sync_freq=5,
            start_time=start_time,
        )
    finally:
        release_lock(lock_fd, lock_file_path)


@app.command()
def create_index() -> None:
    """Create index for the test app."""
    gc = GlobusClient()
    cm = gc.get_client(name = "test")
    sc = cm.search_client

    r = sc.create_index("minxu test index 2", "for testing purpose")

    print (r)

@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def query_globus(
    ctx: typer.Context,
    globus_ep: str = typer.Argument(
        help="globus end point name", callback=_validate_tgt_ep),
    project: str = typer.Argument(help="project name", callback=_validate_project),
    order_by: str = typer.Option(help="sort the result by field_name.asc or field_name.desc"),
    limit: int = typer.Option(10, help="the limit of a page"),
    offset: int = typer.Option(0, help="the offset of a page (less than 10000)"),
    time_range: str = typer.Option(help="time range in search"),
    save: str = typer.Option(None, help="save the page to the json file"),
    printvar: str = typer.Option(None, help="print the content"),
    paginator: str = typer.Option("post", help="globus query type (post and scroll"),
    marker: str = typer.Option("None", help="marker for scroll search"),
    filter_proj: bool = typer.Option(True, help="filter using project name"),
    complete: bool=typer.Option(False, help="without 10 page limitation"),
    total: bool=typer.Option(False, help="just print the total info"),
) -> None:
    """Search globus index with normal and scroll paginations."""
    if "." not in order_by:
        print ("please provide the correct order-by")
        raise typer.Abort()

    order_field = order_by.split('.')[0]
    order = order_by.split('.')[1]
    query = {"filters":[], "sort_field": order_field, "sort": order}

    query["limit"] = limit
    query["offset"] = offset

    if 'TO' not in time_range:
        print ("please provide a validate time range datetime-datetime")
        raise typer.Abort()

    start_time = time_range.split('TO')[0]
    if start_time == '':
        start_iso = "*"
    else:
        t_start = datetime.datetime.fromisoformat(start_time)
        start_iso = t_start.isoformat() + "Z"  # "2023-01-01T00:00:00Z"

    end_time = time_range.split('TO')[1]
    if end_time == '':
        end_iso = "*"
    else:
        t_end = datetime.datetime.fromisoformat(end_time)
        end_iso = t_end.isoformat() + "Z"     # "2023-12-31T00:00:00Z"

    time_cond = {
        "type": "range",
        "field_name": "_timestamp",
        "values": [{
        "from": start_iso,  # Greater than or equal to start_date
        "to": end_iso     # Less than or equal to end_date
         }]
    }
    query["filters"].append(time_cond)

    if project is not None and filter_proj:
        proj_cond = {"type": "match_all", "field_name": "project", "values": [project.value]}
        query["filters"].append(proj_cond)

    kwargs = []
    if ctx.args:
        for arg in ctx.args:
            if "=" in arg and "--" in arg:
                key, value = arg.split("=", 1)
                kwargs.append((key, value))
            else:
                typer.echo(f"Ignoring invalid argument: {arg}")


    # Handle kwargs
    if kwargs:
        for key, value in kwargs:
            if key[2:] == "project":
                 query["filters"].remove(proj_cond)
            if "::" in value:
                value_1 = value.split("::")[0]
                value_2 = value.split("::")[1]

                match value_2:
                    case "like":
                        filter_cond = {"type": value_2, "field_name": key[2:], "value": "*" + value_1 + "*"}
                    case "not":
                        filter_cond = {
                            "type": value_2,
                            "filter":{
                                "type": "match_all",
                                "field_name": key[2:],
                                "values": [value_1],
                            },
                        }
                    case _:
                        filter_cond = {"type": value_2, "field_name": key[2:], "values": [value_1]}
            else:
                filter_cond = {"type": "match_all", "field_name": key[2:], "values": [value]}
            query["filters"].append(filter_cond)

    client_name, index_name = GlobusClient.get_client_index_names(globus_ep, project.value)
    _globus_index_id = GlobusClient.globus_clients[client_name].indexes[index_name]

    if marker != "None" and paginator == "scroll":
        query["marker"] = marker
    else:
        query.pop("marker", None)

    gq = GlobusQuery(
        end_point=_globus_index_id,
        ep_type="globus",
        ep_name=globus_ep,
        project=project,
        query=query,
        generator=True,
        paginator=paginator,
        skip_prov=True,
    )
    for page_num, page in enumerate(gq.run()):
        if total:
            print(page.get("total"))
            return


        if page_num >= 10 and not complete:
            break

        if save is not None:
            with open(save, "w") as f:
                json.dump(page, f)

        if printvar is not None:
            for k, g in enumerate(page.get("gmeta")):

                print_dict = {
                    "total": page.get("total"),
                    "subject": g["subject"],
                }

                for var in printvar.split(','):
                    if var in g["entries"][0]["content"]:
                        print_dict.update({
                            var: g["entries"][0]["content"][var],
                        })
                    elif var in page and var != "gmeta":
                        print_dict.update({
                            var: page[var],
                        })


                print (json.dumps(print_dict))

                #-if k >= 10:
                #-   break
@app.command()
def check_task(
    task_id: str = typer.Option(None, help="the ingest task id"),
    db_file: str = typer.Option(None, help="the ingest task id"),
    update: bool = typer.Option(False, help="update the succeeded flag in the database"),
) -> None:
    """Check the globus task ids."""
    check_ingest_tasks(
        task_id = task_id,
        db_file = db_file,
        update = update,
    )


@app.command()
def delete_subjects(
    globus_ep: str = typer.Argument(
        help="globus end point name", callback=_validate_tgt_ep),
    project: str = typer.Argument(help="project name", callback=_validate_project),
    json_file: str = typer.Argument(help="the json file stores the query results"),
) -> None:
    """Delete the subjects in a globus index."""

    client_name, index_name = GlobusClient.get_client_index_names(globus_ep, project.value)
    _globus_index_id = GlobusClient.globus_clients[client_name].indexes[index_name]

    gc = GlobusClient()
    cm = gc.get_client(globus_ep)
    sc = cm.search_client

    index_data = sc.get_index(_globus_index_id).data



    message = typer.style(
        f"client id: {cm.app_client_id} \n",
        fg=typer.colors.GREEN, bold=True
    ) + typer.style(
        f"token name: {cm.token_name} \n",
        fg=typer.colors.GREEN, bold=True
    ) + typer.style(
        f"globus index id: {_globus_index_id}: {index_data['description']} \n",
        fg=typer.colors.RED, bold=True
    ) + typer.style(
        f"want to delete records in the json file: {json_file} !!!\n",
        fg=typer.colors.RED, bold=True
    )

    typer.echo(message)

    with open(json_file) as f:
        # Load the JSON data from the file
        del_data = json.load(f)

    del_data_length =  len(del_data["gmeta"])

    to_be_deleted = []
    for g in del_data["gmeta"]:
        if g["entries"][0]["content"]["project"] != [project.value]:
            project_in_doc = g["entries"][0]["content"]["project"][0]
            print (f"the project of the document {project_in_doc}, "
                f"but the project {project.value} is provided")
            raise typer.Abort()
        else:
            to_be_deleted.append(g['subject'])

    if len(to_be_deleted) != del_data_length:
        raise ValueError("the length of to be deleted docs are not the same as that from the json file")
    else:
        message = typer.style(
            f"you are going to delete {del_data_length} records \n",
            fg=typer.colors.RED, bold=True
        ) + typer.style(
            "\n\n\n Yes or No?",
            fg=typer.colors.BLUE, bold=True
        )
        confirm = typer.prompt(message)
        if confirm == 'Yes':
            print (confirm)
            response = sc.batch_delete_by_subject(
                _globus_index_id,
                subjects=to_be_deleted,
            )
        else:
            print ("Do nothing and quit\n")
            raise typer.Abort()





@app.callback()
def main(ctx: typer.Context) -> None:
    """Add the tip for more filter functions."""
    if ctx.invoked_subcommand == "query-globus" and (
        "--help" in sys.argv or "-h" in sys.argv):
        print ("\n[bold red]Attention:[/bold red] more globus filters can " +
            "be applied by [green]--keyword=value::filter_option[/green]")


@app.command()
def compare_globus(
    globus_ep: str = typer.Argument(
        help="source end point name", callback=_validate_tgt_ep
    ),
    project: str = typer.Argument(help="project name", callback=_validate_project),
    field_name: str = typer.Argument(help="field_name"),
    field_value: str = typer.Argument(help="field_value"),
    data_node_1: str = typer.Argument(help="data_node_1"),
    data_node_2: str = typer.Argument(help="data_node_2"),
    meta: str = typer.Option("File", help="metadata type"),
) -> None:
    """Compare documents in a globus index."""

    from itertools import zip_longest
    
    client_name, index_name = GlobusClient.get_client_index_names(globus_ep, project.value)
    _globus_index_id = GlobusClient.globus_clients[client_name].indexes[index_name]

    if data_node_1 == "ornl":
        data_node_list_1 = ["esgf-node.ornl.gov"]
    elif data_node_1 == "anl":
        data_node_list_1 = ["eagle.alcf.anl.gov"]
    elif data_node_1 == "llnl":
        data_node_list_1 = [
            "aims3.llnl.gov",
            "esgf-data1.llnl.gov",
            "esgf-data2.llnl.gov",
        ]
    else:
        raise ValueError("wrong data_mode_1")


    if data_node_2 == "ornl":
        data_node_list_2 = ["esgf-node.ornl.gov"]
    elif data_node_2 == "anl":
        data_node_list_2 = ["eagle.alcf.anl.gov"]
    elif data_node_2 == "llnl":
        data_node_list_2 = [
            "aims3.llnl.gov",
            "esgf-data1.llnl.gov",
            "esgf-data2.llnl.gov",
        ]
    else:
        raise ValueError("wrong data_mode_2")

    globus_query_1 = {
        "@version": "query#1.0.0",
        "q": "*",
        "filters": [
            {
                "type": "match_all",
                "field_name": "project",
                "values": [project.value]
            },
            {
                "type": "match_all",
                "field_name": "type",
                "values": [meta]
            },
            {
                "type": "match_all",
                "field_name": field_name,
                "values": [field_value]
            },
            {
                "type": "match_any",
                "field_name": "data_node",
                "values": data_node_list_1
            },
        ],
        "limit": 5000
    }

    globus_query_2 = {
        "@version": "query#1.0.0",
        "q": "*",
        "filters": [
            {
                "type": "match_all",
                "field_name": "project",
                "values": [project.value]
            },
            {
                "type": "match_all",
                "field_name": "type",
                "values": [meta]
            },
            {
                "type": "match_all",
                "field_name": field_name,
                "values": [field_value]
            },
            {
                "type": "match_any",
                "field_name": "data_node",
                "values": data_node_list_2
            },
        ],
        "limit": 5000
    }

    gq_1 = GlobusQuery(
        end_point=_globus_index_id,
        ep_type="globus",
        ep_name=globus_ep,
        project=project,
        query=globus_query_1,
        generator=True,
        paginator="scroll",
        skip_prov=True,
    )

    gq_2 = GlobusQuery(
        end_point=_globus_index_id,
        ep_type="globus",
        ep_name=globus_ep,
        project=project,
        query=globus_query_2,
        generator=True,
        paginator="scroll",
        skip_prov=True,
    )

    s_left = set()
    g_left = set()

    sbase = set()
    ggbase = set()

    missing_files_sleft=set()
    missing_files_gleft=set()

    for num, (page_s, page_g) in enumerate(zip_longest(gq_1.run(), gq_2.run())):

        if page_g is not None and page_s is not None:
            print ("num =", num, page_s["total"], page_g["total"])
        else:
            print ("num =", num)

        #-s_ids = {gmeta["subject"].split("|")[0]: gmeta["entries"][0]["url"]
        #-    for gmeta in page_s["gmeta"]} if page_s is not None else set()
        #-g_ids = {gmeta["subject"].split("|")[0]: gmeta["entries"][0]["url"]
        #-    for gmeta in page_g["gmeta"]} if page_g is not None else set()

        #-s_ids = {gmeta["subject"].split("|")[0] for gmeta in page_s["gmeta"]} if page_s is not None else set()
        #-g_ids = {gmeta["subject"].split("|")[0] for gmeta in page_g["gmeta"]} if page_g is not None else set()

        # Extract {base_id: full_subject} mappings
        if page_s is not None:
            s_entries = {
                gmeta["subject"].split("|")[0]: gmeta["subject"]
                for gmeta in page_s["gmeta"]
            }
        else:
            s_entries = {}

        if page_g is not None:
            g_entries = {
                gmeta["subject"].split("|")[0]: gmeta["subject"]
                for gmeta in page_g["gmeta"]
            }
            gbase={gmeta["subject"] for gmeta in page_g["gmeta"]}
        else:
            g_entries = {}

        #-print (len(s_entries.keys()), len(g_entries.keys()))
        ss_ids = s_entries.keys() | s_left
        gg_ids = g_entries.keys() | g_left
        #-ss_ids = s_ids | s_left
        #-gg_ids = g_ids | g_left
        s_left = ss_ids - gg_ids
        g_left = gg_ids - ss_ids

        sbase = set(s_entries.values()) | sbase
        ggbase = gbase| ggbase


        #for doc in page_s:
        #    print (doc["institution_id"])

        #for gmeta in page_g["gmeta"]:
        #    print (gmeta["entries"][0]["content"]["institution_id"])
        #break


        if missing_files_gleft: 
            to_remove = {item for item in missing_files_gleft 
                        if item.split("|")[0] not in g_left}
            missing_files_gleft -= to_remove  # Remove all at once
        
        if missing_files_sleft:
            to_remove = {item for item in missing_files_sleft 
                        if item.split("|")[0] not in s_left}
            missing_files_sleft -= to_remove

        missing_files_gleft.update(
            g_entries[base_id]
            for base_id in g_left
            if base_id in g_entries  # Ensure base_id is in current page
        )

        missing_files_sleft.update(
            s_entries[base_id]
            for base_id in s_left
            if base_id in s_entries  # Ensure base_id is in current page
        )

        print (len(s_left))
        print (len(g_left))

    print (len(g_left))
    print (len(s_left))

    print ("base")
    print (len(sbase))
    print (len(ggbase))

    with open("test_kiost_duplicated.json", "w") as fw:
         json.dump(list(ggbase), fw)

    with open(f'missing_{meta}_{project}_{field_value}_{data_node_1}.json', 'w') as f:
        json.dump(list(missing_files_gleft), f)
    with open(f'missing_{meta}_{project}_{field_value}_{data_node_2}.json', 'w') as f:
        json.dump(list(missing_files_sleft), f)


@app.command()
def compare_solr_globus(
    source_ep: str = typer.Argument(
        help="source end point name", callback=_validate_src_ep
    ),
    target_ep: str = typer.Argument(
        help="target end point name", callback=_validate_tgt_ep
    ),
    project: str = typer.Argument(help="project name", callback=_validate_project),
    institution_id: str = typer.Argument(help="institution_id"),
    data_node: str = typer.Argument(help="data_node"),
    meta: str = typer.Option(help="metadata type", callback=_validate_meta),
) -> None:
    """Compare documents bwtween a solr and globus index."""

    from itertools import zip_longest

    solr_index_id = SolrIndexes.indexes[source_ep].index_id
    solr_index_type = SolrIndexes.indexes[source_ep].index_type
    meta_type = meta

    client_name, index_name = GlobusClient.get_client_index_names(target_ep, project.value)
    _globus_index_id = GlobusClient.globus_clients[client_name].indexes[index_name]

    solr_search_dict = {
        "sort": "id asc",
        "rows": 1500,
        "cursorMark": "*",
        "wt": "json",
        "q": "project:" + project.value,
        "fq": ["institution_id:"+institution_id, "data_node:"+data_node, "_timestamp:[* TO 2025-03-16T00:00:00Z]"],
    }

    sq = SolrQuery(
        end_point=f"{solr_index_id}/{solr_index_type}/{meta_type}/select",
        ep_type=solr_index_type,
        ep_name=source_ep,
        project=project,
        query=solr_search_dict,
        skip_prov=True,
    )



    globus_query = {
        "q": "*",
        "filters": [
            {
                "type": "match_all",
                "field_name": "project",
                "values": [project.value]
            },
            {
                "type": "match_all",
                "field_name": "type",
                "values": [meta.capitalize()[:-1]]
            },
            {
                "type": "match_all",
                "field_name": "institution_id",
                "values": [institution_id]
            },
            {
                "type": "match_all",
                "field_name": "data_node",
                "values": [data_node]
            },
            {
                "type": "range",
                "field_name": "_timestamp",
                "values": [{"from": "*", "to": "2025-03-16T00:00:00Z"}]
            }
        ],
        "limit": 1500
    }

    gq = GlobusQuery(
        end_point=_globus_index_id,
        ep_type="globus",
        ep_name=target_ep,
        project=project,
        query=globus_query,
        generator=True,
        paginator="scroll",
        skip_prov=True,
    )

    s_left = set()
    g_left = set()


    for num, (page_s, page_g) in enumerate(zip_longest(sq.run(), gq.run())):

        if page_g is not None:
            print ("num =", num, page_g["total"], sq._numFound)
        else:
            print ("num =", num)

        s_ids = {doc["id"] for doc in page_s} if page_s is not None else set()
        g_ids = {gmeta["subject"] for gmeta in page_g["gmeta"]} if page_g is not None else set()

        ss_ids = s_ids | s_left
        gg_ids = g_ids | g_left
        s_left = ss_ids - gg_ids
        g_left = gg_ids - ss_ids


        #for doc in page_s:
        #    print (doc["institution_id"])

        #for gmeta in page_g["gmeta"]:
        #    print (gmeta["entries"][0]["content"]["institution_id"])
        #break


        print (len(s_left))
        print (len(g_left))

    print (len(g_left))
    with open(f'missing_{meta}_{project}_{institution_id}_{data_node}.json', 'w') as f:
        json.dump(list(g_left), f)  # Convert set to list first

@app.command()
def transfer(
    globus_ep_source: str = typer.Argument(help="source globus"),
    globus_ep_target: str = typer.Argument(help="target globus"),
    project: str = typer.Argument(help="project name used for file name"),
    json_file: str = typer.Argument(help="json file"),
    json_type: str = typer.Argument(help="json file type"),
    page_start: int = typer.Option(0, help="start page"),
    per_page: int = typer.Option(5000, help="items per page"),
) -> None:
    """Transfer files from one globus ep to another ep."""

    page = page_start

    while True:
        page = page + 1
        try:
            result = paginate_json(
                json_file,
                page=page,
                per_page=per_page,
                json_type=json_type,
                )

            if len(result["items"]) == 0:
                break

            #-file_paths = []
            #-for item in result["items"]:
            #-    file_paths.append(item["local_path"])
            file_paths = result["items"]

            ep_between = f"{globus_ep_source}-{globus_ep_target}"
            globus_transfer(
                globus_ep_source,
                globus_ep_target,
                file_paths,
                batch_n=page,
                transfer_label=f'prod-{ep_between}-{json_type}_{project}'
            )

            time.sleep(30)

        except Exception as e:
            print (f"No more page left {e}")
            break

@app.command()
def replica(
    globus_ep: str = typer.Argument(
        help="globus index", callback=_validate_tgt_ep
    ),
    project: str = typer.Argument(
        help="project name", callback=_validate_project
    ),

    replica_json: str =  typer.Argument(
        help="json file containing the document needed to replicate"
    ),
    meta: str = typer.Argument(
        help="meta type: File or Dataset"
    ),

    src_data_node: str = typer.Argument(
        help="source data node: llnl"
    ),
    dst_data_node: str = typer.Argument(
        help="target data node: ornl"
    ),

) -> None:
    """Replicate the metadata in the index by changing documents directly."""

    metadata_replica(
        globus_ep,
        project,
        replica_json,
        meta,
        src_data_node,
        dst_data_node,
    )

    pass

if __name__ == "__main__":
    app()
