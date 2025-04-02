#!/usr/bin/env python
import datetime
import sys
from enum import Enum

import typer
import json

from rich import print
from metadata_migrate_sync.check_ingest_tasks import check_ingest_tasks
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.migrate import metadata_migrate
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.sync import metadata_sync

sys.setrecursionlimit(10000)

def combine_enums(*enums, name="CombinedEnum") -> Enum:
    members = {}
    for enum in enums:
        for member in enum:
            # Ensure no duplicate names
            if member.name in members:
                raise ValueError(f"Duplicate member name: {member.name}")
            members[member.name] = member.value
    return Enum(name, members)


Project = combine_enums(ProjectReadOnly, ProjectReadWrite)

app = typer.Typer()


def validate_meta(meta: str) -> str:
    if meta not in ["files", "datasets"]:
        raise typer.BadParameter("meta must be 'files' or 'datasets'")
    return meta


def validate_src_ep(ep: str) -> str:

    if ep not in ["ornl", "anl", "llnl", "stage"]:
        raise typer.BadParameter(f"{ep} is not a supported ep")
    return ep


def validate_tgt_ep(ep: str) -> str:
    if ep not in ["test", "public", "stage"]:
        raise typer.BadParameter(f"{ep} is not a supported ep ")
    return ep


def validate_project(project: str) -> str:
    if project is not None:
        for p in ProjectReadOnly:
            if p.value == project:
                return p

        for p in ProjectReadWrite:
            if p.value == project:
                return p
        raise typer.BadParameter("project not supported")


@app.command()
def migrate(
    source_ep: str = typer.Argument(
        help="source end point name", callback=validate_src_ep
    ),
    target_ep: str = typer.Argument(
        help="target end point name", callback=validate_tgt_ep
    ),
    project: str = typer.Argument(help="project name", callback=validate_project),
    meta: str = typer.Option(help="metadata type", callback=validate_meta),
    prod: bool = typer.Option(help="production run", default=False),
) -> None:

    metadata_migrate(
        source_epname=source_ep,
        target_epname=target_ep,
        metatype=meta,
        project=project,
        production=prod,
    )

def validate_tgt_ep_all(ep: str) -> str:
    if ep not in ["test", "public", "stage", "all-prod"]:
        raise typer.BadParameter(f"{ep} is not a supported ep ")
    return ep

@app.command()
def check_index(
    globus_ep: str = typer.Argument(
        help="globus end point name", callback=validate_tgt_ep_all),
    project: str = typer.Option(None, help="project name", callback=validate_project),
    save: bool = typer.Option(False, help="save to index.json"),
) -> None:

    import json
    import pathlib

    gc = GlobusClient()
    cm = gc.get_client(name = globus_ep)

    sc = cm.search_client

    if project is None:

        tab_index = []
        for index_name in cm.indexes:
            index_id = cm.indexes.get(index_name)
            r = sc.get_index(index_id)
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
        help="source end point name", callback=validate_src_ep
    ),
    target_ep: str = typer.Argument(
        help="target end point name", callback=validate_tgt_ep
    ),
    project: str = typer.Argument(help="project name", callback=validate_project),
    prod: bool = typer.Option(help="production run", default=False),
) -> None:

    metadata_sync(
        source_epname=source_ep,
        target_epname=target_ep,
        project=project,
        production=prod,
    )


@app.command()
def create_index():
    gc = GlobusClient()
    cm = gc.get_client(name = "test")
    sc = cm.search_client

    r = sc.create_index("minxu test index 2", "for testing purpose")

    print (r)

@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def query_globus(
    ctx: typer.Context,
    globus_ep: str = typer.Argument(
        help="globus end point name", callback=validate_tgt_ep),
    project: str = typer.Argument(help="project name", callback=validate_project),
    order_by: str = typer.Option(help="sort the result by field_name.asc or field_name.desc"),
    limit: int = typer.Option(10, help="the limit of a page"),
    offset: int = typer.Option(0, help="the offset of a page (less than 10000)"),
    time_range: str = typer.Option(help="time range in search"),
    save: str = typer.Option(None, help="save the page to the json file"),
    printvar: str = typer.Option(None, help="print the content"),
):

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

    if "." not in order_by:
        print ("please provide the correcit orderbu")
        raise typer.Abort()

    order_field = order_by.split('.')[0]
    order = order_by.split('.')[1]

    query = {"filters":[], "sort_field": order_field, "sort": order}

    if project is not None:
        proj_cond = {"type": "match_all", "field_name": "project", "values": [project.value]}
        query["filters"].append(proj_cond)
    query["filters"].append(time_cond)

    kwargs = {}
    if ctx.args:
        for arg in ctx.args:
            if "=" in arg and "--" in arg:
                key, value = arg.split("=", 1)
                kwargs[key] = value
            else:
                typer.echo(f"Ignoring invalid argument: {arg}")


    # Handle kwargs
    if kwargs:
        for key, value in kwargs.items():
            if key[2:] == "project":
                 query["filters"].remove(proj_cond)
            if "::" in value:
                value_1 = value.split("::")[0]
                value_2 = value.split("::")[1]
                if value_2 == "like":
                    filter_cond = {"type": value_2, "field_name": key[2:], "value": value_1}
                else:
                    filter_cond = {"type": value_2, "field_name": key[2:], "values": [value_1]}

            else:
                filter_cond = {"type": "match_all", "field_name": key[2:], "values": [value]}
            query["filters"].append(filter_cond)

    query["limit"] = limit
    query["offset"] = offset


    if globus_ep == "test":
        client_name = "test"
        index_name = globus_ep
    elif globus_ep == "public" or globus_ep == "public_old":
        client_name = "public"
        index_name = globus_ep
    else:
        client_name = "stage"
        index_name = project.value


    gc = GlobusClient()
    cm = gc.get_client(client_name)
    sc = cm.search_client
    sq = cm.search_query

    _globus_index_id = cm.indexes[index_name]


    sq.set_query("*").set_limit(query["limit"]).set_offset(query["offset"])
    sq.add_sort(query.get("sort_field"), order=query.get("sort"))

    sq["filters"] = query["filters"] 

    r = sc.post_search(_globus_index_id, sq)

    if save is not None:
        with open(save, "w") as f:
            json.dump(r.data, f)

    if printvar is not None:
        for k, g in enumerate(r.data.get("gmeta")):

            print_dict = {
                "total:": r.data.get("total"), 
                "subject": g["subject"], 
            }

            for var in printvar.split(','):
                if var in g["entries"][0]["content"]:
                    print_dict.update({
                        var: g["entries"][0]["content"][var],
                    })

            print (print_dict)

            if k >= 10:
               break

@app.command()
def check_task(
    task_id: str = typer.Option(None, help="the ingest task id"),
    db_file: str = typer.Option(None, help="the ingest task id"),
    update: bool = typer.Option(False, help="update the succeeded flag in the database"),
):
    check_ingest_tasks(
        task_id = task_id,
        db_file = db_file,
        update = update,
    )


if __name__ == "__main__":
    app()
