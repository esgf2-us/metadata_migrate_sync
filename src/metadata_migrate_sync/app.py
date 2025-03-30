#!/usr/bin/env python
import datetime
import sys
from enum import Enum

import typer

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
    orderby: str = typer.Option(help="sort the result by"),
    limit: int = typer.Option(10, help="the limit of a page"),
    timerange: str = typer.Option(help="time range in search")
):



    if 'TO' not in timerange:
        print ("please provide a validate time range datetime-datetime")
        return


    start_time = timerange.split('TO')[0]

    t_start = datetime.datetime.fromisoformat(start_time)
    start_iso = t_start.isoformat() + "Z"  # "2023-01-01T00:00:00Z"


    end_time = timerange.split('TO')[1]
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

    if "." not in orderby:
        print ("please provide the correcit orderbu")
        sys.exit()

    order_field = orderby.split('.')[0]
    order = orderby.split('.')[1]

    kwargs = {}
    if ctx.args:
        for arg in ctx.args:
            typer.echo (arg)
            if "=" in arg and "--" in arg:
                key, value = arg.split("=", 1)
                kwargs[key] = value
            else:
                typer.echo(f"Ignoring invalid argument: {arg}")

    # Handle kwargs
    query = {"filters":[], "sort_field": order_field, "sort": order}
    proj_cond = {"type": "match_all", "field_name": "project", "values": [project.value]}
    query["filters"].append(proj_cond)

    query["filters"].append(time_cond)
    if kwargs:
        typer.echo("Query filters:")
        for key, value in kwargs.items():
            typer.echo(f"{key[2:]}: {value}")
            filter_cond = {"type": "match_all", "field_name": key[2:], "values": [value]}

            if key[2:] == "project":
                 query["filters"].remove(proj_cond)
                 query["filters"].append(filter_cond)

    query["limit"] = limit

    #-gq = GlobusQuery(
    #-    end_point="c0173b0c-5587-437a-a912-ef09b6d14e9c",
    #-    ep_type="globus",
    #-    ep_name="public_old",
    #-    project=ProjectReadOnly.CMIP6,
    #-    query=query,
    #-    generator=False,
    #-)
    #-# project is not used
    #-gq.run()

        #-else:
        #-sq.set_query("*").set_limit(page_size).set_offset(0)
        #-r = sc.post_search(_globus_index_id, sq)


        #-with open("data.json", "w") as f:
        #-    json.dump(r.data, f)

        #-for g in r.data.get("gmeta"):
        #-    print (g["entries"][0]["content"]["_timestamp"])

    pass


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
