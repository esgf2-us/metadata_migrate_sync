#!/usr/bin/env python
import typer
from typer import Context
from uuid import UUID
from enum import Enum
from typing import Literal, Annotated
from pydantic import AnyUrl, HttpUrl, AnyHttpUrl

from metadata_migrate_sync.migrate import metadata_migrate
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.query import GlobusQuery
from metadata_migrate_sync.globus import GlobusClient

from typing import Any

import sys

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

    if ep not in ["ornl", "anl", "llnl"]:
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

    import json, pathlib

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
def sync():
    pass

@app.command()
def create_index():
    gc = GlobusClient()
    cm = gc.get_client(name = "test")
    sc = cm.search_client

    r = sc.create_index("minxu test index 2", "for testing purpose")

    print (r)

@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": False})
def query_globus(
    ctx: typer.Context,
    globus_ep: str = typer.Argument(
        help="globus end point name", callback=validate_tgt_ep),
    project: str = typer.Argument(help="project name", callback=validate_project),
    more_args: list[str] | None = typer.Option(None, help="more options")
):


    kwargs = {}
    if more_args:
        for arg in more_args:
            typer.echo (arg)
            if "=" in arg:
                key, value = arg.split("=", 1)
                kwargs[key] = value
            else:
                typer.echo(f"Ignoring invalid argument: {arg}")

    # Handle kwargs
    if kwargs:
        typer.echo("Additional options:")
        for key, value in kwargs.items():
            typer.echo(f"  {key}: {value}")

    sys.exit()

    gq = GlobusQuery(
        end_point="52eff156-6141-4fde-9efe-c08c92f3a706",
        ep_type="globus",
        ep_name="test",
        project=ProjectReadOnly.CMIP6,
    )
    # project is not used 

    gq.run()

    pass


if __name__ == "__main__":
    app()
