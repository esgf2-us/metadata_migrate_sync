import typer
from uuid import UUID
from enum import Enum
from typing import Literal, Annotated
from pydantic import AnyUrl, HttpUrl, AnyHttpUrl

from metadata_migrate_sync.migrate import metadata_migrate
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.query import GlobusQuery


def combine_enums(*enums, name="CombinedEnum"):
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


def validate_meta(meta: str):
    if meta not in ["files", "datasets"]:
        raise typer.BadParameter("meta must be 'files' or 'datasets'")
    return meta


def validate_src_ep(ep: str):

    if ep not in ["ornl", "anl", "llnl"]:
        raise typer.BadParameter(f"{ep} is not a supported ep")
    return ep


def validate_tgt_ep(ep: str):
    if ep not in ["test", "public", "stage"]:
        raise typer.BadParameter(f"{ep} is not a supported ep ")
    return ep


def validate_project(project: str):
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
):

    metadata_migrate(
        source_epname=source_ep,
        target_epname=target_ep,
        metatype=meta,
        project=project,
        production=prod,
    )


@app.command()
def check_ingest():
    pass


@app.command()
def sync():
    pass


@app.command()
def query_globus(
    globus_ep: str = typer.Argument(
        help="globus end point name", callback=validate_tgt_ep),
    project: str = typer.Argument(help="project name", callback=validate_project),
):


    gq = GlobusQuery(
        end_point="52eff156-6141-4fde-9efe-c08c92f3a706",
        ep_type="globus",
        ep_name="test",
        project=project,
    )

    gq.run()

    pass


if __name__ == "__main__":
    app()
