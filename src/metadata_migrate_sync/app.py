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
from enum import Enum
from typing import Literal

import typer
from rich import print

from metadata_migrate_sync.check_ingest_tasks import check_ingest_tasks
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.migrate import metadata_migrate
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite
from metadata_migrate_sync.query import GlobusQuery
from metadata_migrate_sync.sync import metadata_sync
from metadata_migrate_sync.util import create_lock, release_lock

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


def _validate_meta(meta: str) -> Literal["files", "datasets"]:
    if meta not in ["files", "datasets"]:
        raise typer.BadParameter("meta must be 'files' or 'datasets'")
    return meta


def _validate_src_ep(ep: str) -> Literal["ornl", "anl", "llnl", "stage", "test_1", "test"]:
    if ep not in ["ornl", "anl", "llnl", "stage", "test_1", "test"]:
        raise typer.BadParameter(f"{ep} is not a supported ep")
    return ep


def _validate_tgt_ep(ep: str) -> Literal["test", "test_1", "public", "stage", "backup"]:
    if ep not in ["test", "test_1", "public", "stage", "backup"]:
        raise typer.BadParameter(f"{ep} is not a supported ep ")
    return ep


def _validate_project(project: str) -> ProjectReadOnly | ProjectReadWrite:
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
    source_ep: str = typer.Argument(help="source end point name"),
    target_ep: str = typer.Argument(help="target end point name"),
    project: str = typer.Argument(help="project name"),
    meta: str = typer.Option(help="metadata type"),
    prod: bool = typer.Option(help="production run", default=False),
) -> None:
    """Migrate documents in solr index to the globus index.

    Following the ESGF-1.5 migration plan and desingation
    """
    metadata_migrate(
        source_epname=_validate_src_ep(source_ep),
        target_epname=_validate_tgt_ep(target_ep),
        metatype=_validate_meta(meta),
        project=_validate_project(project),
        production=prod,
    )

def _validate_tgt_ep_all(ep: str) -> Literal["test", "test_1", "public", "stage", "all-prod", "backup"]:
    if ep not in ["test", "test_1", "public", "stage", "all-prod", "backup"]:
        raise typer.BadParameter(f"{ep} is not a supported ep ")
    return ep

@app.command()
def check_index(
    globus_ep: str = typer.Argument(help="globus end point name", callback=_validate_tgt_ep_all),
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
    source_ep: str = typer.Argument(help="source end point name"),
    target_ep: str = typer.Argument(help="target end point name"),
    project: ProjectReadWrite = typer.Argument(help="project name"),
    prod: bool = typer.Option(help="production run", default=False),
    start_time: datetime.datetime = typer.Option(help="start time", default=None),
    work_dir: pathlib.Path = typer.Option(help="writable directory to store database and outputs", default=pathlib.Path(".")),
    dry_run: bool = typer.Option(help="do everything the same except don't write to the target index", default=False),
) -> None:
    """Sync the ESGF-1.5 staged indexes to the public index.

    Details can be seen in the design.md
    """

    metadata_sync(
        source_epname=_validate_src_ep(source_ep),
        target_epname=_validate_tgt_ep(target_ep),
        project=_validate_project(project),
        production=prod,
        sync_freq=5,
        start_time=start_time,
        work_dir=work_dir,
        dry_run=dry_run,
    )


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
        if page_num >= 10:
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

                if k >= 10:
                   break

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

if __name__ == "__main__":
    app()
