"""Check the status of ingest tasks."""

import pathlib
from uuid import UUID

from globus_sdk import GlobusAPIError
from rich import print
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from metadata_migrate_sync.database import Ingest, MigrationDB
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.project import ProjectReadWrite


def check_ingest_tasks(*,
    task_id: str | None = None,
    db_file: pathlib.Path | str | None = None,
    update: bool = False,
) -> None:
    """Check the status of ingest tasks from the globus_index_name.

    1. only globus_index_name is required
    2. if task_id is not provided, then it means bulk checking,
    all task_ids are from the database
    3. update is only for the bulk checking, to update the succeeded
    value in the ingest table and success value in the files/datasets
    table. If it is False, print the first 10 tasks and status
    """
    gc = GlobusClient()

    target_index_name = "test"  # the app_client_id is needed and checkingg task_id
                                 # need more privileges
    cm = gc.get_client(name = target_index_name)

    sc = cm.search_client

    if task_id is None:
        if db_file is None:
            print ("Please provide either task id or the migration database file")
            return None

        file_path = pathlib.Path(db_file) if isinstance(db_file, str) else db_file

        if file_path.exists():

            if update:
                _ = MigrationDB(str(file_path)+"?mode=ro", False)
            else:
                _ = MigrationDB(str(file_path), False)

            updated_freq = 500
            page_size = 20

            DBsession = MigrationDB.get_session()
            with DBsession() as session:

                if update:
                    last_id = None

                    progress_columns = [
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                        TextColumn("✅ Success: {task.fields[success]}"),
                        TextColumn("📊 Ingest: {task.fields[ingest]}"),
                        TextColumn("📊 Total: {task.fields[total_ingest]}"),
                        TimeElapsedColumn(),
                    ]
                    with Progress(*progress_columns) as progress:
                         total_tasks = session.query(Ingest).count()
                         success_tasks = session.query(Ingest).filter_by(succeeded=1).count()
                         ingest_tasks = session.query(Ingest).filter(Ingest.task_id != "skip").count()

                         task = progress.add_task(
                             description="[cyan]Processing tasks...",
                             total=total_tasks,
                             success=success_tasks,
                             ingest=ingest_tasks,
                             total_ingest=total_tasks,
                         )

                         while True:
                             ingest = session.query(Ingest).filter_by(succeeded=0).order_by(Ingest.id)

                             if last_id is not None:
                                 ingest = ingest.filter(Ingest.id > last_id)

                             results = ingest.limit(page_size).all()

                             if not results:
                                 break

                             for item in results:
                                 if item.task_id != "skip":
                                     try:
                                         r = sc.get_task(item.task_id)
                                         if r.data["state"] == "SUCCESS":
                                             item.succeeded = 1
                                             progress.update(task, advance=1,
                                                 success=progress.tasks[0].fields["success"] + 1)
                                         else:
                                             progress.update(task, advance=1)
                                     except GlobusAPIError as e:
                                         print(f"Error processing task {item.task_id}: {e}")
                                         progress.update(task, advance=1)
                                 else:
                                     progress.update(task, advance=1)

                                 last_id = item.id

                                 if progress.tasks[0].completed % updated_freq == 0:
                                     session.commit()
                         session.commit()

                else:
                    task_ids = session.query(Ingest.task_id)\
                        .order_by(Ingest.id.desc())\
                        .offset(0).limit(10).all()

                    for (task,) in task_ids:  # task is single-item tuple
                        if task != "skip":
                            try:
                                r = sc.get_task(task)
                                print(r.data["task_id"], r.data["state"])
                            except GlobusAPIError as e:
                                print(f"Error fetching task {task_id}: {e}")

        else:
            print (f"{file_path} is not exist")
            print ("please provide the data file path")
    else:

        if db_file is not None:
            print ("both task_id and db_file provided, will skip db_file")
        try:
            UUID(task_id)
            print (sc.get_task(task_id))
        except ValueError:
            print ("please provide validate task id")
    return None



if __name__ == "__main__":

    check_ingest_tasks(
        solr_index_name = "llnl",
        globus_index_name = "stage",
        project = ProjectReadWrite.E3SM,
        meta_type = "files",
        update = False,
        task_id = '142631c4-2b09-4b13-b343-cbc8d98e67c5',
    )
