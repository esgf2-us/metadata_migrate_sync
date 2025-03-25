"""Check the status of ingest tasks
"""



import pathlib
from uuid import UUID

from rich import print

from metadata_migrate_sync.database import Query, Ingest, Datasets, Files, MigrationDB
from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite



def paginated_query(session, query, page_size=1000):
    for page in query.yield_per(page_size):
        yield page

def check_ingest_tasks(*,
    solr_index_name: str | None = None,
    globus_index_name: str,
    task_id: str | None = None,
    project: ProjectReadOnly | ProjectReadWrite | None = None,
    meta_type: str | None,
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

    cm = gc.get_client(name = globus_index_name)

    sc = cm.search_client

    if task_id is None:
        if solr_index_name is None or project is None or meta_type is None:
            print ("task ids can only from task_id or database")
            return None

        file_path = pathlib.Path(
            f"migration_{solr_index_name}_{globus_index_name}_{project.value}_{meta_type}.sqlite"
        )
        if file_path.exists():
            mdb = MigrationDB(str(file_path)+"?mode=ro", False)

            DBsession = MigrationDB.get_session()
            with DBsession() as session:

                if update:
                    #-query = session.query(Ingest.task_id).order_by(Ingest.id)
                    #-for item in paginated_query(session, query, 10):
                    #-    #for doc in item:
                    #-    print (item)
                    last_id = None
                    page_size = 20
                    while True:
                        ingest = session.query(Ingest).order_by(Ingest.id)
                        
                        if last_id is not None:
                            ingest = ingest.filter(Ingest.id > last_id)
                        
                        results = ingest.limit(page_size).all()
                        
                        if not results:
                            break
                        
                        for item in results:
                            if item.task_id != "skip":
                                r = sc.get_task(item.task_id)
                                print (item.task_id, r.data["state"])

                                query = session.query(Query).filter_by(pages = item.pages).first()

                                files = session.query(Files).filter_by(pages = item.pages).all()

                                print (query.id, item.pages, item.succeeded)

                                for file in files:
                                    print (file.pages, item.pages, 'files')

                                #item.succeeded = 1

                            last_id = item.id
                        #XXXXX

                        #session.commit()
                        break

                else:
                    task_ids = session.query(Ingest.task_id).offset(0).limit(10).all()

                    for k, task in enumerate(task_ids):  # task is single-item tuple

                        if task[0] != "skip":
                            r = sc.get_task(task[0])
                            print (r.data["task_id"], r.data["state"])

        else:

            print (f"{file_path} is not exist")
            print ("please provide the valid options")
    else:
        try:
            UUID(task_id)
            print (sc.get_task(task_id))
        except ValueError:
            print ("please provide validate task id")
        return None



if __name__ == "__main__":

    check_ingest_tasks(
        solr_index_name = "ornl",
        globus_index_name = "test",
        project = ProjectReadOnly.CMIP6,
        meta_type = "files",
        update = True,
    )



