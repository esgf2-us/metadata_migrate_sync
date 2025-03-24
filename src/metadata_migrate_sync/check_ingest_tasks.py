"""
Check the status of ingest tasks
"""



from uuid import UUID
from metadata_migrate_sync.project import ProjectReadOnly, ProjectReadWrite

from metadata_migrate_sync.globus import GlobusClient
from metadata_migrate_sync.database import MigrationDB, Ingest
from rich import print
import pathlib

def check_ingest_tasks(*, 
    solr_index_name: str | None = None,
    globus_index_name: str,
    task_id: str | None = None,
    project: ProjectReadOnly | ProjectReadWrite | None = None,
    meta_type: str | None,
) -> None:


    gc = GlobusClient()

    cm = gc.get_client(name = globus_index_name) 

    sc = cm.search_client

    if task_id is None:
        if solr_index_name is None or project is None or meta_type is None:
            print ("task ids can only from task_id or database")
            return None
        else:
            file_path = pathlib.Path(
                f"migration_{solr_index_name}_{globus_index_name}_{project.value}_{meta_type}.sqlite"
            )
            if file_path.exists():
                mdb = MigrationDB(str(file_path)+"?mode=ro", False)

                DBsession = MigrationDB.get_session()
                with DBsession() as session:
                    task_ids = session.query(Ingest.task_id).offset(0).limit(500).all()

                    for k, task in enumerate(task_ids):
                        print (task)
                        
                        if k == 0:
                            print (sc.get_task(task[0])) 

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
    )


    
