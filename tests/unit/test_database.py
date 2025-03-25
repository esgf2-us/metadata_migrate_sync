from metadata_migrate_sync.database import MigrationDB


def test_databse():
    mdb = MigrationDB("test.db", True)
