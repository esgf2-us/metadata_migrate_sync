from metadata_migrate_sync.replica import metadata_replica
from metadata_migrate_sync.project import ProjectReadWrite
from metadata_migrate_sync.provenance import provenance
import glob
import sqlite3
from pathlib import Path
import pytest
import json
from datetime import datetime


@pytest.mark.parametrize("gmeta_name,expected", [
    ("gmeta_sample_right_type", 0),
    ("gmeta_sample_wrong_type", 1),
])
def test_replica(datadir, tmp_path, mock_globus_query, gmeta_name, expected, request):


    json_path = datadir / "replica_list.json" 

    mock_globus_query['instance'].reset_mock()
    gmeta_sample = request.getfixturevalue(gmeta_name)
    mock_globus_query['instance'].run.return_value = [gmeta_sample]

    # provenance is singleton calss
    provenance._instance = None

    metadata_replica(
        source_ep = "stage",
        target_ep = "backup",
        project = ProjectReadWrite.INPUT4MIPS,
        replica_json = str(json_path),
        meta = "Dataset",
        src_data_node = "nersc",
        dst_data_node = "ornl",
        dry_run = True,
        output_path = tmp_path,
    )


    # Assert - Verify GlobusQuery was called
    mock_globus_query['class'].assert_called_once()
    mock_globus_query['instance'].run.assert_called_once()

    db_file = glob.glob(str(tmp_path) + "/replication_stage_backup_input4MIPs_Dataset_*.sqlite") 

    assert len(db_file) > 0, "No database files found in temporary path"

    # Check if file exists
    assert Path(db_file[0]).exists(), f"Database file '{db_file}' not found"
    
    conn = None
    try:
        conn = sqlite3.connect(db_file[0])
        cursor = conn.cursor()
        
        # Query the success value
        cursor.execute(f"SELECT success FROM datasets")
        results = cursor.fetchall()
        
        assert len(results) == expected, "No records or more than 1 found in the 'datasets' table"
            
        if results:
            assert results[0][0] == -9
        
    except sqlite3.Error as e:
        pytest.fail(f"SQLite error when verifying database: {e}")
    finally:
        if conn:
            conn.close()

        
