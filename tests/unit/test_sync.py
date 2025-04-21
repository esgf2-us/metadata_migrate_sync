from metadata_migrate_sync.project import ProjectReadWrite
from metadata_migrate_sync.app import _validate_project
from metadata_migrate_sync.sync import _setup_time_range_filter
from metadata_migrate_sync.util import get_utc_time_from_server
import logging
from datetime import datetime
from pathlib import Path
import os
import pytest

logging.root.handlers = []  # Clear existing handlers
logging.basicConfig(filename='test123.log', level=logging.INFO)

@pytest.mark.parametrize("input, expected", [
    ({"production": True, "data_dir": "./"}, 'Prod-noDB'),
    ({"production": True, "data_dir": None}, 'Prod-DB'),
    ({"production": False, "data_dir": "./"}, 'noProd-noDB'),
    ({"production": False, "data_dir": None}, 'noProd-DB'),
])
def test_setup_time_range_filter(datadir, input, expected):

    logger = logging.getLogger(__name__)

    time_str = datetime.now().strftime("%Y-%m-%d")

    source = Path(datadir / "synchronization_stage_backup_obs4MIPs_2025-04-16.sqlite")

    logger.info("Testing if log file is created")

    newfile = "synchronization_stage_backup_obs4MIPs_" + time_str + ".sqlite"
    target = Path(datadir / newfile)
    target.symlink_to(source)

    project = _validate_project("obs4MIPs")

    with pytest.raises(ValueError):
        time_range = _setup_time_range_filter(
            path_db_base = "synchronization_stage_backup_obs4MIPs",
            production = True,
            sync_freq = 5,
            start_time = None,
            logger = logger,
            data_dir = "./",
        )

    time_range = _setup_time_range_filter(
        path_db_base = "synchronization_stage_backup_obs4MIPs",
        production = input["production"],
        sync_freq = 5,
        start_time = datetime.fromisoformat("2025-03-16"),
        logger = logger,
        data_dir = datadir if input["data_dir"] is None else input["data_dir"],
    )


    match expected:
        case 'Prod-noDB':
            assert time_range["restart"] == None
            assert time_range["normal"]["values"][0]["from"] == "2025-03-16T00:00:00Z"
        case 'Prod-DB':
            assert time_range["restart"] == {
                'field_name': '_timestamp', 
                'type': 'range', 
                'values': [
                    {
                        'from': '2025-04-17T02:27:00.000Z', 
                        'to': '2025-04-17T02:32:00.000Z'
                    }
                ]
            }
            assert time_range["normal"]["values"][0]["from"] == "2025-04-17T02:32:00.000Z"
        case 'noProd-noDB':
            assert time_range["restart"] is None
            assert time_range["normal"]["values"][0]["from"] == get_utc_time_from_server(ahead_minutes=20)
        case 'noProd-DB':
            assert time_range["restart"] is None
            assert time_range["normal"]["values"][0]["from"] == get_utc_time_from_server(ahead_minutes=20)

    os.unlink(target)
