"""Utility tools."""

import contextlib
import datetime
import fcntl
import logging
import os
import sqlite3
import sys
from collections.abc import Generator
from io import TextIOWrapper
from pathlib import Path

import ntplib
import requests
from ntplib import NTPException

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def file_lock(lock_file_path: Path = Path("/var/run")) -> Generator[TextIOWrapper, None, None]:
    """Context manager for file based locking."""
    lock_file = lock_file_path / "esgf15mms.pid"
    acquired = False
    try:
        # Open the lock file. Use 'a+' mode to create the file if it doesn't exist.
        with open(lock_file, "a+") as file_handle:
            # Acquire an exclusive lock (LOCK_EX).
            # LOCK_NB makes the lock non-blocking. If the lock cannot be acquired
            # immediately, an OSError is raised. Remove LOCK_NB for a blocking lock.
            fcntl.flock(file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True

            # Write the current process ID to the lock file.
            file_handle.write(str(os.getpid()))
            logger.info(f"Lock acquired on {lock_file}")

            # Yield control to the 'with' block.
            yield file_handle

            # Release the lock.
            fcntl.flock(file_handle, fcntl.LOCK_UN)
            logger.info(f"Lock released on {lock_file}")

    except OSError as e:
        # If the lock is already held (due to LOCK_NB), an OSError is raised.
        # errno.EWOULDBLOCK is the specific error code for this.
        import errno

        if e.errno == errno.EWOULDBLOCK:
            logger.error("Could not acquire lock on {lock_file}", exc_info=True)
            sys.exit(1)
        else:
            # Re-raise the exception if it's not a lock error
            raise

    finally:
        if acquired:
            lock_file.unlink()


def get_utc_time_from_server(ahead_minutes: int = 3) -> str:
    """Get UTC time from a NTP server and other time APIs."""
    apis = [
        "http://worldtimeapi.org/api/timezone/Etc/UTC",  # HTTP (no SSL)
        "https://timeapi.io/api/Time/current/zone?timeZone=UTC",
        "http://worldclockapi.com/api/json/utc/now",  # HTTP
    ]

    try:
        client = ntplib.NTPClient()
        response = client.request("pool.ntp.org")
        cur_time = datetime.datetime.fromtimestamp(response.tx_time, datetime.timezone.utc)
    except NTPException or requests.RequestException:
        cur_time = datetime.datetime.now(datetime.timezone.utc)  # Local fallback
        for api in apis:
            try:
                response = requests.get(api, timeout=5)
                data = response.json()

                print(data)
                if "datetime" in data:
                    utc_time = data["datetime"]  # 2025-04-09T19:42:34.490293+00:00
                if "dateTime" in data:
                    utc_time = data["dateTime"]  # 2025-04-09T19:44:44.024434

                if "Z" in utc_time:
                    cur_time = datetime.datetime.fromisoformat(utc_time.replace("Z", "+00:00"))
                elif "+00:00" not in utc_time:
                    cur_time = datetime.datetime.fromisoformat(utc_time[:-1] + "+00:00")

                else:
                    cur_time = datetime.datetime.fromisoformat(utc_time)
                break
            except requests.RequestException as e:
                print(f"Error fetching UTC time: {e}")
                continue

    cur_time_minus3 = (cur_time - datetime.timedelta(minutes=ahead_minutes)).replace(second=0, microsecond=0)
    return cur_time_minus3.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def get_last_value(column_name: str, table_name: str, db_path: Path = Path("database.db")) -> str | None:
    """Get a column value in the last row of a table."""
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        cursor = conn.cursor()

        if not (table_name.replace("_", "").isalnum() and column_name.replace("_", "").isalnum()):
            raise ValueError(
                "Invalid table or column name - \
                only alphanumeric and underscore characters allowed"
            )

        query = """
            SELECT ?
            FROM ?
            ORDER BY id DESC
            LIMIT 1
        """

        safe_query = query.replace("?", f'"{column_name}"', 1).replace("?", f'"{table_name}"', 1)
        cursor.execute(safe_query)
        result = cursor.fetchone()
        return result[0] if result else None
