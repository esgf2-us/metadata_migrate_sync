"""Utility tools."""
import datetime
import fcntl
import os
import sqlite3
import sys
from pathlib import Path

import ntplib
from ntplib import NTPException
import requests


def create_lock(lockfile_path: str) -> int:
    """Create a lock file to prevent multiple instances."""
    lock_file = Path(lockfile_path)

    try:
        # Create or open the lock file
        fd = os.open(lockfile_path, os.O_WRONLY | os.O_CREAT)

        # Try to acquire an exclusive lock (non-blocking)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Write our PID to the file
        os.write(fd, str(os.getpid()).encode())

        return fd
    except (OSError, BlockingIOError):
        print(f"Another instance is already running (PID: {lock_file.read_text().strip()})")
        sys.exit(1)

def release_lock(fd: int, lockfile_path: str) -> None:
    """Release the lock file."""
    try:
        os.unlink(lockfile_path)
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    except OSError:
        pass


def get_utc_time_from_server(ahead_minutes: int = 3) -> str:
    """Get UTC time from a NTP server and other time APIs."""
    apis = [
        "http://worldtimeapi.org/api/timezone/Etc/UTC",  # HTTP (no SSL)
        "https://timeapi.io/api/Time/current/zone?timeZone=UTC",
        "http://worldclockapi.com/api/json/utc/now"      # HTTP
    ]

    try:
        client = ntplib.NTPClient()
        response = client.request("pool.ntp.org")
        cur_time =  datetime.datetime.fromtimestamp(response.tx_time, datetime.timezone.utc)
    except NTPException or requests.RequestException:
        cur_time =  datetime.datetime.now(datetime.timezone.utc)  # Local fallback
        for api in apis:
            try:
                response = requests.get(api, timeout=5)
                data = response.json()

                print (data)
                if 'datetime' in data:
                     utc_time = data["datetime"]  # 2025-04-09T19:42:34.490293+00:00
                if 'dateTime' in data:
                     utc_time = data["dateTime"]  # 2025-04-09T19:44:44.024434

                if 'Z' in utc_time:
                    cur_time = datetime.datetime.fromisoformat(utc_time.replace('Z', '+00:00'))
                elif '+00:00' not in utc_time:
                    cur_time = datetime.datetime.fromisoformat(utc_time[:-1] + '+00:00')

                else:
                    cur_time = datetime.datetime.fromisoformat(utc_time)
                break
            except requests.RequestException as e:
                print(f"Error fetching UTC time: {e}")
                continue

    cur_time_minus3 = (cur_time -
        datetime.timedelta(minutes=ahead_minutes)).replace(second=0, microsecond=0)
    return cur_time_minus3.isoformat(timespec='milliseconds').replace('+00:00', 'Z')



def get_last_value(column_name:str, table_name:str, db_path:str='database.db') -> str | None:
    """Get a column value in the last row of a table."""
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        cursor = conn.cursor()


        if not (table_name.replace('_', '').isalnum() and
            column_name.replace('_', '').isalnum()):
            raise ValueError("Invalid table or column name - \
                only alphanumeric and underscore characters allowed")

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
