import sqlite3
import os
import json

def query_files_table_context(
    db_file_path: str, 
    meta: str,
) -> list[str]:
    """
    Query using context manager for automatic resource cleanup
    """
    if not os.path.exists(db_file_path):
        print(f"Error: Database file '{db_file_path}' does not exist.")
        return []

    try:
        db_uri = f"file:{db_file_path}?mode=ro"
        with sqlite3.connect(db_uri, uri=True) as connection:
            cursor = connection.cursor()

            if meta == "Dataset":
                query = """
                SELECT f.datasets_id, q.date_range
                FROM files f
                JOIN query q ON f.pages = q.id
                WHERE f.success = -9
                """
                #cursor.execute("SELECT datasets_id FROM datasets WHERE success = -9")
            else:
                query = """
                SELECT f.files_id, q.date_range
                FROM files f
                JOIN query q ON f.pages = q.id
                WHERE f.success = -9
                """
                #cursor.execute("SELECT files_id FROM files WHERE success = -9")

            # Execute the query
            cursor.execute(query)
            # Fetch all results
            results = cursor.fetchall()
        
            return results
            # Get all files_id values in one line
            #-files_ids = [row[0] for row in cursor.fetchall()]

            #-print(f"Found {len(files_ids)} records where success = -9")
            #-return files_ids

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

