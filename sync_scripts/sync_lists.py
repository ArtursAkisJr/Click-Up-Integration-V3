import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import subprocess
from datetime import datetime
import logging
from supabase_upload.lists_upload import upload_lists_to_supabase
from sync_scripts.incremental_helper import get_last_successful_sync_time
import psycopg2
import os
from dotenv import load_dotenv
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)
SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')

def update_sync_status(table_name, status, records_processed, error_message=None):
    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT
    )
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO clickup.sync_status (table_name, last_sync_time, sync_status, records_processed, error_message)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id, last_sync_time
    ''', ('lists', datetime.now(), status, records_processed, error_message))
    sync_status_id, sync_status_timestamp = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return sync_status_id, sync_status_timestamp

def run_and_load_json(script_path):
    result = subprocess.run(
        [sys.executable, script_path],
        check=True,
        capture_output=True,
        text=True
    )
    print(f'---RAW STDOUT START--- {script_path}')
    print(result.stdout)
    print(f'---RAW STDOUT END--- {script_path}')
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])

def main():
    try:
        folderless_lists = run_and_load_json(str(Path(__file__).parent.parent / 'clickup_api' / 'get_lists_folderless.py'))
        folder_lists = run_and_load_json(str(Path(__file__).parent.parent / 'clickup_api' / 'get_lists_folder.py'))
        all_lists = folderless_lists + folder_lists
        print(f"Fetched {len(all_lists)} lists (folderless + foldered).")

        last_sync = get_last_successful_sync_time('lists')
        if last_sync:
            # ClickUp returns updated_at as a Unix ms timestamp string
            last_sync_ms = int(last_sync.timestamp() * 1000)
            changed_lists = [
                lst for lst in all_lists
                if not lst.get('updated_at') or int(lst['updated_at']) > last_sync_ms
            ]
            logger.info(f"Incremental lists sync: {len(changed_lists)}/{len(all_lists)} lists changed since {last_sync}")
        else:
            changed_lists = all_lists
            logger.info(f"Full lists sync: {len(all_lists)} lists")

        # Insert sync status first
        sync_status_id, sync_status_timestamp = update_sync_status('lists', 'SUCCESS', len(changed_lists))
        # Upload to Supabase with sync_status_id and timestamp
        upload_lists_to_supabase(changed_lists, sync_status_id, sync_status_timestamp)
        logger.info('Lists sync completed successfully.')
    except Exception as e:
        update_sync_status('lists', 'ERROR', 0, str(e))
        logger.error(f'Error in sync_lists: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main() 