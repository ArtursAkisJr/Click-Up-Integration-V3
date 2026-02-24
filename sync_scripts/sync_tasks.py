import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import subprocess
from datetime import datetime, timedelta
import logging
from supabase_upload.tasks_upload import upload_tasks_to_supabase
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
    ''', ('tasks', datetime.now(), status, records_processed, error_message))
    sync_status_id, sync_status_timestamp = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return sync_status_id, sync_status_timestamp

def main():
    try:
        cmd = [sys.executable, str(Path(__file__).parent.parent / 'clickup_api' / 'get_tasks.py')]

        last_sync = get_last_successful_sync_time('tasks')
        if last_sync:
            # 5-minute overlap buffer to avoid missing tasks updated near the boundary
            cutoff = last_sync - timedelta(minutes=5)
            date_updated_gt = int(cutoff.timestamp() * 1000)
            cmd += ['--date_updated_gt', str(date_updated_gt)]
            logger.info(f"Incremental task sync: fetching tasks updated after {cutoff} (Unix ms: {date_updated_gt})")
        else:
            logger.info("No previous successful sync found — performing full task sync")

        # Run get_tasks.py and capture its output
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        print('---RAW STDOUT START---')
        print(result.stdout)
        print('---RAW STDOUT END---')
        # Extract the last non-empty line for JSON parsing
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        tasks = json.loads(lines[-1])
        print(f"Fetched {len(tasks)} tasks.")
        # Insert sync status first
        sync_status_id, sync_status_timestamp = update_sync_status('tasks', 'SUCCESS', len(tasks))
        # Upload to Supabase with sync_status_id and timestamp
        upload_tasks_to_supabase(tasks, sync_status_id, sync_status_timestamp)
        logger.info('Tasks sync completed successfully.')
    except Exception as e:
        update_sync_status('tasks', 'ERROR', 0, str(e))
        logger.error(f'Error in sync_tasks: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main() 