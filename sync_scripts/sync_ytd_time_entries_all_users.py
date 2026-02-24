import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import subprocess
from datetime import datetime, date, timedelta, timezone
import logging
from supabase_upload.time_entries_upload import upload_time_entries_to_supabase
from supabase_upload.cleanup_duplicate_time_entries import cleanup_duplicate_time_entries
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
    ''', ('time_entries', datetime.now(), status, records_processed, error_message))
    sync_status_id, sync_status_timestamp = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return sync_status_id, sync_status_timestamp

def get_all_user_ids():
    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT
    )
    cur = conn.cursor()
    cur.execute('SELECT id FROM clickup.team_members')
    user_ids = [str(row[0]) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return user_ids

def transform_time_entry(entry):
    # Convert ms to seconds for start/end/duration, then to datetime/hours
    def ms_to_datetime(ms):
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc) if ms else None
    def ms_to_hours(ms):
        return float(ms) / 3600000 if ms else None
    entry['start_time'] = int(entry['start']) if entry.get('start') else None
    entry['end_time'] = int(entry['end']) if entry.get('end') else None
    entry['duration_hours'] = ms_to_hours(entry.get('duration'))
    entry['start_datetime'] = ms_to_datetime(entry.get('start')).isoformat() if entry.get('start') else None
    entry['end_datetime'] = ms_to_datetime(entry.get('end')).isoformat() if entry.get('end') else None
    return entry

def main():
    try:
        # First, clean up any existing duplicates
        logger.info("Cleaning up existing duplicate time entries...")
        cleanup_duplicate_time_entries()
        
        user_ids = get_all_user_ids()
        year = datetime.now().year
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        # Run get_time_entries.py and capture its output
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent.parent / 'clickup_api' / 'get_time_entries.py'),
             '--user_ids', *user_ids,
             '--start_date', start_date.strftime('%Y-%m-%d'),
             '--end_date', end_date.strftime('%Y-%m-%d')],
            check=True,
            capture_output=True,
            text=True
        )
        print('---RAW STDOUT START---')
        print(result.stdout)
        print('---RAW STDOUT END---')
        # Extract the last non-empty line for JSON parsing
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        time_entries = json.loads(lines[-1])
        # Transform time fields
        time_entries = [transform_time_entry(e) for e in time_entries]
        print(f"Fetched {len(time_entries)} time entries.")
        # Insert sync status first
        sync_status_id, sync_status_timestamp = update_sync_status('time_entries', 'SUCCESS', len(time_entries))
        # Upload to Supabase with sync_status_id and timestamp
        upload_time_entries_to_supabase(time_entries, sync_status_id, sync_status_timestamp)
        # Free memory
        del time_entries
        import gc
        gc.collect()
        logger.info('Time entries sync completed successfully.')
    except Exception as e:
        update_sync_status('time_entries', 'ERROR', 0, str(e))
        logger.error(f'Error in sync_ytd_time_entries_all_users: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main() 