import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')

BATCH_SIZE = 500

def upload_time_entries_to_supabase(time_entries, sync_status_id, sync_status_timestamp):
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        cur = conn.cursor()
        # Delete all previous records
        cur.execute('DELETE FROM clickup.time_entries')
        # Prepare data for batch insert
        records = []
        for entry in time_entries:
            tags_json = json.dumps(entry.get('tags', [])) if entry.get('tags') is not None else None
            records.append((
                entry.get('id'),  # clickup_time_entry_id
                entry.get('user', {}).get('id'),
                entry.get('task', {}).get('id'),
                entry.get('billable'),
                entry.get('start_time'),
                entry.get('end_time'),
                entry.get('duration'),
                entry.get('duration_hours'),
                entry.get('start_datetime'),
                entry.get('end_datetime'),
                entry.get('description'),
                entry.get('source'),
                entry.get('at'),
                entry.get('is_locked'),
                entry.get('approval_id'),
                entry.get('task_url'),
                entry.get('task', {}).get('name'),
                entry.get('task', {}).get('status', {}).get('status'),
                entry.get('task', {}).get('status', {}).get('type'),
                entry.get('task', {}).get('status', {}).get('color'),
                entry.get('task', {}).get('status', {}).get('orderindex'),
                entry.get('task', {}).get('custom_type'),
                entry.get('task_location', {}).get('list_id'),
                entry.get('task_location', {}).get('folder_id'),
                entry.get('task_location', {}).get('space_id'),
                entry.get('wid'),
                tags_json,
                sync_status_id,
                sync_status_timestamp
            ))
        # Batch insert
        insert_sql = '''
            INSERT INTO clickup.time_entries (
                clickup_time_entry_id, user_id, task_id, billable, start_time, end_time, duration, duration_hours, start_datetime, end_datetime,
                description, source, at, is_locked, approval_id, task_url, task_name, task_status, task_status_type, task_status_color,
                task_status_orderindex, task_custom_type, list_id, folder_id, space_id, wid, tags, sync_status_id, sync_status_timestamp
            ) VALUES %s
        '''
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            execute_values(cur, insert_sql, batch)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Uploaded {len(time_entries)} time entries to Supabase in batches of {BATCH_SIZE}.")
    except Exception as e:
        logger.error(f"Error uploading time entries to Supabase: {e}")
        raise 