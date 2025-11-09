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

def upload_folders_to_supabase(folders, sync_status_id, sync_status_timestamp):
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        cur = conn.cursor()
        # Use UPSERT (ON CONFLICT) instead of DELETE + INSERT to avoid foreign key issues
        # Prepare data for batch insert
        records = []
        for folder in folders:
            records.append((
                folder.get('id'),
                folder.get('name'),
                folder.get('orderindex'),
                folder.get('override_statuses'),
                folder.get('hidden'),
                int(folder.get('space_id')) if folder.get('space_id') is not None else None,
                folder.get('space_name'),
                folder.get('task_count'),
                folder.get('lists'),
                sync_status_id,
                sync_status_timestamp
            ))
        # Batch insert with UPSERT
        insert_sql = '''
            INSERT INTO clickup.folders (
                id, name, orderindex, override_statuses, hidden, space_id, space_name, task_count, lists, sync_status_id, sync_status_timestamp
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                orderindex = EXCLUDED.orderindex,
                override_statuses = EXCLUDED.override_statuses,
                hidden = EXCLUDED.hidden,
                space_id = EXCLUDED.space_id,
                space_name = EXCLUDED.space_name,
                task_count = EXCLUDED.task_count,
                lists = EXCLUDED.lists,
                sync_status_id = EXCLUDED.sync_status_id,
                sync_status_timestamp = EXCLUDED.sync_status_timestamp
        '''
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            execute_values(cur, insert_sql, batch)
        # Note: We don't delete orphaned folders here to avoid foreign key constraint issues
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Uploaded {len(folders)} folders to Supabase in batches of {BATCH_SIZE}.")
    except Exception as e:
        logger.error(f"Error uploading folders to Supabase: {e}")
        raise 