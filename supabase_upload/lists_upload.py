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

def upload_lists_to_supabase(lists, sync_status_id, sync_status_timestamp):
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
        for lst in lists:
            records.append((
                lst.get('id'),
                lst.get('name'),
                lst.get('orderindex'),
                lst.get('content'),
                json.dumps(lst.get('status')) if lst.get('status') is not None else None,
                json.dumps(lst.get('priority')) if lst.get('priority') is not None else None,
                json.dumps(lst.get('assignee')) if lst.get('assignee') is not None else None,
                lst.get('task_count'),
                lst.get('due_date'),
                lst.get('start_date'),
                lst.get('folder_id'),
                lst.get('space_id'),
                lst.get('space_name'),
                lst.get('archived'),
                lst.get('override_statuses'),
                lst.get('permission_level'),
                json.dumps(lst.get('statuses')) if lst.get('statuses') is not None else None,
                lst.get('template_id'),
                lst.get('public'),
                lst.get('drop_down'),
                lst.get('created_at'),
                lst.get('updated_at'),
                lst.get('icon'),
                lst.get('list_type'),
                json.dumps(lst.get('custom_fields')) if lst.get('custom_fields') is not None else None,
                sync_status_id,
                sync_status_timestamp
            ))
        # Batch insert with UPSERT
        insert_sql = '''
            INSERT INTO clickup.lists (
                id, name, orderindex, content, status, priority, assignee, task_count, due_date, start_date, folder_id, space_id, space_name, archived, override_statuses, permission_level, statuses, template_id, public, drop_down, created_at, updated_at, icon, list_type, custom_fields, sync_status_id, sync_status_timestamp
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                orderindex = EXCLUDED.orderindex,
                content = EXCLUDED.content,
                status = EXCLUDED.status,
                priority = EXCLUDED.priority,
                assignee = EXCLUDED.assignee,
                task_count = EXCLUDED.task_count,
                due_date = EXCLUDED.due_date,
                start_date = EXCLUDED.start_date,
                folder_id = EXCLUDED.folder_id,
                space_id = EXCLUDED.space_id,
                space_name = EXCLUDED.space_name,
                archived = EXCLUDED.archived,
                override_statuses = EXCLUDED.override_statuses,
                permission_level = EXCLUDED.permission_level,
                statuses = EXCLUDED.statuses,
                template_id = EXCLUDED.template_id,
                public = EXCLUDED.public,
                drop_down = EXCLUDED.drop_down,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                icon = EXCLUDED.icon,
                list_type = EXCLUDED.list_type,
                custom_fields = EXCLUDED.custom_fields,
                sync_status_id = EXCLUDED.sync_status_id,
                sync_status_timestamp = EXCLUDED.sync_status_timestamp
        '''
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            execute_values(cur, insert_sql, batch)
        # Note: We don't delete orphaned lists here to avoid foreign key constraint issues
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Uploaded {len(lists)} lists to Supabase in batches of {BATCH_SIZE}.")
    except Exception as e:
        logger.error(f"Error uploading lists to Supabase: {e}")
        raise 