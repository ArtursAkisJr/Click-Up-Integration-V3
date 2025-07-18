import os
import json
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')


def upload_spaces_to_supabase(spaces, sync_status_id, sync_status_timestamp):
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
        cur.execute('DELETE FROM clickup.spaces')
        # Insert new records
        for space in spaces:
            cur.execute('''
                INSERT INTO clickup.spaces (id, name, private, status, color, avatar, archived, sync_status_id, sync_status_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                space.get('id'),
                space.get('name'),
                space.get('private'),
                space.get('status'),
                space.get('color'),
                space.get('avatar'),
                space.get('archived'),
                sync_status_id,
                sync_status_timestamp
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Uploaded {len(spaces)} spaces to Supabase.")
    except Exception as e:
        logger.error(f"Error uploading spaces to Supabase: {e}")
        raise 