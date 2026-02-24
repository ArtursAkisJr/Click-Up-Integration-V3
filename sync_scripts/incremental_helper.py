import os
import logging
import psycopg2
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

logger = logging.getLogger(__name__)

SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')


def get_last_successful_sync_time(table_name: str) -> datetime | None:
    """
    Returns the timestamp of the last successful sync for the given table,
    or None if the table has never been synced successfully.
    """
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT last_sync_time
                FROM clickup.sync_status
                WHERE table_name = %s AND sync_status = 'SUCCESS'
                ORDER BY last_sync_time DESC
                LIMIT 1
                """,
                (table_name,)
            )
            row = cur.fetchone()
            cur.close()
            return row[0] if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Could not retrieve last sync time for '{table_name}': {e}")
        return None
