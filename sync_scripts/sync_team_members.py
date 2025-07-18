import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
import subprocess
from datetime import datetime
import logging
from supabase_upload.team_members_upload import upload_team_members_to_supabase
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
    ''', (table_name, datetime.now(), status, records_processed, error_message))
    sync_status_id, sync_status_timestamp = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return sync_status_id, sync_status_timestamp

def main():
    try:
        # Run get_team_members.py and capture its output
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent.parent / 'clickup_api' / 'get_team_members.py')],
            check=True,
            capture_output=True,
            text=True
        )
        # Debug: print the raw stdout to troubleshoot JSON parsing
        print('---RAW STDOUT START---')
        print(result.stdout)
        print('---RAW STDOUT END---')
        # Extract the last non-empty line for JSON parsing
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        team_members = json.loads(lines[-1])
        # Insert sync status first
        sync_status_id, sync_status_timestamp = update_sync_status('team_members', 'SUCCESS', len(team_members))
        # Upload to Supabase with sync_status_id and timestamp
        upload_team_members_to_supabase(team_members, sync_status_id, sync_status_timestamp)
        logger.info('Team members sync completed successfully.')
    except Exception as e:
        update_sync_status('team_members', 'ERROR', 0, str(e))
        logger.error(f'Error in sync_team_members: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main() 