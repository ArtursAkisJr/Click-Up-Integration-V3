import os
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

def upload_team_members_to_supabase(team_members, sync_status_id, sync_status_timestamp):
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
        cur.execute('DELETE FROM clickup.team_members')
        # Insert new records
        for member in team_members:
            cur.execute('''
                INSERT INTO clickup.team_members (id, username, email, color, profile_picture, initials, role, sync_status_id, sync_status_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                member.get('id'),
                member.get('username'),
                member.get('email'),
                member.get('color'),
                member.get('profilePicture'),
                member.get('initials'),
                member.get('role'),
                sync_status_id,
                sync_status_timestamp
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Uploaded {len(team_members)} team members to Supabase.")
    except Exception as e:
        logger.error(f"Error uploading team members to Supabase: {e}")
        raise 