import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')

def create_tables():
    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT
    )
    cur = conn.cursor()
    # Create schema if not exists
    cur.execute('CREATE SCHEMA IF NOT EXISTS clickup')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.spaces (
            id BIGINT PRIMARY KEY,
            name TEXT,
            private BOOLEAN,
            status TEXT,
            color TEXT,
            avatar TEXT,
            archived BOOLEAN,
            sync_status_id INTEGER REFERENCES clickup.sync_status(id),
            sync_status_timestamp TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.sync_status (
            id SERIAL PRIMARY KEY,
            table_name TEXT,
            last_sync_time TIMESTAMP,
            sync_status TEXT,
            records_processed INT,
            error_message TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.team_members (
            id BIGINT PRIMARY KEY,
            username TEXT,
            email TEXT,
            color TEXT,
            profile_picture TEXT,
            initials TEXT,
            role TEXT,
            sync_status_id INTEGER REFERENCES clickup.sync_status(id),
            sync_status_timestamp TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.lists (
            id TEXT PRIMARY KEY,
            name TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.folders (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            orderindex INTEGER NOT NULL,
            override_statuses BOOLEAN NOT NULL,
            hidden BOOLEAN NOT NULL,
            space_id BIGINT NOT NULL REFERENCES clickup.spaces(id),
            space_name TEXT,
            task_count INTEGER,
            lists TEXT[] NOT NULL,
            sync_status_id INTEGER REFERENCES clickup.sync_status(id),
            sync_status_timestamp TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.time_entries (
            id BIGINT PRIMARY KEY,
            user_id BIGINT REFERENCES clickup.team_members(id),
            task_id TEXT,
            billable BOOLEAN,
            start_time BIGINT,
            end_time BIGINT,
            duration BIGINT,
            duration_hours FLOAT,
            start_datetime TIMESTAMP,
            end_datetime TIMESTAMP,
            description TEXT,
            source TEXT,
            at BIGINT,
            is_locked BOOLEAN,
            approval_id TEXT,
            task_url TEXT,
            task_name TEXT,
            task_status TEXT,
            task_status_type TEXT,
            task_status_color TEXT,
            task_status_orderindex INTEGER,
            task_custom_type INTEGER,
            list_id TEXT REFERENCES clickup.lists(id),
            folder_id TEXT REFERENCES clickup.folders(id),
            space_id BIGINT REFERENCES clickup.spaces(id),
            wid TEXT,
            tags JSONB,
            sync_status_id INTEGER REFERENCES clickup.sync_status(id),
            sync_status_timestamp TIMESTAMP
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()

def test_create_test_table():
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name TEXT
            )
        ''')
        conn.commit()
        cur.close()
        conn.close()
        print('Test table created successfully.')
    except Exception as e:
        print(f'Error creating test table: {e}')

if __name__ == '__main__':
    create_tables()
    # test_create_test_table()  # Optional: keep for future troubleshooting 