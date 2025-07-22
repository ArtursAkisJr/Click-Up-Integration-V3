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

    # 1. Create tables WITHOUT foreign keys
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
        CREATE TABLE IF NOT EXISTS clickup.spaces (
            id BIGINT PRIMARY KEY,
            name TEXT,
            private BOOLEAN,
            status TEXT,
            color TEXT,
            avatar TEXT,
            archived BOOLEAN,
            sync_status_id INTEGER,
            sync_status_timestamp TIMESTAMP
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
            sync_status_id INTEGER,
            sync_status_timestamp TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.folders (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            orderindex INTEGER NOT NULL,
            override_statuses BOOLEAN NOT NULL,
            hidden BOOLEAN NOT NULL,
            space_id BIGINT NOT NULL,
            space_name TEXT,
            task_count INTEGER,
            lists TEXT[] NOT NULL,
            sync_status_id INTEGER,
            sync_status_timestamp TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.lists (
            id TEXT PRIMARY KEY,
            name TEXT,
            orderindex INTEGER,
            content TEXT,
            status JSONB,
            priority JSONB,
            assignee JSONB,
            task_count INTEGER,
            due_date TEXT,
            start_date TEXT,
            folder_id TEXT,
            space_id BIGINT,
            space_name TEXT,
            archived BOOLEAN,
            override_statuses BOOLEAN,
            permission_level TEXT,
            statuses JSONB,
            template_id TEXT,
            public BOOLEAN,
            drop_down TEXT,
            created_at TEXT,
            updated_at TEXT,
            icon TEXT,
            list_type TEXT,
            custom_fields JSONB,
            sync_status_id INTEGER,
            sync_status_timestamp TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.time_entries (
            id BIGINT PRIMARY KEY,
            user_id BIGINT,
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
            list_id TEXT,
            folder_id TEXT,
            space_id BIGINT,
            wid TEXT,
            tags JSONB,
            sync_status_id INTEGER,
            sync_status_timestamp TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.tasks (
            id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT,
            orderindex INTEGER,
            priority TEXT,
            assignees JSONB,
            creator TEXT,
            due_date TEXT,
            start_date TEXT,
            date_created TEXT,
            date_updated TEXT,
            date_closed TEXT,
            closed BOOLEAN,
            archived BOOLEAN,
            url TEXT,
            list_id TEXT,
            list_name TEXT,
            folder_id TEXT,
            space_id BIGINT,
            custom_id TEXT,
            parent TEXT,
            tags JSONB,
            checklists JSONB,
            custom_fields JSONB,
            team_id TEXT,
            points FLOAT,
            time_estimate BIGINT,
            time_spent BIGINT,
            duration_hours FLOAT,
            dependencies JSONB,
            linked_tasks JSONB,
            subtasks JSONB,
            attachments JSONB,
            comments JSONB,
            description TEXT,
            text_content TEXT,
            created_by TEXT,
            updated_by TEXT,
            recurring BOOLEAN,
            template_id TEXT,
            time_in_status JSONB,
            time_tracking JSONB,
            reminders JSONB,
            watchers JSONB,
            parent_ids JSONB,
            subtask_ids JSONB,
            linked_task_ids JSONB,
            permission_level TEXT,
            list_type TEXT,
            start_time BIGINT,
            due_time BIGINT,
            date_done TEXT,
            date_started TEXT,
            date_last_activity TEXT,
            date_template TEXT,
            date_reminder TEXT,
            date_recurring TEXT,
            date_closed_by TEXT,
            date_created_by TEXT,
            date_updated_by TEXT,
            date_archived TEXT,
            date_deleted TEXT,
            date_restored TEXT,
            date_trashed TEXT,
            date_moved TEXT,
            date_duplicated TEXT,
            date_merged TEXT,
            date_split TEXT,
            date_linked TEXT,
            date_unlinked TEXT,
            date_parent_changed TEXT,
            date_subtask_changed TEXT,
            date_priority_changed TEXT,
            date_status_changed TEXT,
            date_assignee_changed TEXT,
            date_due_changed TEXT,
            date_start_changed TEXT,
            date_time_estimate_changed TEXT,
            date_time_spent_changed TEXT,
            date_points_changed TEXT,
            date_tags_changed TEXT,
            date_custom_fields_changed TEXT,
            date_watchers_changed TEXT,
            date_comments_changed TEXT,
            date_checklists_changed TEXT,
            date_attachments_changed TEXT,
            date_dependencies_changed TEXT,
            date_linked_tasks_changed TEXT,
            date_subtasks_changed TEXT,
            date_recurring_changed TEXT,
            date_template_changed TEXT,
            date_permission_level_changed TEXT,
            date_list_type_changed TEXT,
            date_icon_changed TEXT,
            date_public_changed TEXT,
            date_drop_down_changed TEXT,
            sync_status_id INTEGER,
            sync_status_timestamp TIMESTAMP
        )
    ''')
    conn.commit()

    # 2. Add foreign keys with ALTER TABLE
    # spaces
    cur.execute('''
        ALTER TABLE clickup.spaces
        ADD CONSTRAINT fk_spaces_sync_status
        FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    # team_members
    cur.execute('''
        ALTER TABLE clickup.team_members
        ADD CONSTRAINT fk_team_members_sync_status
        FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    # folders
    cur.execute('''
        ALTER TABLE clickup.folders
        ADD CONSTRAINT fk_folders_space_id
        FOREIGN KEY (space_id) REFERENCES clickup.spaces(id),
        ADD CONSTRAINT fk_folders_sync_status
        FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    # lists
    cur.execute('''
        ALTER TABLE clickup.lists
        ADD CONSTRAINT fk_lists_folder_id
        FOREIGN KEY (folder_id) REFERENCES clickup.folders(id),
        ADD CONSTRAINT fk_lists_space_id
        FOREIGN KEY (space_id) REFERENCES clickup.spaces(id),
        ADD CONSTRAINT fk_lists_sync_status
        FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    # time_entries
    cur.execute('''
        ALTER TABLE clickup.time_entries
        ADD CONSTRAINT fk_time_entries_user_id
        FOREIGN KEY (user_id) REFERENCES clickup.team_members(id),
        ADD CONSTRAINT fk_time_entries_list_id
        FOREIGN KEY (list_id) REFERENCES clickup.lists(id),
        ADD CONSTRAINT fk_time_entries_folder_id
        FOREIGN KEY (folder_id) REFERENCES clickup.folders(id),
        ADD CONSTRAINT fk_time_entries_space_id
        FOREIGN KEY (space_id) REFERENCES clickup.spaces(id),
        ADD CONSTRAINT fk_time_entries_sync_status
        FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    # Add foreign keys for tasks
    cur.execute('''
        ALTER TABLE clickup.tasks
        ADD CONSTRAINT fk_tasks_list_id FOREIGN KEY (list_id) REFERENCES clickup.lists(id),
        ADD CONSTRAINT fk_tasks_folder_id FOREIGN KEY (folder_id) REFERENCES clickup.folders(id),
        ADD CONSTRAINT fk_tasks_space_id FOREIGN KEY (space_id) REFERENCES clickup.spaces(id),
        ADD CONSTRAINT fk_tasks_sync_status FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    create_tables() 