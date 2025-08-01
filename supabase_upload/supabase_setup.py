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
    # Drop and recreate schema for a clean slate
    cur.execute('DROP SCHEMA IF EXISTS clickup CASCADE')
    cur.execute('CREATE SCHEMA clickup')
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
            timesheet_id SERIAL PRIMARY KEY,
            clickup_time_entry_id TEXT UNIQUE,
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
            due_date TIMESTAMP,
            start_date TIMESTAMP,
            date_created TIMESTAMP,
            date_updated TIMESTAMP,
            date_closed TIMESTAMP,
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
        ADD CONSTRAINT fk_time_entries_user_id FOREIGN KEY (user_id) REFERENCES clickup.team_members(id),
        ADD CONSTRAINT fk_time_entries_task_id FOREIGN KEY (task_id) REFERENCES clickup.tasks(id),
        ADD CONSTRAINT fk_time_entries_list_id FOREIGN KEY (list_id) REFERENCES clickup.lists(id),
        ADD CONSTRAINT fk_time_entries_space_id FOREIGN KEY (space_id) REFERENCES clickup.spaces(id),
        ADD CONSTRAINT fk_time_entries_sync_status FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    # Add foreign keys for tasks (remove folder_id FK)
    cur.execute('''
        ALTER TABLE clickup.tasks
        ADD CONSTRAINT fk_tasks_list_id FOREIGN KEY (list_id) REFERENCES clickup.lists(id),
        ADD CONSTRAINT fk_tasks_space_id FOREIGN KEY (space_id) REFERENCES clickup.spaces(id),
        ADD CONSTRAINT fk_tasks_sync_status FOREIGN KEY (sync_status_id) REFERENCES clickup.sync_status(id)
    ''')
    
    # 3. Create national holidays table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS clickup.national_holidays (
            date date PRIMARY KEY,
            name text
        )
    ''')
    
    # Insert Latvia National Holidays 2025
    cur.execute('''
        INSERT INTO clickup.national_holidays (date, name) VALUES
        ('2025-01-01', 'New Year''s Day'),
        ('2025-03-29', 'Good Friday'),
        ('2025-03-31', 'Easter Monday'),
        ('2025-05-01', 'Labour Day'),
        ('2025-05-04', 'Restoration of Independence'),
        ('2025-05-11', 'Mother''s Day'),
        ('2025-06-23', 'Līgo Day'),
        ('2025-06-24', 'Jāņi (Midsummer)'),
        ('2025-11-18', 'Proclamation Day of the Republic of Latvia'),
        ('2025-12-24', 'Christmas Eve'),
        ('2025-12-25', 'Christmas Day'),
        ('2025-12-26', 'Second Day of Christmas'),
        ('2025-12-31', 'New Year''s Eve')
        ON CONFLICT (date) DO NOTHING
    ''')
    
    # 4. Create PowerBI views in public schema
    # 1. clickup_dim_Tasks: Extract Account Name (Client_ID, Client_Name) from custom_fields
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Tasks AS
        SELECT
            t.id AS task_id,
            t.name AS task_name,
            t.status,
            t.orderindex,
            t.priority,
            t.due_date,
            t.start_date,
            t.date_created,
            t.date_updated,
            t.date_closed,
            t.closed,
            t.archived,
            t.list_id,
            t.folder_id,
            t.space_id,
            t.custom_id,
            t.parent,
            t.team_id,
            t.points,
            t.time_estimate,
            t.time_spent,
            t.duration_hours,
            -- Extract Account Name (Client_ID, Client_Name) from custom_fields JSONB
            (
                SELECT cf->'value'->0->>'id'
                FROM jsonb_array_elements(t.custom_fields) cf
                WHERE cf->>'name' = 'Account Name'
                LIMIT 1
            ) AS client_id,
            (
                SELECT cf->'value'->0->>'name'
                FROM jsonb_array_elements(t.custom_fields) cf
                WHERE cf->>'name' = 'Account Name'
                LIMIT 1
            ) AS client_name
        FROM clickup.tasks t
    ''')
    
    # 2. clickup_dim_Clients: All columns from tasks where list_id = 901509635413 (no JSON extraction)
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Clients AS
        SELECT *
        FROM clickup.tasks t
        WHERE t.list_id = '901509635413'
    ''')
    
    # 3. clickup_dim_Lists
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Lists AS
        SELECT * FROM clickup.lists
    ''')
    
    # 4. clickup_dim_Spaces
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Spaces AS
        SELECT * FROM clickup.spaces
    ''')
    
    # 5. clickup_dim_Folders
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Folders AS
        SELECT * FROM clickup.folders
    ''')
    
    # 6. clickup_dim_Projects: Only tasks where list_id = 901510836048, extract Account Name
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Projects AS
        SELECT
            t.*,
            (
                SELECT cf->'value'->0->>'id'
                FROM jsonb_array_elements(t.custom_fields) cf
                WHERE cf->>'name' = 'Account Name'
                LIMIT 1
            ) AS client_id,
            (
                SELECT cf->'value'->0->>'name'
                FROM jsonb_array_elements(t.custom_fields) cf
                WHERE cf->>'name' = 'Account Name'
                LIMIT 1
            ) AS client_name
        FROM clickup.tasks t
        WHERE t.list_id = '901510836048'
    ''')
    
    # 7. clickup_fact_Timesheet
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_fact_Timesheet AS
        SELECT * FROM clickup.time_entries
    ''')
    
    # 8. clickup_dim_WIP: Custom dimension (empty for now)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS public.clickup_dim_WIP (
            wip_id SERIAL PRIMARY KEY,
            wip_name TEXT
        )
    ''')
    
    # 9. clickup_dim_Calendar: Date dimension for PowerBI
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Calendar AS
        SELECT
            d::date AS date_id,
            d::date AS date,
            EXTRACT(YEAR FROM d) AS year,
            EXTRACT(MONTH FROM d) AS month,
            TO_CHAR(d, 'Mon') AS month_short,
            TO_CHAR(d, 'Month') AS month_long,
            EXTRACT(DAY FROM d) AS day,
            EXTRACT(DOY FROM d) AS day_of_year,
            EXTRACT(DOW FROM d) AS day_of_week,
            TO_CHAR(d, 'Dy') AS day_short,
            TO_CHAR(d, 'Day') AS day_name,
            EXTRACT(WEEK FROM d) AS week_of_year,
            'Q' || EXTRACT(QUARTER FROM d) || '-' || EXTRACT(YEAR FROM d) AS quarter,
            CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d) + 1 ELSE EXTRACT(YEAR FROM d) END AS fiscal_year,
            'FQ' ||
              CASE 
                WHEN EXTRACT(MONTH FROM d) BETWEEN 7 AND 9 THEN 1
                WHEN EXTRACT(MONTH FROM d) BETWEEN 10 AND 12 THEN 2
                WHEN EXTRACT(MONTH FROM d) BETWEEN 1 AND 3 THEN 3
                ELSE 4
              END || '-' ||
              (CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d) + 1 ELSE EXTRACT(YEAR FROM d) END) AS fiscal_quarter,
            (EXTRACT(DOW FROM d) IN (0,6)) AS is_weekend,
            nh.name AS holiday_name,
            CASE WHEN nh.date IS NOT NULL OR EXTRACT(DOW FROM d) IN (0,6) THEN 0 ELSE 8 END AS standard_hours,
            CASE WHEN nh.date IS NOT NULL OR EXTRACT(DOW FROM d) IN (0,6) THEN 0 ELSE 8 END AS working_hours
        FROM generate_series('2025-01-01'::date, '2025-12-31'::date, interval '1 day') d
        LEFT JOIN clickup.national_holidays nh ON d::date = nh.date
    ''')
    
    # 10. clickup_dim_Members: All columns from team_members for PowerBI
    cur.execute('''
        CREATE OR REPLACE VIEW public.clickup_dim_Members AS
        SELECT * FROM clickup.team_members
    ''')
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    create_tables() 