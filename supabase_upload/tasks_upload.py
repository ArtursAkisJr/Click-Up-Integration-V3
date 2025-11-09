import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from pathlib import Path
import logging
import json
from datetime import datetime

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

def parse_unix_ms_to_datetime(val):
    try:
        if val is None or val == '' or int(val) == 0:
            return None
        # Accept both string and int
        return datetime.utcfromtimestamp(int(val) / 1000)
    except Exception:
        return None

def upload_tasks_to_supabase(tasks, sync_status_id, sync_status_timestamp):
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
        for task in tasks:
            time_spent = task.get('time_spent')
            duration_hours = float(time_spent) / 3600000 if time_spent not in (None, '', 0) else None
            records.append((
                task.get('id'),
                task.get('name'),
                task.get('status'),
                task.get('orderindex'),
                task.get('priority'),
                task.get('assignees'),
                task.get('creator'),
                parse_unix_ms_to_datetime(task.get('due_date')),
                parse_unix_ms_to_datetime(task.get('start_date')),
                parse_unix_ms_to_datetime(task.get('date_created')),
                parse_unix_ms_to_datetime(task.get('date_updated')),
                parse_unix_ms_to_datetime(task.get('date_closed')),
                task.get('closed'),
                task.get('archived'),
                task.get('url'),
                task.get('list_id'),
                task.get('list_name'),
                task.get('folder_id'),
                task.get('space_id'),
                task.get('custom_id'),
                task.get('parent'),
                task.get('tags'),
                task.get('checklists'),
                task.get('custom_fields'),
                task.get('team_id'),
                task.get('points'),
                task.get('time_estimate'),
                time_spent,
                duration_hours,
                task.get('dependencies'),
                task.get('linked_tasks'),
                task.get('subtasks'),
                task.get('attachments'),
                task.get('comments'),
                task.get('description'),
                task.get('text_content'),
                task.get('created_by'),
                task.get('updated_by'),
                task.get('recurring'),
                task.get('template_id'),
                task.get('time_in_status'),
                task.get('time_tracking'),
                task.get('reminders'),
                task.get('watchers'),
                task.get('parent_ids'),
                task.get('subtask_ids'),
                task.get('linked_task_ids'),
                task.get('permission_level'),
                task.get('list_type'),
                task.get('start_time'),
                task.get('due_time'),
                task.get('date_done'),
                task.get('date_started'),
                task.get('date_last_activity'),
                task.get('date_template'),
                task.get('date_reminder'),
                task.get('date_recurring'),
                task.get('date_closed_by'),
                task.get('date_created_by'),
                task.get('date_updated_by'),
                task.get('date_archived'),
                task.get('date_deleted'),
                task.get('date_restored'),
                task.get('date_trashed'),
                task.get('date_moved'),
                task.get('date_duplicated'),
                task.get('date_merged'),
                task.get('date_split'),
                task.get('date_linked'),
                task.get('date_unlinked'),
                task.get('date_parent_changed'),
                task.get('date_subtask_changed'),
                task.get('date_priority_changed'),
                task.get('date_status_changed'),
                task.get('date_assignee_changed'),
                task.get('date_due_changed'),
                task.get('date_start_changed'),
                task.get('date_time_estimate_changed'),
                task.get('date_time_spent_changed'),
                task.get('date_points_changed'),
                task.get('date_tags_changed'),
                task.get('date_custom_fields_changed'),
                task.get('date_watchers_changed'),
                task.get('date_comments_changed'),
                task.get('date_checklists_changed'),
                task.get('date_attachments_changed'),
                task.get('date_dependencies_changed'),
                task.get('date_linked_tasks_changed'),
                task.get('date_subtasks_changed'),
                task.get('date_recurring_changed'),
                task.get('date_template_changed'),
                task.get('date_permission_level_changed'),
                task.get('date_list_type_changed'),
                task.get('date_icon_changed'),
                task.get('date_public_changed'),
                task.get('date_drop_down_changed'),
                sync_status_id,
                sync_status_timestamp
            ))
        # Batch insert
        insert_sql = '''
            INSERT INTO clickup.tasks (
                id, name, status, orderindex, priority, assignees, creator, due_date, start_date, date_created, date_updated, date_closed, closed, archived, url, list_id, list_name, folder_id, space_id, custom_id, parent, tags, checklists, custom_fields, team_id, points, time_estimate, time_spent, duration_hours, dependencies, linked_tasks, subtasks, attachments, comments, description, text_content, created_by, updated_by, recurring, template_id, time_in_status, time_tracking, reminders, watchers, parent_ids, subtask_ids, linked_task_ids, permission_level, list_type, start_time, due_time, date_done, date_started, date_last_activity, date_template, date_reminder, date_recurring, date_closed_by, date_created_by, date_updated_by, date_archived, date_deleted, date_restored, date_trashed, date_moved, date_duplicated, date_merged, date_split, date_linked, date_unlinked, date_parent_changed, date_subtask_changed, date_priority_changed, date_status_changed, date_assignee_changed, date_due_changed, date_start_changed, date_time_estimate_changed, date_time_spent_changed, date_points_changed, date_tags_changed, date_custom_fields_changed, date_watchers_changed, date_comments_changed, date_checklists_changed, date_attachments_changed, date_dependencies_changed, date_linked_tasks_changed, date_subtasks_changed, date_recurring_changed, date_template_changed, date_permission_level_changed, date_list_type_changed, date_icon_changed, date_public_changed, date_drop_down_changed, sync_status_id, sync_status_timestamp
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name, status = EXCLUDED.status, orderindex = EXCLUDED.orderindex, priority = EXCLUDED.priority,
                assignees = EXCLUDED.assignees, creator = EXCLUDED.creator, due_date = EXCLUDED.due_date, start_date = EXCLUDED.start_date,
                date_created = EXCLUDED.date_created, date_updated = EXCLUDED.date_updated, date_closed = EXCLUDED.date_closed,
                closed = EXCLUDED.closed, archived = EXCLUDED.archived, url = EXCLUDED.url, list_id = EXCLUDED.list_id,
                list_name = EXCLUDED.list_name, folder_id = EXCLUDED.folder_id, space_id = EXCLUDED.space_id, custom_id = EXCLUDED.custom_id,
                parent = EXCLUDED.parent, tags = EXCLUDED.tags, checklists = EXCLUDED.checklists, custom_fields = EXCLUDED.custom_fields,
                team_id = EXCLUDED.team_id, points = EXCLUDED.points, time_estimate = EXCLUDED.time_estimate, time_spent = EXCLUDED.time_spent,
                duration_hours = EXCLUDED.duration_hours, dependencies = EXCLUDED.dependencies, linked_tasks = EXCLUDED.linked_tasks,
                subtasks = EXCLUDED.subtasks, attachments = EXCLUDED.attachments, comments = EXCLUDED.comments, description = EXCLUDED.description,
                text_content = EXCLUDED.text_content, created_by = EXCLUDED.created_by, updated_by = EXCLUDED.updated_by,
                recurring = EXCLUDED.recurring, template_id = EXCLUDED.template_id, time_in_status = EXCLUDED.time_in_status,
                time_tracking = EXCLUDED.time_tracking, reminders = EXCLUDED.reminders, watchers = EXCLUDED.watchers,
                parent_ids = EXCLUDED.parent_ids, subtask_ids = EXCLUDED.subtask_ids, linked_task_ids = EXCLUDED.linked_task_ids,
                permission_level = EXCLUDED.permission_level, list_type = EXCLUDED.list_type, start_time = EXCLUDED.start_time,
                due_time = EXCLUDED.due_time, date_done = EXCLUDED.date_done, date_started = EXCLUDED.date_started,
                date_last_activity = EXCLUDED.date_last_activity, date_template = EXCLUDED.date_template, date_reminder = EXCLUDED.date_reminder,
                date_recurring = EXCLUDED.date_recurring, date_closed_by = EXCLUDED.date_closed_by, date_created_by = EXCLUDED.date_created_by,
                date_updated_by = EXCLUDED.date_updated_by, date_archived = EXCLUDED.date_archived, date_deleted = EXCLUDED.date_deleted,
                date_restored = EXCLUDED.date_restored, date_trashed = EXCLUDED.date_trashed, date_moved = EXCLUDED.date_moved,
                date_duplicated = EXCLUDED.date_duplicated, date_merged = EXCLUDED.date_merged, date_split = EXCLUDED.date_split,
                date_linked = EXCLUDED.date_linked, date_unlinked = EXCLUDED.date_unlinked, date_parent_changed = EXCLUDED.date_parent_changed,
                date_subtask_changed = EXCLUDED.date_subtask_changed, date_priority_changed = EXCLUDED.date_priority_changed,
                date_status_changed = EXCLUDED.date_status_changed, date_assignee_changed = EXCLUDED.date_assignee_changed,
                date_due_changed = EXCLUDED.date_due_changed, date_start_changed = EXCLUDED.date_start_changed,
                date_time_estimate_changed = EXCLUDED.date_time_estimate_changed, date_time_spent_changed = EXCLUDED.date_time_spent_changed,
                date_points_changed = EXCLUDED.date_points_changed, date_tags_changed = EXCLUDED.date_tags_changed,
                date_custom_fields_changed = EXCLUDED.date_custom_fields_changed, date_watchers_changed = EXCLUDED.date_watchers_changed,
                date_comments_changed = EXCLUDED.date_comments_changed, date_checklists_changed = EXCLUDED.date_checklists_changed,
                date_attachments_changed = EXCLUDED.date_attachments_changed, date_dependencies_changed = EXCLUDED.date_dependencies_changed,
                date_linked_tasks_changed = EXCLUDED.date_linked_tasks_changed, date_subtasks_changed = EXCLUDED.date_subtasks_changed,
                date_recurring_changed = EXCLUDED.date_recurring_changed, date_template_changed = EXCLUDED.date_template_changed,
                date_permission_level_changed = EXCLUDED.date_permission_level_changed, date_list_type_changed = EXCLUDED.date_list_type_changed,
                date_icon_changed = EXCLUDED.date_icon_changed, date_public_changed = EXCLUDED.date_public_changed,
                date_drop_down_changed = EXCLUDED.date_drop_down_changed, sync_status_id = EXCLUDED.sync_status_id,
                sync_status_timestamp = EXCLUDED.sync_status_timestamp
        '''
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i+BATCH_SIZE]
            execute_values(cur, insert_sql, batch)
        # Note: We don't delete orphaned tasks here to avoid foreign key constraint issues
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Uploaded {len(tasks)} tasks to Supabase in batches of {BATCH_SIZE}.")
    except Exception as e:
        logger.error(f"Error uploading tasks to Supabase: {e}")
        raise 