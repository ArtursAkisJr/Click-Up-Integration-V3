import os
import argparse
import requests
from dotenv import load_dotenv
import logging
import sys
import json
from datetime import datetime
from pathlib import Path
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)
SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')
CLICKUP_API_KEY = os.getenv('CLICKUP_API_KEY')

# Fetch all list ids and names from Supabase
def get_all_lists():
    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT
    )
    cur = conn.cursor()
    cur.execute('SELECT id, name FROM clickup.lists')
    lists = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return lists

def fetch_tasks_for_list(list_id, list_name, date_updated_gt=None):
    url = f'https://api.clickup.com/api/v2/list/{list_id}/task'
    headers = {
        'Authorization': CLICKUP_API_KEY,
        'Content-Type': 'application/json'
    }
    tasks = []
    page = 0
    limit = 100
    while True:
        params = {
            'page': page,
            'limit': limit,
            'subtasks': 'true',         # Include subtasks
            'include_closed': 'true'    # Include closed tasks
        }
        if date_updated_gt is not None:
            params['date_updated_gt'] = date_updated_gt
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            page_tasks = data.get('tasks', [])
            for task in page_tasks:
                task['list_id'] = list_id
                task['list_name'] = list_name
            tasks.extend(page_tasks)
            if len(page_tasks) < limit:
                break
            page += 1
        except Exception as e:
            logger.error(f"Failed to fetch tasks for list {list_id}: {str(e)}")
            break
    return tasks

def flatten_task(task):
    # Flatten task fields to match supabase_upload/tasks_upload.py
    # Use .get() for all fields, defaulting to None if missing
    def safe_int(val):
        try:
            if val is None:
                return None
            return int(float(val))
        except Exception:
            return None

    flat = {
        'id': task.get('id'),
        'name': task.get('name'),
        'status': json.dumps(task.get('status')) if task.get('status') is not None else None,
        'orderindex': safe_int(task.get('orderindex')),
        'priority': json.dumps(task.get('priority')) if task.get('priority') is not None else None,
        'assignees': json.dumps(task.get('assignees')) if task.get('assignees') is not None else None,
        'creator': json.dumps(task.get('creator')) if task.get('creator') is not None else None,
        'due_date': task.get('due_date'),
        'start_date': task.get('start_date'),
        'date_created': task.get('date_created'),
        'date_updated': task.get('date_updated'),
        'date_closed': task.get('date_closed'),
        'closed': task.get('closed'),
        'archived': task.get('archived'),
        'url': task.get('url'),
        'list_id': task.get('list_id'),
        'list_name': task.get('list_name'),
        'folder_id': task.get('folder', {}).get('id') if isinstance(task.get('folder'), dict) else None,
        'space_id': task.get('space', {}).get('id') if isinstance(task.get('space'), dict) else None,
        'custom_id': task.get('custom_id'),
        'parent': task.get('parent'),
        'tags': json.dumps(task.get('tags')) if task.get('tags') is not None else None,
        'checklists': json.dumps(task.get('checklists')) if task.get('checklists') is not None else None,
        'custom_fields': json.dumps(task.get('custom_fields')) if task.get('custom_fields') is not None else None,
        'team_id': task.get('team_id'),
        'points': task.get('points'),
        'time_estimate': task.get('time_estimate'),
        'time_spent': task.get('time_spent'),
        'dependencies': json.dumps(task.get('dependencies')) if task.get('dependencies') is not None else None,
        'linked_tasks': json.dumps(task.get('linked_tasks')) if task.get('linked_tasks') is not None else None,
        'subtasks': json.dumps(task.get('subtasks')) if task.get('subtasks') is not None else None,
        'attachments': json.dumps(task.get('attachments')) if task.get('attachments') is not None else None,
        'comments': json.dumps(task.get('comments')) if task.get('comments') is not None else None,
        'description': task.get('description'),
        'text_content': task.get('text_content'),
        'created_by': task.get('created_by'),
        'updated_by': task.get('updated_by'),
        'recurring': json.dumps(task.get('recurring')) if task.get('recurring') is not None else None,
        'template_id': task.get('template_id'),
        'time_in_status': json.dumps(task.get('time_in_status')) if task.get('time_in_status') is not None else None,
        'time_tracking': json.dumps(task.get('time_tracking')) if task.get('time_tracking') is not None else None,
        'reminders': json.dumps(task.get('reminders')) if task.get('reminders') is not None else None,
        'watchers': json.dumps(task.get('watchers')) if task.get('watchers') is not None else None,
        'parent_ids': json.dumps(task.get('parent_ids')) if task.get('parent_ids') is not None else None,
        'subtask_ids': json.dumps(task.get('subtask_ids')) if task.get('subtask_ids') is not None else None,
        'linked_task_ids': json.dumps(task.get('linked_task_ids')) if task.get('linked_task_ids') is not None else None,
        'permission_level': task.get('permission_level'),
        'list_type': task.get('list_type'),
        'start_time': task.get('start_time'),
        'due_time': task.get('due_time'),
        'date_done': task.get('date_done'),
        'date_started': task.get('date_started'),
        'date_last_activity': task.get('date_last_activity'),
        'date_template': task.get('date_template'),
        'date_reminder': task.get('date_reminder'),
        'date_recurring': task.get('date_recurring'),
        'date_closed_by': task.get('date_closed_by'),
        'date_created_by': task.get('date_created_by'),
        'date_updated_by': task.get('date_updated_by'),
        'date_archived': task.get('date_archived'),
        'date_deleted': task.get('date_deleted'),
        'date_restored': task.get('date_restored'),
        'date_trashed': task.get('date_trashed'),
        'date_moved': task.get('date_moved'),
        'date_duplicated': task.get('date_duplicated'),
        'date_merged': task.get('date_merged'),
        'date_split': task.get('date_split'),
        'date_linked': task.get('date_linked'),
        'date_unlinked': task.get('date_unlinked'),
        'date_parent_changed': task.get('date_parent_changed'),
        'date_subtask_changed': task.get('date_subtask_changed'),
        'date_priority_changed': task.get('date_priority_changed'),
        'date_status_changed': task.get('date_status_changed'),
        'date_assignee_changed': task.get('date_assignee_changed'),
        'date_due_changed': task.get('date_due_changed'),
        'date_start_changed': task.get('date_start_changed'),
        'date_time_estimate_changed': task.get('date_time_estimate_changed'),
        'date_time_spent_changed': task.get('date_time_spent_changed'),
        'date_points_changed': task.get('date_points_changed'),
        'date_tags_changed': task.get('date_tags_changed'),
        'date_custom_fields_changed': task.get('date_custom_fields_changed'),
        'date_watchers_changed': task.get('date_watchers_changed'),
        'date_comments_changed': task.get('date_comments_changed'),
        'date_checklists_changed': task.get('date_checklists_changed'),
        'date_attachments_changed': task.get('date_attachments_changed'),
        'date_dependencies_changed': task.get('date_dependencies_changed'),
        'date_linked_tasks_changed': task.get('date_linked_tasks_changed'),
        'date_subtasks_changed': task.get('date_subtasks_changed'),
        'date_recurring_changed': task.get('date_recurring_changed'),
        'date_template_changed': task.get('date_template_changed'),
        'date_permission_level_changed': task.get('date_permission_level_changed'),
        'date_list_type_changed': task.get('date_list_type_changed'),
        'date_icon_changed': task.get('date_icon_changed'),
        'date_public_changed': task.get('date_public_changed'),
        'date_drop_down_changed': task.get('date_drop_down_changed'),
    }
    return flat

def main():
    parser = argparse.ArgumentParser(description='Fetch ClickUp tasks')
    parser.add_argument('--date_updated_gt', required=False, type=int,
                        help='Only fetch tasks updated after this Unix ms timestamp (incremental sync)')
    args = parser.parse_args()

    try:
        all_lists = get_all_lists()
        all_tasks = []
        for lst in all_lists:
            tasks = fetch_tasks_for_list(lst['id'], lst['name'], date_updated_gt=args.date_updated_gt)
            for task in tasks:
                flat = flatten_task(task)
                all_tasks.append(flat)
        # Save to JSON file in clickup_api_response
        response_dir = Path(__file__).parent.parent / 'clickup_api_response'
        response_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = response_dir / f'get_tasks_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(all_tasks, f, indent=2)
        logger.info(f"Tasks data saved to {output_file}")
        # Print pure JSON to stdout
        print(json.dumps(all_tasks))
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 