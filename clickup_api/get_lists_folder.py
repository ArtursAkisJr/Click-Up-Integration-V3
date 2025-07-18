import os
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

# Fetch all folder ids and space ids from Supabase
def get_all_folders():
    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT
    )
    cur = conn.cursor()
    cur.execute('SELECT id, space_id FROM clickup.folders')
    folders = [{'id': row[0], 'space_id': row[1]} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return folders

def fetch_lists_for_folder(folder_id):
    url = f'https://api.clickup.com/api/v2/folder/{folder_id}/list'
    headers = {
        'Authorization': CLICKUP_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get('lists', [])
    except Exception as e:
        logger.error(f"Failed to fetch lists for folder {folder_id}: {str(e)}")
        return []

def flatten_list(list_obj, folder_id, space_id):
    flat = {
        'id': list_obj.get('id'),
        'name': list_obj.get('name'),
        'orderindex': list_obj.get('orderindex'),
        'content': list_obj.get('content'),
        'status': list_obj.get('status'),
        'priority': list_obj.get('priority'),
        'assignee': list_obj.get('assignee'),
        'task_count': list_obj.get('task_count'),
        'due_date': list_obj.get('due_date'),
        'start_date': list_obj.get('start_date'),
        'folder_id': folder_id,
        'space_id': space_id,
        'archived': list_obj.get('archived'),
        'override_statuses': list_obj.get('override_statuses'),
        'permission_level': list_obj.get('permission_level'),
        'statuses': list_obj.get('statuses'),
        'template_id': list_obj.get('template_id'),
        'public': list_obj.get('public'),
        'drop_down': list_obj.get('drop_down'),
        'created_at': list_obj.get('created_at'),
        'updated_at': list_obj.get('updated_at'),
        'icon': list_obj.get('icon'),
        'list_type': list_obj.get('list_type'),
        'custom_fields': list_obj.get('custom_fields'),
    }
    return flat

def main():
    try:
        all_folders = get_all_folders()
        all_lists = []
        for folder in all_folders:
            lists = fetch_lists_for_folder(folder['id'])
            for lst in lists:
                flat = flatten_list(lst, folder['id'], folder['space_id'])
                all_lists.append(flat)
        # Save to JSON file in clickup_api_response
        response_dir = Path(__file__).parent.parent / 'clickup_api_response'
        response_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = response_dir / f'get_lists_folder_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(all_lists, f, indent=2)
        logger.info(f"Folder lists data saved to {output_file}")
        # Print pure JSON to stdout
        print(json.dumps(all_lists))
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 