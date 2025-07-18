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

# Fetch all space ids and names from Supabase
def get_all_spaces():
    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT
    )
    cur = conn.cursor()
    cur.execute('SELECT id, name FROM clickup.spaces')
    spaces = [{'id': row[0], 'name': row[1]} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return spaces

def fetch_folders_for_space(space_id):
    url = f'https://api.clickup.com/api/v2/space/{space_id}/folder'
    headers = {
        'Authorization': CLICKUP_API_KEY,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get('folders', [])
    except Exception as e:
        logger.error(f"Failed to fetch folders for space {space_id}: {str(e)}")
        return []

def main():
    try:
        all_spaces = get_all_spaces()
        all_folders = []
        for space in all_spaces:
            folders = fetch_folders_for_space(space['id'])
            for folder in folders:
                # Flatten space object and convert lists to array of ids
                folder_obj = {
                    'id': folder.get('id'),
                    'name': folder.get('name'),
                    'orderindex': folder.get('orderindex'),
                    'override_statuses': folder.get('override_statuses'),
                    'hidden': folder.get('hidden'),
                    'space_id': folder.get('space', {}).get('id'),
                    'space_name': folder.get('space', {}).get('name'),
                    'task_count': int(folder.get('task_count')) if folder.get('task_count') is not None else None,
                    'lists': [lst.get('id') for lst in folder.get('lists', [])],
                }
                all_folders.append(folder_obj)
        # Save to JSON file in clickup_api_response
        response_dir = Path(__file__).parent.parent / 'clickup_api_response'
        response_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = response_dir / f'get_folders_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(all_folders, f, indent=2)
        logger.info(f"Folders data saved to {output_file}")
        # Print pure JSON to stdout
        print(json.dumps(all_folders))
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 