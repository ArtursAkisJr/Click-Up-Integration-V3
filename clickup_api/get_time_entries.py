import os
import requests
from dotenv import load_dotenv
import logging
import sys
import json
from datetime import datetime
from pathlib import Path
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

class ClickUpTimeEntries:
    def __init__(self):
        logger.info("Initializing ClickUpTimeEntries")
        load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)
        self.clickup_api_key = os.getenv('CLICKUP_API_KEY')
        self.clickup_team_id = os.getenv('CLICKUP_TEAM_ID')
        if not self.clickup_api_key:
            raise ValueError("CLICKUP_API_KEY not found in environment variables")
        if not self.clickup_team_id:
            raise ValueError("CLICKUP_TEAM_ID not found in environment variables")
        self.headers = {
            'Authorization': self.clickup_api_key,
            'Content-Type': 'application/json'
        }
    def get_time_entries(self, user_ids, start_date, end_date):
        url = f'https://api.clickup.com/api/v2/team/{self.clickup_team_id}/time_entries'
        params = {}
        if user_ids:
            params['assignee'] = ','.join(user_ids)
        if start_date:
            params['start_date'] = int(start_date.timestamp() * 1000)
        if end_date:
            params['end_date'] = int(end_date.timestamp() * 1000)
        all_entries = []
        seen_entry_ids = set()
        page = 0
        max_pages = 50
        while page < max_pages:
            params['page'] = page
            try:
                logger.info(f"Fetching time entries, page {page + 1}")
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                entries = data.get('data', [])
                if not entries:
                    break
                
                # Deduplicate entries by ID
                for entry in entries:
                    entry_id = entry.get('id')
                    if entry_id and entry_id not in seen_entry_ids:
                        seen_entry_ids.add(entry_id)
                        all_entries.append(entry)
                
                if len(entries) < 100:
                    break
                page += 1
            except Exception as e:
                logger.error(f"Failed to fetch time entries, page {page + 1}: {str(e)}")
                break
        
        logger.info(f"Fetched {len(all_entries)} unique time entries from {len(seen_entry_ids)} total entries seen")
        return all_entries

def main():
    parser = argparse.ArgumentParser(description='Fetch ClickUp time entries for user IDs and date range')
    parser.add_argument('--user_ids', nargs='+', help='List of ClickUp user IDs (optional, defaults to API key owner)')
    parser.add_argument('--start_date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    user_ids = args.user_ids if args.user_ids else [os.getenv('CLICKUP_USER_ID')]
    if not user_ids or not user_ids[0]:
        print('No user IDs provided and CLICKUP_USER_ID not set in environment.')
        sys.exit(1)
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    try:
        client = ClickUpTimeEntries()
        entries = client.get_time_entries(user_ids, start_date, end_date)
        print(f"Fetched {len(entries)} time entries.")
        # Save to JSON file in clickup_api_response
        response_dir = Path(__file__).parent.parent / 'clickup_api_response'
        response_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = response_dir / f'get_time_entries_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(entries, f, indent=2)
        logger.info(f"Time entries data saved to {output_file}")
        # Print pure JSON to stdout
        print(json.dumps(entries))
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 