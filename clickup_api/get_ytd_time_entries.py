import os
import requests
from dotenv import load_dotenv
import logging
import sys
import json
from datetime import datetime, date
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ClickUpYTDTimeEntries:
    def __init__(self):
        logger.info("Initializing ClickUpYTDTimeEntries")
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
    def get_all_user_ids(self):
        # If CLICKUP_USER_ID is set, use it; otherwise, log a warning and return an empty list
        user_id = os.getenv('CLICKUP_USER_ID')
        if user_id:
            return [user_id]
        else:
            logger.warning('No user IDs provided and CLICKUP_USER_ID not set in environment. Will attempt to fetch for API key owner only.')
            return []
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
                all_entries.extend(entries)
                if len(entries) < 100:
                    break
                page += 1
            except Exception as e:
                logger.error(f"Failed to fetch time entries, page {page + 1}: {str(e)}")
                break
        return all_entries

def main():
    current_year = date.today().year
    start_date = datetime(current_year, 1, 1)
    end_date = datetime(current_year, 12, 31)
    try:
        client = ClickUpYTDTimeEntries()
        user_ids = client.get_all_user_ids()
        if not user_ids:
            logger.warning("No user IDs found. Attempting to fetch time entries for API key owner only.")
            user_ids = []
        entries = client.get_time_entries(user_ids, start_date, end_date)
        print(f"Fetched {len(entries)} YTD time entries.")
        # Save to JSON file in clickup_api_response
        response_dir = Path(__file__).parent.parent / 'clickup_api_response'
        response_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = response_dir / f'get_ytd_time_entries_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(entries, f, indent=2)
        logger.info(f"YTD time entries data saved to {output_file}")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 