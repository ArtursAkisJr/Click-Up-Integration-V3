import os
import requests
from dotenv import load_dotenv
import logging
import sys
import json
from datetime import datetime
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

class ClickUpSpaces:
    def __init__(self):
        logger.info("Initializing ClickUpSpaces")
        # Load environment variables from the project root
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
    def get_spaces(self):
        """
        Get all spaces for the team from ClickUp
        Returns:
            list: List of dictionaries containing space information
        """
        url = f'https://api.clickup.com/api/v2/team/{self.clickup_team_id}/space'
        try:
            logger.info(f"Fetching spaces from {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            spaces = data.get('spaces', [])
            logger.info(f"Successfully fetched {len(spaces)} spaces")
            return spaces
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch spaces: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text}")
            raise

def main():
    try:
        spaces_client = ClickUpSpaces()
        spaces = spaces_client.get_spaces()
        print("\nClickUp Spaces:")
        print("-" * 80)
        for space in spaces:
            print(f"ID: {space.get('id')}")
            print(f"Name: {space.get('name')}")
            print(f"Private: {space.get('private')}")
            print("-" * 80)
        # Save to JSON file in clickup_api_response
        response_dir = Path(__file__).parent.parent / 'clickup_api_response'
        response_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = response_dir / f'get_spaces_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(spaces, f, indent=2)
        logger.info(f"Spaces data saved to {output_file}")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 