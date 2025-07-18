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
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

class ClickUpUsers:
    def __init__(self):
        logger.info("Initializing ClickUpUsers")
        # Load environment variables from the project root
        load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)
        
        self.clickup_api_key = os.getenv('CLICKUP_API_KEY')
        self.clickup_team_id = os.getenv('CLICKUP_TEAM_ID')
        
        # Validate required environment variables
        if not self.clickup_api_key:
            raise ValueError("CLICKUP_API_KEY not found in environment variables")
        if not self.clickup_team_id:
            raise ValueError("CLICKUP_TEAM_ID not found in environment variables")
        
        self.headers = {
            'Authorization': self.clickup_api_key,
            'Content-Type': 'application/json'
        }
    
    def get_team_members(self):
        """
        Get all team members from ClickUp
        Returns:
            list: List of dictionaries containing user information
        """
        url = f'https://api.clickup.com/api/v2/team/{self.clickup_team_id}'
        try:
            logger.info(f"Fetching team members from {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            team = data.get('team', {})
            members = team.get('members', [])
            # Extract relevant user information
            users = []
            for member in members:
                user = member.get('user', {})
                users.append({
                    'id': user.get('id'),
                    'username': user.get('username'),
                    'email': user.get('email'),
                    'color': user.get('color'),
                    'profilePicture': user.get('profilePicture'),
                    'initials': user.get('initials'),
                    'role': member.get('role')  # Team role (admin, member, etc.)
                })
            logger.info(f"Successfully fetched {len(users)} team members")
            return users
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch team members: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text}")
            raise

def main():
    try:
        # Create an instance of ClickUpUsers
        users_client = ClickUpUsers()
        # Get team members
        users = users_client.get_team_members()
        # Print users in a readable format
        print("\nClickUp Team Members:")
        print("-" * 80)
        for user in users:
            print(f"ID: {user['id']}")
            print(f"Username: {user['username']}")
            print(f"Email: {user['email']}")
            print(f"Role: {user['role']}")
            print(f"Initials: {user['initials']}")
            print("-" * 80)
        # Optionally save to a JSON file in clickup_api_response
        response_dir = Path(__file__).parent.parent / 'clickup_api_response'
        response_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = response_dir / f'get_team_members_{timestamp}.json'
        with open(output_file, 'w') as f:
            json.dump(users, f, indent=2)
        logger.info(f"User data saved to {output_file}")
        # Print pure JSON to stdout
        print(json.dumps(users))
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 