import os
import requests
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env',override=True)

CLICKUP_API_KEY = os.getenv('CLICKUP_API_KEY')
CLICKUP_TEAM_ID = os.getenv('CLICKUP_TEAM_ID')

if not CLICKUP_API_KEY or not CLICKUP_TEAM_ID:
    print('Missing CLICKUP_API_KEY or CLICKUP_TEAM_ID in environment variables.')
    exit(1)

url = f'https://api.clickup.com/api/v2/team/{CLICKUP_TEAM_ID}'
headers = {
    'Authorization': CLICKUP_API_KEY
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    print('Successfully connected to ClickUp API!')
    print('Team Info:', response.json())
else:
    print(f'Failed to connect to ClickUp API. Status code: {response.status_code}')
    print('Response:', response.text) 