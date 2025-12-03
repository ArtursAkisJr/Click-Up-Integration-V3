import subprocess
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYNC_ORDER = [
    'sync_spaces.py',
    'sync_team_members.py',
    'sync_folders.py',
    'sync_lists.py',
    'sync_tasks.py',
    'sync_ytd_time_entries_all_users.py',
    'sync_latvia_holidays.py',
]

SCRIPT_DIR = Path(__file__).parent


def run_script(script_name):
    script_path = SCRIPT_DIR / script_name
    logger.info(f'Running {script_name}...')
    result = subprocess.run([sys.executable, str(script_path)])
    if result.returncode != 0:
        logger.error(f'{script_name} failed with exit code {result.returncode}. Stopping sync.')
        sys.exit(result.returncode)
    logger.info(f'{script_name} completed successfully.')


def main():
    for script in SYNC_ORDER:
        run_script(script)
    logger.info('All syncs completed successfully.')

if __name__ == '__main__':
    main() 