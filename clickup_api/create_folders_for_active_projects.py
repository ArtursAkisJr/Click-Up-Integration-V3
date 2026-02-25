import os
from pathlib import Path

import logging
import requests
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent.parent / "env_real.env", override=True)

CLICKUP_API_KEY = os.getenv("CLICKUP_API_KEY")
# Space where project folders should be created
CLICKUP_PROJECT_SPACE_ID = os.getenv("CLICKUP_PROJECT_SPACE_ID")
# List that contains all project tasks
CLICKUP_PROJECT_LIST_ID = os.getenv("CLICKUP_PROJECT_LIST_ID")


def get_active_project_task_names(session: requests.Session, list_id: str) -> list[str]:
    """
    Return distinct names of active tasks from the given ClickUp list.

    This assumes the list contains one task per project and that
    an "active" project has task.status.status = 'active'.
    """
    url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
    headers = {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json",
    }

    names: set[str] = set()
    page = 0
    limit = 100

    while True:
        params = {
            "page": page,
            "limit": limit,
            "subtasks": "false",
            "include_closed": "false",
        }
        resp = session.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        tasks = data.get("tasks", [])

        for task in tasks:
            raw_status = task.get("status")
            if isinstance(raw_status, dict):
                status = raw_status.get("status")
            else:
                status = raw_status

            if status != "active":
                continue

            name = task.get("name")
            if name:
                names.add(name)

        if len(tasks) < limit:
            break
        page += 1

    return sorted(names)


def get_existing_folder_names(session: requests.Session, space_id: str) -> set[str]:
    """Fetch existing folder names in the given ClickUp space."""
    url = f"https://api.clickup.com/api/v2/space/{space_id}/folder"
    headers = {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json",
    }
    params = {"archived": "false"}
    resp = session.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    folders = data.get("folders", [])
    return {f.get("name") for f in folders if f.get("name")}


def create_folder(session: requests.Session, space_id: str, name: str) -> None:
    """Create a single folder in the given ClickUp space."""
    url = f"https://api.clickup.com/api/v2/space/{space_id}/folder"
    headers = {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {"name": name}
    resp = session.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    logger.info("Created folder '%s' in space %s", name, space_id)


def main():
    if not CLICKUP_API_KEY:
        raise SystemExit("CLICKUP_API_KEY is not set in env_real.env")
    if not CLICKUP_PROJECT_SPACE_ID:
        raise SystemExit("CLICKUP_PROJECT_SPACE_ID is not set in env_real.env")
    if not CLICKUP_PROJECT_LIST_ID:
        raise SystemExit("CLICKUP_PROJECT_LIST_ID is not set in env_real.env")

    with requests.Session() as session:
        project_names = get_active_project_task_names(session, CLICKUP_PROJECT_LIST_ID)
        logger.info("Found %d active project tasks in list %s", len(project_names), CLICKUP_PROJECT_LIST_ID)

        existing_names = get_existing_folder_names(session, CLICKUP_PROJECT_SPACE_ID)
        logger.info(
            "Existing folders in space %s: %d",
            CLICKUP_PROJECT_SPACE_ID,
            len(existing_names),
        )

        for name in project_names:
            if name in existing_names:
                logger.info("Folder already exists for project '%s', skipping", name)
                continue
            create_folder(session, CLICKUP_PROJECT_SPACE_ID, name)


if __name__ == "__main__":
    main()

