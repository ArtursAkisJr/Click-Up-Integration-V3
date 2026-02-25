import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent.parent / "env_real.env", override=True)

SUPABASE_DB_USER = os.getenv("SUPABASE_DB_USER")
SUPABASE_DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
SUPABASE_DB_HOST = os.getenv("SUPABASE_DB_HOST")
SUPABASE_DB_PORT = os.getenv("SUPABASE_DB_PORT")
SUPABASE_DB_NAME = os.getenv("SUPABASE_DB_NAME")


def get_active_projects(conn):
    """Return (id, name, status, client_name) for active projects from dim_Projects."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, status, client_name
        FROM public.clickup_dim_Projects
        WHERE status = 'active'
        ORDER BY client_name NULLS LAST, name
        """
    )
    rows = cur.fetchall()
    cur.close()
    return rows


INVALID_FS_CHARS = '<>:"/\\\\|?*'


def sanitize_for_fs(name: str) -> str:
    """Make a string safe for use as a folder name on Windows."""
    if not name:
        return "unnamed_project"
    cleaned = "".join(c for c in name if c not in INVALID_FS_CHARS)
    cleaned = cleaned.strip().rstrip(". ")
    return cleaned or "unnamed_project"


def main():
    base_path_env = os.getenv("PROJECTS_BASE_PATH", "").strip()
    if base_path_env:
        base_dir = Path(base_path_env)
    else:
        # Default to ./projects under current working directory
        base_dir = Path.cwd() / "projects"

    base_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Using base directory: %s", base_dir)

    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT,
    )

    try:
        projects = get_active_projects(conn)
        logger.info("Found %d active projects", len(projects))

        for project_id, project_name, status, client_name in projects:
            client_part = sanitize_for_fs(client_name) if client_name else "UnknownClient"
            project_part = sanitize_for_fs(project_name or project_id)

            target_dir = base_dir / client_part / project_part
            target_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Ensured folder for project %s (%s): %s",
                project_id,
                status,
                target_dir,
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()

