import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


load_dotenv(dotenv_path=Path(__file__).parent.parent / "env_real.env", override=True)

SUPABASE_DB_USER = os.getenv("SUPABASE_DB_USER")
SUPABASE_DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
SUPABASE_DB_HOST = os.getenv("SUPABASE_DB_HOST")
SUPABASE_DB_PORT = os.getenv("SUPABASE_DB_PORT")
SUPABASE_DB_NAME = os.getenv("SUPABASE_DB_NAME")


def normalize_status_column():
    """
    Normalize clickup.tasks.status so it always contains plain text,
    not a JSON string.

    Any rows where status currently looks like JSON (starts with '{')
    are cast to jsonb and we extract the "status" field.
    """
    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT,
    )
    cur = conn.cursor()

    # Only touch rows where status appears to be JSON to avoid
    # breaking already-normalized plain-text statuses.
    update_sql = """
        UPDATE clickup.tasks
        SET status = (status::jsonb ->> 'status')
        WHERE status IS NOT NULL
          AND status LIKE '{%%'
    """
    cur.execute(update_sql)
    updated = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()

    print(f"Normalized status column for {updated} task rows.")


if __name__ == "__main__":
    normalize_status_column()

