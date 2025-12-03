import os
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


load_dotenv(dotenv_path=Path(__file__).parent.parent / "env_real.env", override=True)

SUPABASE_DB_USER = os.getenv("SUPABASE_DB_USER")
SUPABASE_DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
SUPABASE_DB_HOST = os.getenv("SUPABASE_DB_HOST")
SUPABASE_DB_PORT = os.getenv("SUPABASE_DB_PORT")
SUPABASE_DB_NAME = os.getenv("SUPABASE_DB_NAME")


def upload_latvia_holidays(holidays: list[dict]) -> int:
    """
    Upsert Latvia public holidays into clickup.national_holidays.

    The Nager API returns a list of objects with (among others):
      - date: 'YYYY-MM-DD'
      - localName / name
      - countryCode

    We only persist (date, name) and let the calendar view join on date.
    """
    if not holidays:
        return 0

    conn = psycopg2.connect(
        dbname=SUPABASE_DB_NAME,
        user=SUPABASE_DB_USER,
        password=SUPABASE_DB_PASSWORD,
        host=SUPABASE_DB_HOST,
        port=SUPABASE_DB_PORT,
    )
    cur = conn.cursor()

    # Ensure table exists (idempotent safety)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clickup.national_holidays (
            date date PRIMARY KEY,
            name text
        )
        """
    )

    upsert_sql = """
        INSERT INTO clickup.national_holidays (date, name)
        VALUES (%s, %s)
        ON CONFLICT (date) DO UPDATE
        SET name = EXCLUDED.name
    """

    rows = []
    for h in holidays:
        date_str = h.get("date")
        name = h.get("localName") or h.get("name")
        if not date_str or not name:
            continue
        # Validate/normalize date
        try:
            parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        rows.append((parsed, name))

    for row in rows:
        cur.execute(upsert_sql, row)

    conn.commit()
    cur.close()
    conn.close()

    return len(rows)


