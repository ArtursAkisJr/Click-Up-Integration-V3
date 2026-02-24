import logging
import os
import sys
from datetime import date
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

# Ensure project root is on sys.path so we can import supabase_upload
sys.path.append(str(Path(__file__).parent.parent))

from supabase_upload.holidays_upload import upload_latvia_holidays

load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NAGER_API_TEMPLATE = "https://date.nager.at/api/v3/PublicHolidays/{year}/LV"


def is_year_already_synced(year: int) -> bool:
    """Return True if at least one holiday row for this year exists in the DB."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('SUPABASE_DB_NAME'),
            user=os.getenv('SUPABASE_DB_USER'),
            password=os.getenv('SUPABASE_DB_PASSWORD'),
            host=os.getenv('SUPABASE_DB_HOST'),
            port=os.getenv('SUPABASE_DB_PORT'),
        )
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM clickup.national_holidays WHERE EXTRACT(YEAR FROM date) = %s",
                (year,)
            )
            count = cur.fetchone()[0]
            cur.close()
            return count > 0
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"Could not check existing holidays for {year}: {e}")
        return False


def fetch_latvia_holidays(year: int) -> list[dict]:
    url = NAGER_API_TEMPLATE.format(year=year)
    logger.info(f"Fetching Latvia public holidays for {year} from {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    holidays = resp.json()
    logger.info(f"Fetched {len(holidays)} holidays for {year}")
    return holidays


def main(target_year: int | None = None) -> None:
    """
    Sync Latvia public holidays for the given year into Supabase.

    If target_year is None, uses the current calendar year.
    Past years that are already fully synced are skipped — holidays don't change.
    The current year is always re-synced in case new holidays were added.
    """
    year = target_year or date.today().year
    current_year = date.today().year

    if year < current_year and is_year_already_synced(year):
        logger.info(f"Holidays for {year} already in DB and year is complete — skipping fetch.")
        return

    try:
        holidays = fetch_latvia_holidays(year)
        inserted = upload_latvia_holidays(holidays)
        logger.info(f"Upserted {inserted} Latvia holidays for {year} into Supabase.")
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Failed to sync Latvia holidays for {year}: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()


