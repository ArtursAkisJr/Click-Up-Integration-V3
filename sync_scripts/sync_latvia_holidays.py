import logging
import sys
from datetime import date
from pathlib import Path

import requests

# Ensure project root is on sys.path so we can import supabase_upload
sys.path.append(str(Path(__file__).parent.parent))

from supabase_upload.holidays_upload import upload_latvia_holidays


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NAGER_API_TEMPLATE = "https://date.nager.at/api/v3/PublicHolidays/{year}/LV"


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
    """
    year = target_year or date.today().year

    try:
        holidays = fetch_latvia_holidays(year)
        inserted = upload_latvia_holidays(holidays)
        logger.info(f"Upserted {inserted} Latvia holidays for {year} into Supabase.")
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Failed to sync Latvia holidays for {year}: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()


