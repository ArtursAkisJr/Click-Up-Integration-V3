import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')

def cleanup_duplicate_time_entries():
    """Remove duplicate time entries keeping only the most recent one for each clickup_time_entry_id"""
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        cur = conn.cursor()
        
        # First, let's see how many duplicates we have
        cur.execute('''
            SELECT clickup_time_entry_id, COUNT(*) as count
            FROM clickup.time_entries
            WHERE clickup_time_entry_id IS NOT NULL
            GROUP BY clickup_time_entry_id
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        ''')
        
        duplicates = cur.fetchall()
        logger.info(f"Found {len(duplicates)} duplicate clickup_time_entry_ids")
        
        if duplicates:
            logger.info("Duplicate counts:")
            for entry_id, count in duplicates[:10]:  # Show first 10
                logger.info(f"  {entry_id}: {count} occurrences")
        
        # Remove duplicates keeping the most recent entry (highest timesheet_id)
        cur.execute('''
            DELETE FROM clickup.time_entries
            WHERE timesheet_id NOT IN (
                SELECT MAX(timesheet_id)
                FROM clickup.time_entries
                WHERE clickup_time_entry_id IS NOT NULL
                GROUP BY clickup_time_entry_id
            )
            AND clickup_time_entry_id IS NOT NULL
        ''')
        
        deleted_count = cur.rowcount
        logger.info(f"Removed {deleted_count} duplicate time entries")
        
        # Verify the cleanup
        cur.execute('''
            SELECT clickup_time_entry_id, COUNT(*) as count
            FROM clickup.time_entries
            WHERE clickup_time_entry_id IS NOT NULL
            GROUP BY clickup_time_entry_id
            HAVING COUNT(*) > 1
        ''')
        
        remaining_duplicates = cur.fetchall()
        if remaining_duplicates:
            logger.warning(f"Still have {len(remaining_duplicates)} duplicate clickup_time_entry_ids after cleanup")
        else:
            logger.info("All duplicates have been successfully removed")
        
        conn.commit()
        cur.close()
        conn.close()
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error cleaning up duplicate time entries: {e}")
        raise

def add_unique_constraint():
    """Add UNIQUE constraint to clickup_time_entry_id if it doesn't exist"""
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        cur = conn.cursor()
        
        # Check if constraint already exists
        cur.execute('''
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'clickup'
            AND table_name = 'time_entries'
            AND constraint_type = 'UNIQUE'
            AND constraint_name LIKE '%clickup_time_entry_id%'
        ''')
        
        existing_constraint = cur.fetchone()
        
        if not existing_constraint:
            # Add UNIQUE constraint
            cur.execute('''
                ALTER TABLE clickup.time_entries
                ADD CONSTRAINT time_entries_clickup_time_entry_id_unique
                UNIQUE (clickup_time_entry_id)
            ''')
            logger.info("Added UNIQUE constraint to clickup_time_entry_id")
        else:
            logger.info("UNIQUE constraint already exists on clickup_time_entry_id")
        
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error adding unique constraint: {e}")
        raise

if __name__ == '__main__':
    logger.info("Starting cleanup of duplicate time entries...")
    
    # First clean up existing duplicates
    deleted_count = cleanup_duplicate_time_entries()
    
    # Then ensure we have the unique constraint
    add_unique_constraint()
    
    logger.info("Cleanup completed successfully!") 