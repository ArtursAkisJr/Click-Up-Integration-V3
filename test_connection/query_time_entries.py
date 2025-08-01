import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import json
from datetime import datetime

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

SUPABASE_DB_USER = os.getenv('SUPABASE_DB_USER')
SUPABASE_DB_PASSWORD = os.getenv('SUPABASE_DB_PASSWORD')
SUPABASE_DB_HOST = os.getenv('SUPABASE_DB_HOST')
SUPABASE_DB_PORT = os.getenv('SUPABASE_DB_PORT')
SUPABASE_DB_NAME = os.getenv('SUPABASE_DB_NAME')

def query_time_entries():
    """Query all entries from clickup.time_entries table"""
    try:
        # Connect to Supabase database
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        print('Successfully connected to Supabase database!')
        
        # Create cursor
        cur = conn.cursor()
        
        # Query all time entries
        query = """
        SELECT 
            timesheet_id,
            clickup_time_entry_id,
            user_id,
            task_id,
            task_name,
            billable,
            start_time,
            end_time,
            duration,
            duration_hours,
            start_datetime,
            end_datetime,
            description,
            source,
            at,
            is_locked,
            approval_id,
            task_url,
            task_status,
            task_status_type,
            task_status_color,
            task_status_orderindex,
            task_custom_type,
            list_id,
            folder_id,
            space_id,
            wid,
            tags,
            sync_status_id,
            sync_status_timestamp
        FROM clickup.time_entries
        ORDER BY timesheet_id DESC
        LIMIT 100
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        # Get column names
        column_names = [desc[0] for desc in cur.description]
        
        print(f"Found {len(rows)} time entries (showing latest 100):")
        print("-" * 80)
        
        # Convert rows to list of dictionaries for easier viewing
        entries = []
        for row in rows:
            entry = dict(zip(column_names, row))
            entries.append(entry)
            
            # Print first few entries in a readable format
            if len(entries) <= 5:
                print(f"Entry {entry['timesheet_id']}:")
                print(f"  Task: {entry['task_name']}")
                print(f"  User ID: {entry['user_id']}")
                print(f"  Duration: {entry['duration_hours']} hours")
                print(f"  Start: {entry['start_datetime']}")
                print(f"  End: {entry['end_datetime']}")
                print(f"  Status: {entry['task_status']}")
                print("-" * 40)
        
        # Save all entries to JSON file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = Path(__file__).parent / f'time_entries_query_{timestamp}.json'
        
        with open(output_file, 'w') as f:
            json.dump(entries, f, indent=2, default=str)
        
        print(f"\nAll {len(entries)} entries saved to: {output_file}")
        
        # Close cursor and connection
        cur.close()
        conn.close()
        
        return entries
        
    except Exception as e:
        print('Failed to query Supabase database:')
        print(e)
        return None

def query_specific_task(task_name):
    """Query time entries for a specific task name"""
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        
        cur = conn.cursor()
        
        query = """
        SELECT 
            timesheet_id,
            clickup_time_entry_id,
            user_id,
            task_id,
            task_name,
            duration_hours,
            start_datetime,
            end_datetime,
            task_status
        FROM clickup.time_entries
        WHERE task_name ILIKE %s
        ORDER BY start_datetime DESC
        """
        
        cur.execute(query, (f'%{task_name}%',))
        rows = cur.fetchall()
        
        column_names = [desc[0] for desc in cur.description]
        entries = [dict(zip(column_names, row)) for row in rows]
        
        print(f"\nFound {len(entries)} entries for task containing '{task_name}':")
        for entry in entries:
            print(f"  ID: {entry['timesheet_id']}, User: {entry['user_id']}, Duration: {entry['duration_hours']}h, Start: {entry['start_datetime']}")
        
        cur.close()
        conn.close()
        
        return entries
        
    except Exception as e:
        print(f'Failed to query task "{task_name}":')
        print(e)
        return None

if __name__ == "__main__":
    print("Querying all time entries from Supabase...")
    all_entries = query_time_entries()
    
    if all_entries:
        print("\n" + "="*80)
        print("Now querying specific task 'Setup Timesheet for Ieva'...")
        specific_entries = query_specific_task("Setup Timesheet for Ieva") 