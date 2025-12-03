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

def test_standard_hours_view():
    """Test the new StandardHours view"""
    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB_NAME,
            user=SUPABASE_DB_USER,
            password=SUPABASE_DB_PASSWORD,
            host=SUPABASE_DB_HOST,
            port=SUPABASE_DB_PORT
        )
        cur = conn.cursor()
        
        # Test 1: Check if view exists and has data
        cur.execute('''
            SELECT COUNT(*) as total_records
            FROM public.clickup_fact_StandardHours
        ''')
        total_records = cur.fetchone()[0]
        print(f"✅ StandardHours view has {total_records} records")
        
        # Test 2: Check unique users
        cur.execute('''
            SELECT COUNT(DISTINCT user_id) as unique_users
            FROM public.clickup_fact_StandardHours
        ''')
        unique_users = cur.fetchone()[0]
        print(f"✅ Found {unique_users} unique users")
        
        # Test 3: Check date range
        cur.execute('''
            SELECT 
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM public.clickup_fact_StandardHours
        ''')
        date_range = cur.fetchone()
        print(f"✅ Date range: {date_range[0]} to {date_range[1]}")
        
        # Test 4: Sample data for first user
        cur.execute('''
            SELECT 
                user_id,
                join_date,
                date,
                final_standard_hours,
                is_weekend,
                holiday_name,
                user_status
            FROM public.clickup_fact_StandardHours
            WHERE user_id IS NOT NULL
            ORDER BY user_id, date
            LIMIT 10
        ''')
        sample_data = cur.fetchall()
        print("\n📊 Sample data (first 10 records):")
        for row in sample_data:
            print(f"  User {row[0]}: Joined {row[1]}, Date {row[2]}, Hours {row[3]:.2f}, Weekend: {row[4]}, Holiday: {row[5]}, Status: {row[6]}")
        
        # Test 5: Check for users with partial months
        cur.execute('''
            SELECT 
                user_id,
                join_date,
                COUNT(*) as total_days,
                SUM(final_standard_hours) as total_hours,
                AVG(final_standard_hours) as avg_hours_per_day
            FROM public.clickup_fact_StandardHours
            WHERE EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM join_date) 
                  AND EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM join_date)
            GROUP BY user_id, join_date
            ORDER BY join_date
        ''')
        partial_months = cur.fetchall()
        print(f"\n📅 Users with partial months (join month): {len(partial_months)}")
        for row in partial_months:
            print(f"  User {row[0]}: Joined {row[1]}, Days: {row[2]}, Total Hours: {row[3]:.2f}, Avg/Day: {row[4]:.2f}")
        
        # Test 6: Check standard hours calculation
        cur.execute('''
            SELECT 
                date,
                COUNT(DISTINCT user_id) as active_users,
                SUM(final_standard_hours) as total_standard_hours,
                AVG(final_standard_hours) as avg_standard_hours
            FROM public.clickup_fact_StandardHours
            WHERE final_standard_hours > 0
            GROUP BY date
            ORDER BY date
            LIMIT 5
        ''')
        daily_summary = cur.fetchall()
        print(f"\n📈 Daily summary (first 5 days):")
        for row in daily_summary:
            print(f"  {row[0]}: {row[1]} users, {row[2]:.2f} total hours, {row[3]:.2f} avg hours")
        
        conn.close()
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error testing StandardHours view: {e}")
        raise

if __name__ == '__main__':
    test_standard_hours_view() 