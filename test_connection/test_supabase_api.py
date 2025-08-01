import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

# Supabase API configuration
SUPABASE_URL = "https://vhaxbdqeqbvltgscqobp.supabase.co"  # Extract from your project URL
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')  # You'll need to add this to your .env file
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')  # You'll need to add this to your .env file

def test_supabase_api_connection():
    """Test basic connection to Supabase API"""
    try:
        headers = {
            'apikey': SUPABASE_ANON_KEY,
            'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Test endpoint - use clickup schema
        url = f"{SUPABASE_URL}/rest/v1/clickup_time_entries"
        
        response = requests.get(url, headers=headers)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully connected to Supabase API!")
            print(f"Retrieved {len(data)} records")
            return data
        else:
            print(f"Failed to connect: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error testing Supabase API connection: {e}")
        return None

def query_time_entries_via_api():
    """Query time entries using Supabase REST API"""
    try:
        headers = {
            'apikey': SUPABASE_ANON_KEY,
            'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
            'Content-Type': 'application/json',
            'Prefer': 'count=exact'  # Get total count
        }
        
        # Query parameters
        params = {
            'select': 'timesheet_id,clickup_time_entry_id,user_id,task_id,task_name,duration_hours,start_datetime,end_datetime,task_status',
            'order': 'timesheet_id.desc',
            'limit': '100'
        }
        
        url = f"{SUPABASE_URL}/rest/v1/clickup_time_entries"
        
        response = requests.get(url, headers=headers, params=params)
        
        print(f"API Request Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully retrieved {len(data)} time entries via API")
            
            # Display first few entries
            for i, entry in enumerate(data[:5]):
                print(f"\nEntry {i+1}:")
                print(f"  ID: {entry.get('timesheet_id')}")
                print(f"  Task: {entry.get('task_name')}")
                print(f"  User ID: {entry.get('user_id')}")
                print(f"  Duration: {entry.get('duration_hours')} hours")
                print(f"  Start: {entry.get('start_datetime')}")
                print(f"  Status: {entry.get('task_status')}")
            
            # Save to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = Path(__file__).parent / f'time_entries_api_{timestamp}.json'
            
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            
            print(f"\nAll entries saved to: {output_file}")
            return data
        else:
            print(f"API request failed: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error querying time entries via API: {e}")
        return None

def query_specific_task_via_api(task_name):
    """Query specific task using Supabase REST API"""
    try:
        headers = {
            'apikey': SUPABASE_ANON_KEY,
            'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Query parameters with filter
        params = {
            'select': 'timesheet_id,clickup_time_entry_id,user_id,task_id,task_name,duration_hours,start_datetime,end_datetime,task_status',
            'task_name': f'ilike.*{task_name}*',
            'order': 'start_datetime.desc'
        }
        
        url = f"{SUPABASE_URL}/rest/v1/clickup_time_entries"
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"\nFound {len(data)} entries for task containing '{task_name}' via API:")
            
            for entry in data:
                print(f"  ID: {entry.get('timesheet_id')}, User: {entry.get('user_id')}, Duration: {entry.get('duration_hours')}h, Start: {entry.get('start_datetime')}")
            
            return data
        else:
            print(f"API request failed: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error querying specific task via API: {e}")
        return None

def test_rpc_function():
    """Test calling a PostgreSQL function via Supabase API"""
    try:
        headers = {
            'apikey': SUPABASE_ANON_KEY,
            'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Test calling a simple function (if you have one)
        url = f"{SUPABASE_URL}/rest/v1/rpc/get_time_entries_count"
        
        response = requests.post(url, headers=headers, json={})
        
        print(f"RPC Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"RPC Response: {data}")
        else:
            print(f"RPC failed: {response.text}")
            
    except Exception as e:
        print(f"Error testing RPC: {e}")

if __name__ == "__main__":
    print("Testing Supabase API connection...")
    
    # Check if API keys are available
    if not SUPABASE_ANON_KEY:
        print("ERROR: SUPABASE_ANON_KEY not found in environment variables!")
        print("Please add your Supabase anon key to env_real.env")
        print("You can find this in your Supabase project settings under API")
        exit(1)
    
    # Test basic connection
    test_supabase_api_connection()
    
    print("\n" + "="*80)
    print("Querying time entries via API...")
    all_entries = query_time_entries_via_api()
    
    if all_entries:
        print("\n" + "="*80)
        print("Querying specific task 'Setup Timesheet for Ieva' via API...")
        specific_entries = query_specific_task_via_api("Setup Timesheet for Ieva")
    
    print("\n" + "="*80)
    print("Testing RPC function...")
    test_rpc_function() 