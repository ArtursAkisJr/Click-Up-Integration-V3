import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / 'env_real.env', override=True)

# Supabase API configuration
SUPABASE_URL = "https://vhaxbdqeqbvltgscqobp.supabase.co"
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

def test_different_endpoints():
    """Test different endpoint variations to access clickup schema"""
    
    headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Test different endpoint variations
    endpoints = [
        "/rest/v1/time_entries",
        "/rest/v1/clickup_time_entries", 
        "/rest/v1/clickup.time_entries",
        "/rest/v1/clickup_time_entries?select=*",
        "/rest/v1/time_entries?select=*",
        "/rest/v1/rpc/get_schema_tables",  # Test if we can get schema info
    ]
    
    for endpoint in endpoints:
        print(f"\nTesting endpoint: {endpoint}")
        try:
            url = f"{SUPABASE_URL}{endpoint}"
            response = requests.get(url, headers=headers)
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Success! Found {len(data)} records")
                if len(data) > 0:
                    print(f"Sample record: {list(data[0].keys())}")
            else:
                print(f"Error: {response.text}")
        except Exception as e:
            print(f"Exception: {e}")

def test_schema_discovery():
    """Try to discover what schemas and tables are available"""
    
    headers = {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Try to get schema information
    discovery_endpoints = [
        "/rest/v1/",
        "/rest/v1/rpc/pg_catalog.pg_tables",
        "/rest/v1/rpc/information_schema.tables",
    ]
    
    for endpoint in discovery_endpoints:
        print(f"\nTrying discovery endpoint: {endpoint}")
        try:
            url = f"{SUPABASE_URL}{endpoint}"
            response = requests.get(url, headers=headers)
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Discovery success! Data: {data}")
            else:
                print(f"Discovery failed: {response.text}")
        except Exception as e:
            print(f"Discovery exception: {e}")

def test_with_service_role():
    """Test with service role key which might have more permissions"""
    
    if not SUPABASE_SERVICE_ROLE_KEY:
        print("No service role key available")
        return
    
    headers = {
        'apikey': SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
        'Content-Type': 'application/json'
    }
    
    endpoints = [
        "/rest/v1/time_entries",
        "/rest/v1/clickup_time_entries",
        "/rest/v1/clickup.time_entries",
    ]
    
    for endpoint in endpoints:
        print(f"\nTesting with service role: {endpoint}")
        try:
            url = f"{SUPABASE_URL}{endpoint}"
            response = requests.get(url, headers=headers)
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Service role success! Found {len(data)} records")
            else:
                print(f"Service role failed: {response.text}")
        except Exception as e:
            print(f"Service role exception: {e}")

if __name__ == "__main__":
    print("Testing Supabase API with different approaches...")
    
    if not SUPABASE_ANON_KEY:
        print("ERROR: SUPABASE_ANON_KEY not found!")
        exit(1)
    
    print("\n" + "="*60)
    print("1. Testing different endpoint variations...")
    test_different_endpoints()
    
    print("\n" + "="*60)
    print("2. Testing schema discovery...")
    test_schema_discovery()
    
    print("\n" + "="*60)
    print("3. Testing with service role key...")
    test_with_service_role() 