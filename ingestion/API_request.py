import requests
import json
import time
import hashlib
import pickle
import os

CACHE_FILE = "api_cache.pkl"  # File to store cached API responses
CACHE_EXPIRY = 28800  # Expiry time in seconds (8 hours)

# Load existing cache if available
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "rb") as f:
        api_cache = pickle.load(f)
else:
    api_cache = {}

# API rate limits
BURST_LIMIT = 4  
SUSTAINED_LIMIT = 500  

requests_made = 0
start_time = time.time()

def rate_limiter():
    """Enforce API rate limits"""
    global requests_made, start_time

    if requests_made >= BURST_LIMIT:
        time.sleep(1)  # Wait 1 sec before making more requests
        requests_made = 0  

    elapsed_time = time.time() - start_time
    if requests_made >= SUSTAINED_LIMIT / 60:  
        time.sleep(8)  # Pause for 8 seconds

    requests_made += 1

def get_api_data(url):
    """Fetch API data with persistent caching"""
    cache_key = hashlib.md5(url.encode()).hexdigest()

    # Check if cached and still valid
    if cache_key in api_cache:
        cached_entry = api_cache[cache_key]
        if time.time() - cached_entry["timestamp"] < CACHE_EXPIRY:
            print(f"Using cached data for {url}")
            return cached_entry["data"]
    
    # Apply rate limiting
    rate_limiter()

    # Fetch from API
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch {url}, status: {response.status_code}")
        return None

    # Store response in cache
    api_cache[cache_key] = {
        "data": response.json(),
        "timestamp": time.time(),
    }

    # Save cache to file
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(api_cache, f)

    return api_cache[cache_key]["data"]

# API Base URL
API_BASE_URL = "http://api.jolpi.ca/ergast/f1"

# Define API endpoints
TABLES = {
    "circuits": f"{API_BASE_URL}/circuits",
    "constructors": f"{API_BASE_URL}/constructors",
    "drivers": f"{API_BASE_URL}/drivers",
    "races": f"{API_BASE_URL}/races",
    "results": f"{API_BASE_URL}/results"
    
}

# Pagination settings
LIMIT = 100  # Adjust based on API limits

def fetch_and_store(table_name, base_url):
    """Fetch paginated API data and write it incrementally to a JSON file"""
    print(f"Fetching data for {table_name}...")

    offset = 0
    output_file = f"{table_name}_cleaned.jsonl"  # JSON Lines format for efficient appending

    while True:
        api_url = f"{base_url}?limit={LIMIT}&offset={offset}"
        data = get_api_data(api_url)

        if not data:
            print(f"Error or empty response for {table_name}, stopping pagination.")
            break

        # Extract data from JSON response
        if table_name == "circuits":
            records = data.get("MRData", {}).get("CircuitTable", {}).get("Circuits", [])
        elif table_name == "constructors":
            records = data.get("MRData", {}).get("ConstructorTable", {}).get("Constructors", [])
        elif table_name == "drivers":
            records = data.get("MRData", {}).get("DriverTable", {}).get("Drivers", [])
        elif table_name == "races":
            records = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        elif table_name == "results":
            records = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
         
        else:
            records = []
        
        if not records:
            print(f"No more data found for {table_name}. Stopping pagination.")
            break        

        # Append each record as a separate JSON object in a new line
        with open(output_file, "a", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        offset += LIMIT  # Move to the next batch
        print(f"Fetched {len(records)} records for {table_name}, total offset: {offset}")

        # Optional delay to avoid rate limits
        time.sleep(8)

    print(f"Data for {table_name} saved incrementally to {output_file}")