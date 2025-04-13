# src/ingestion.py
import requests
import json
import os
from datetime import datetime

# Configuration
USGS_API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
RAW_DATA_DIR = "data/raw"
OUTPUT_FILENAME = f"earthquakes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
OUTPUT_PATH = os.path.join(RAW_DATA_DIR, OUTPUT_FILENAME)

def download_earthquake_data():
    """Downloads earthquake data from USGS and saves it as JSON."""
    print(f"Attempting to download data from: {USGS_API_URL}")
    try:
        response = requests.get(USGS_API_URL, timeout=30) # Added timeout
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        print(f"Successfully downloaded data. Features count: {len(data.get('features', []))}")

        # Ensure the output directory exists
        os.makedirs(RAW_DATA_DIR, exist_ok=True)

        # Save the data
        with open(OUTPUT_PATH, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Raw earthquake data saved to: {OUTPUT_PATH}")
        return OUTPUT_PATH # Return the path for potential chaining

    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

if __name__ == "__main__":
    download_earthquake_data()