import pandas as pd
from pathlib import Path
import requests
from requests.exceptions import RequestException
import time
import os


def fetch_and_process_stations(api_key, client_id, categories="1-2", max_retries=5):
    # API configuration
    base_url = "https://apis.deutschebahn.com/db-api-marketplace/apis/station-data/v2/stations"
    headers = {
        "DB-Api-Key": api_key,
        "DB-Client-Id": client_id,
        "accept": "application/json"
    }
    
    # Add query parameters
    params = {"category": categories}
    
    # Attempt API request with retries
    for attempt in range(max_retries):
        try:
            response = requests.get(
                base_url,
                headers=headers,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            
            if attempt > 0:
                print(f"Success after {attempt} attempts.")
            
            # Process the JSON response
            json_data = response.json()
            
            # Process station data
            stations = []
            for station in json_data['result']:
                evas = []
                longitude = None
                latitude = None
                
                for eva in station['evaNumbers']:
                    evas.append(f"0{eva.get('number')}")  # add a leading 0 for the eva
                    if eva.get('isMain'):
                        coords = eva['geographicCoordinates']['coordinates']
                        longitude = coords[0]
                        latitude = coords[1]
                
                station_data = {
                    'name': station.get('name'),
                    'category': station.get('category'),
                    'evas': ",".join(evas),
                    'longitude': longitude,
                    'latitude': latitude,
                }
                stations.append(station_data)
            
            # Create and return DataFrame
            df = pd.DataFrame(stations)
            return df.sort_values('name', ascending=True)
            
        except (RequestException, ConnectionError) as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print("Retrying in 2 seconds...")
                time.sleep(2)
            else:
                print(f"Failed to fetch data after {max_retries} attempts")
                return None
        
        finally:
            time.sleep(1 / 60)  # Rate limiting
    
    return None

if __name__ == "__main__":
    # Get API credentials from environment variables
    api_key = os.getenv("API_KEY")
    client_id = os.getenv("CLIENT_ID")
    
    if not api_key or not client_id:
        print("Error: API_KEY and CLIENT_ID environment variables must be set")
        exit(1)
    
    # Fetch and process the data
    df = fetch_and_process_stations(api_key, client_id)
    
    if df is not None:
        df.to_csv(Path("monthly_data_releases") / 'current_eva_list.csv', index=False, quoting=2, quotechar='"')
        print(f"Successfully processed {len(df)} stations")
    else:
        print("Failed to fetch and process station data")