import os
import time
import requests
from xml.dom.minidom import parseString

import pdfplumber
import pandas as pd

# Retrieve the secret API key from the environment variable
api_key = os.getenv('API_KEY')
if not api_key:
    raise ValueError("No API Key provided!")

client_id = os.getenv('CLIENT_ID')
if not client_id:
    raise ValueError("No Client Id provided!")

headers = {
    'DB-Api-Key': api_key,
    'DB-Client-Id': client_id,
    'accept': 'application/xml'
}

def extract_tables_to_df(pdf_path):
    all_tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                df = pd.DataFrame(table[1:], columns=table[0])
                all_tables.append(df)
    
    combined_df = pd.concat(all_tables, ignore_index=True)
    combined_df['Bahnhof ohne Leerzeichen'] = combined_df['Bahnhof'].str.replace(' (', '(', regex=False)
    return combined_df

# # Example usage
print("getting 'Bahnhof' names")
df = extract_tables_to_df('Stationspreisliste-2024-data.pdf')
print("getting the eva for each 'Bahnhof' name")

def get_eva(bahnhof_ohne_leerzeichen):
    formatted_url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/station/{bahnhof_ohne_leerzeichen}"
    
    response = requests.get(formatted_url, headers=headers)
    time.sleep(1/60) # because the can be a maximum of 60 requests per minute
    if response.status_code != 200:
        print(f"error {response.status_code}: {formatted_url}")
        return

    print(response.text)
    #print(parseString(response.content).toprettyxml())

get_eva(df['Bahnhof ohne Leerzeichen'][10])