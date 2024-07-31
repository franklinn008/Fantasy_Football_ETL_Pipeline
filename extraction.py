import requests
import pandas as pd
from google.cloud import storage
import os

def fetch_fpl_data(url):
    response = requests.get(url)
    return response.json()

def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(data)

def extract_data(service_account_key):
    urls = [
        "https://fantasy.premierleague.com/api/bootstrap-static/",
        "https://fantasy.premierleague.com/api/fixtures/"
    ]

    bucket_name = "fanzysports-bucket"
    
    
    wd = os.getcwd()
    # Define paths
    raw_data_dir = os.path.join(wd, 'rawdata')
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)
    #extract_path = os.path.join(raw_data_dir, 'extracted_data.csv')
    

    #load_path = os.path.join(raw_data_dir, 'final_data.csv')

    # Fetch general data
    general_data = fetch_fpl_data(urls[0])
    events_data = pd.json_normalize(general_data["events"], sep="_")
    elements_data = pd.json_normalize(general_data["elements"], sep="_")
    teams_data = pd.json_normalize(general_data["teams"], sep="_")
    element_type_data = pd.json_normalize(general_data["element_types"], sep="_")

    # Save to CSV
    events_data.to_csv(os.path.join(raw_data_dir,"raw_events.csv"), index=False)
    elements_data.to_csv(os.path.join(raw_data_dir,"raw_elements.csv"), index=False)
    teams_data.to_csv(os.path.join(raw_data_dir,"raw_teams_data.csv"), index=False)
    element_type_data.to_csv(os.path.join(raw_data_dir,"raw_element_type_data.csv"), index=False)

    # Upload to GCS
    upload_to_gcs(bucket_name, "rawfiles/raw_events.csv", os.path.join(raw_data_dir, "raw_events.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_elements.csv", os.path.join(raw_data_dir, "raw_elements.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_teams_data.csv", os.path.join(raw_data_dir, "raw_teams_data.csv"), service_account_key)
    upload_to_gcs(bucket_name, "rawfiles/raw_element_type_data.csv", os.path.join(raw_data_dir, "raw_element_type_data.csv"), service_account_key)

    # Fetch fixtures data
    fixtures_data = fetch_fpl_data(urls[1])
    fixtures_data = pd.json_normalize(fixtures_data, sep="_")
    fixtures_data.to_csv(os.path.join(raw_data_dir,"raw_fixtures_data.csv"), index=False)

    

    # Upload fixtures to GCS
     # Upload fixtures to GCS
    upload_to_gcs(bucket_name, "rawfiles/raw_fixtures_data.csv", os.path.join(raw_data_dir, "raw_fixtures_data.csv"), service_account_key)
