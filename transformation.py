import pandas as pd
from google.cloud import storage
import os

def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(data)

def transform_data(service_account_key):
    wd = os.getcwd()
    transform_path = os.path.join(wd, 'cleandata')
    raw_data_dir = os.path.join(wd, 'rawdata')
    if not os.path.exists(transform_path):
        os.makedirs(transform_path)

    df_elements = pd.read_csv(os.path.join(raw_data_dir,"raw_elements.csv"))
    df_elements = df_elements[["id", "first_name", "second_name", "team", "element_type"]]
    df_elements.to_csv(os.path.join(transform_path,"t_elements.csv"), index=False)

    df_elements_type = pd.read_csv(os.path.join(raw_data_dir,"raw_element_type_data.csv"))
    df_elements_type = df_elements_type[["id", "singular_name_short"]]
    df_elements_type.to_csv(os.path.join(transform_path,"t_elements_type.csv"), index=False)

    df_events = pd.read_csv(os.path.join(raw_data_dir,"raw_events.csv"))
    df_events = df_events[["id", "average_entry_score", "highest_score", "most_selected", "most_transferred_in", "top_element", "transfers_made", "most_captained", "most_vice_captained"]]
    df_events.to_csv(os.path.join(transform_path,"t_events.csv"), index=False)

    df_fixtures = pd.read_csv(os.path.join(raw_data_dir,"raw_fixtures_data.csv"))
    df_fixtures = df_fixtures[["id", "event", "team_a", "team_h", "team_a_score", "team_h_score", "minutes", "team_a_difficulty", "team_h_difficulty"]]
    df_fixtures.to_csv(os.path.join(transform_path,"t_fixtures.csv"), index=False)

    df_teams = pd.read_csv(os.path.join(raw_data_dir,"raw_teams_data.csv"))
    df_teams = df_teams[["id", "name", "short_name"]]
    df_teams.to_csv(os.path.join(transform_path,"t_teams.csv"), index=False)

    bucket_name = "fanzysports-bucket"
    

    # Upload transformed data to GCS
    upload_to_gcs(bucket_name, "transformedfiles/t_events.csv", os.path.join(transform_path,"t_events.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_elements.csv", os.path.join(transform_path,"t_elements.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_teams.csv", os.path.join(transform_path,"t_teams.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_elements_type.csv", os.path.join(transform_path,"t_elements_type.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_fixtures.csv", os.path.join(transform_path,"t_fixtures.csv"), service_account_key)
