import pandas as pd
from google.cloud import storage
import os
import tempfile

def upload_to_gcs(bucket_name, blob_name, data, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(data)

def download_from_gcs(bucket_name, blob_name, service_account_key):
    client = storage.Client.from_service_account_json(service_account_key)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    blob.download_to_filename(temp_file.name)
    return temp_file.name

def transform_data(service_account_key):
    bucket_name = "fanzysports-bucket"
    raw_files = {
        "raw_elements.csv": "rawfiles/raw_elements.csv",
        "raw_element_type_data.csv": "rawfiles/raw_element_type_data.csv",
        "raw_events.csv": "rawfiles/raw_events.csv",
        "raw_fixtures_data.csv": "rawfiles/raw_fixtures_data.csv",
        "raw_teams_data.csv": "rawfiles/raw_teams_data.csv"
    }

    wd = os.getcwd()
    print(f"Current working directory: {wd}") 

    transform_path = os.path.join(wd, 'cleandata')
    if not os.path.exists(transform_path):
        os.makedirs(transform_path)
    print(f"Transform data directory: {transform_path}")

    temp_files = []
    
    # Download files from GCS and transform them
    for local_file, gcs_path in raw_files.items():
        temp_file = download_from_gcs(bucket_name, gcs_path, service_account_key)
        temp_files.append(temp_file)
        
        if local_file == "raw_elements.csv":
            df_elements = pd.read_csv(temp_file)
            df_elements = df_elements[["id", "first_name", "second_name", "team", "element_type"]]
            df_elements.to_csv(os.path.join(transform_path, "t_elements.csv"), index=False)

        elif local_file == "raw_element_type_data.csv":
            df_elements_type = pd.read_csv(temp_file)
            df_elements_type = df_elements_type[["id", "singular_name_short"]]
            df_elements_type.to_csv(os.path.join(transform_path, "t_elements_type.csv"), index=False)

        elif local_file == "raw_events.csv":
            df_events = pd.read_csv(temp_file)
            df_events = df_events[["id", "average_entry_score", "highest_score", "most_selected", "most_transferred_in", "top_element", "transfers_made", "most_captained", "most_vice_captained"]]
            df_events.to_csv(os.path.join(transform_path, "t_events.csv"), index=False)

        elif local_file == "raw_fixtures_data.csv":
            df_fixtures = pd.read_csv(temp_file)
            df_fixtures = df_fixtures[["id", "event", "team_a", "team_h", "team_a_score", "team_h_score", "minutes", "team_a_difficulty", "team_h_difficulty"]]
            df_fixtures.to_csv(os.path.join(transform_path, "t_fixtures.csv"), index=False)

        elif local_file == "raw_teams_data.csv":
            df_teams = pd.read_csv(temp_file)
            df_teams = df_teams[["id", "name", "short_name"]]
            df_teams.to_csv(os.path.join(transform_path, "t_teams.csv"), index=False)

    # Upload transformed data to GCS
    upload_to_gcs(bucket_name, "transformedfiles/t_events.csv", os.path.join(transform_path, "t_events.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_elements.csv", os.path.join(transform_path, "t_elements.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_teams.csv", os.path.join(transform_path, "t_teams.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_elements_type.csv", os.path.join(transform_path, "t_elements_type.csv"), service_account_key)
    upload_to_gcs(bucket_name, "transformedfiles/t_fixtures.csv", os.path.join(transform_path, "t_fixtures.csv"), service_account_key)

    # Clean up the temporary files after uploads are complete
    for temp_file in temp_files:
        try:
            os.remove(temp_file)
            print(f"Successfully removed temporary file {temp_file}")
        except PermissionError as e:
            print(f"Could not remove temporary file {temp_file}: {e}")

# Example usage
#transform_data('dags\\etl\\service_account_key.json')
