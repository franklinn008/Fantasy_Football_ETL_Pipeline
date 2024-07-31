from google.cloud import bigquery

def load_csv_to_bigquery(dataset_id, table_id, csv_file, service_account_key):
    client = bigquery.Client.from_service_account_json(service_account_key)
    uri = f"gs://{csv_file}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )
    table_ref = client.dataset(dataset_id).table(table_id)
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )
    load_job.result()
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_id}")

def load_data(service_account_key):
    files = [
        {"filepath": "fanzysports-bucket/transformedfiles/t_fixtures.csv", "table_id": "fixtures"},
        {"filepath": "fanzysports-bucket/transformedfiles/t_elements.csv", "table_id": "elements"},
        {"filepath": "fanzysports-bucket/transformedfiles/t_elements_type.csv", "table_id": "elements_type"},
        {"filepath": "fanzysports-bucket/transformedfiles/t_events.csv", "table_id": "events"},
        {"filepath": "fanzysports-bucket/transformedfiles/t_teams.csv", "table_id": "teams"}
    ]

    for file in files:
        load_csv_to_bigquery("abc_STG", file['table_id'], file['filepath'], service_account_key)
