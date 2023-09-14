def access_secret_version(secret_id, version_id=1):
    """
    Accesses a secret version from Google Secret Manager.

    Args:
        secret_id (str): The ID of the secret to access.
        version_id (int, optional): The version of the secret to access (default is 1).

    Returns:
        str: The payload data of the secret version as a UTF-8 decoded string.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_NO}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


# get new records added from input file weekly
def get_new_records(source_file_name, timestamp):
    if timestamp!=0:
        timestamp_datetime = datetime.fromtimestamp(timestamp)
    records = []
    storage_client = storage.Client()
    bucket = storage_client.bucket(source_bucket_name)
    blob_name = f"google_forms_data/{source_file_name}"   # input data path
    blob = bucket.blob(blob_name)
    file_content = blob.download_as_string().decode('utf-8')
    reader = csv.reader(file_content.splitlines())
    header = next(reader)
    for row in reader:
        time_datetime = datetime.strptime(row[0], "%d/%m/%Y %H:%M:%S")
        if timestamp == 0 or time_datetime > timestamp_datetime: 
            records.append(row)
    return records





# Get timestamp of last record in the processed file
def get_last_timestamp(target_file_name):
     client = storage.Client()
     bucket = client.bucket(target_bucket_name)
     blob_name = f"google_forms_data/{target_file_name}"
     blob = bucket.blob(blob_name)
     if blob.exists():
          text = blob.download_as_string().decode("utf-8")
          lines = csv.reader(text.splitlines())
          lines = list(lines)
          last_line = lines[-1]
          first_column = last_line[0]
          first_column = datetime.strptime(first_column, "%Y-%m-%d %H:%M:%S").timestamp()
          return first_column
     else:
          return 0






OFFICES = {
    "Manchester": "",
    "Utrecht": "",
    "Edinburgh": "",
    "London": "",
}

# Change destination location to exact office location if location in OFFICES dict
def get_office_address(location):
    return OFFICES[location] if location in OFFICES else location

def append_new_records_to_processed_file(df):
    client = storage.Client()
    bucket = client.bucket(target_bucket_name)
    blob_name = f"google_forms_data/{target_file_name}"  # processed file path
    blob = bucket.blob(blob_name)

    if blob.exists():
        file_content = blob.download_as_string().decode("utf-8")
        print(file_content)
        df_old = pd.read_csv(io.StringIO(file_content))

        updated_df = pd.concat([df_old, df], ignore_index=True)
        updated_csv = updated_df.to_csv(index=False, header=True)
    else:
        updated_csv = df.to_csv(index=False, header=True)
        
    blob.upload_from_string(updated_csv, content_type='text/csv')







def load_to_bq(df):
    """
    Loads a pandas DataFrame into a BigQuery table.

    Args:
        df (pandas DataFrame): The DataFrame containing the data to be loaded into BigQuery.

    Returns:
        pandas DataFrame: The same DataFrame that was passed as input.
    """
    # Read in the data from the file and process it
    df.columns = ['timeststamp', 'email_id', 'travel_date', 'origin', 'destination', 'journey_type', 'mode_travel', 'distance_in_km','time_in_sec','Emissions_in_kgco2e', 'Cost']
    print(df.dtypes)
    df['timeststamp'] = pd.to_datetime(df['timeststamp'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    df['travel_date'] = pd.to_datetime(df['travel_date'], format='%d/%m/%Y', errors='coerce')
    bq_client = bigquery.Client(project=project_id)
    dataset_ref = bq_client.dataset(bq_dataset_name)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "europe-west2"
    try:
        bq_client.get_dataset(dataset)
    except NotFound:
        # If the dataset does not exist, create it
        bq_client.create_dataset(dataset)

    table_path = f"{project_id}.{bq_dataset_name}.{bq_table_name}"
    try:
        table = bq_client.get_table(table_path)
        
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job = bq_client.load_table_from_dataframe(df, table_path, job_config=job_config)
    
    except NotFound:
        # Table does not exist, create the table and load the data
        schema = schema = [
            bigquery.SchemaField("timeststamp", "DATETIME"),
            bigquery.SchemaField("email_id", "STRING"),
            bigquery.SchemaField("travel_date", "DATE"),
            bigquery.SchemaField("origin", "STRING"),
            bigquery.SchemaField("destination", "STRING"),
            bigquery.SchemaField("journey_type", "STRING"),
            bigquery.SchemaField("mode_travel", "STRING"),
            bigquery.SchemaField("distance_in_km", "FLOAT"),
            bigquery.SchemaField("time_in_sec", "FLOAT"),
            bigquery.SchemaField("Emissions_in_kgco2e", "FLOAT"),
            bigquery.SchemaField("Cost", "FLOAT")

        ]

        table = bigquery.Table(table_path, schema=schema)
        table = bq_client.create_table(table)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = schema
        job = bq_client.load_table_from_dataframe(df, table_path, job_config=job_config)
        job.result()
    return df