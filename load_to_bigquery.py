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