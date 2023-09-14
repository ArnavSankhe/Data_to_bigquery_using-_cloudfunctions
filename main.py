import numpy as np
import pandas as pd
from function_to_get_co_ordinates import check_local_cache
from get_distance import get_distance_flight,get_distance
from emissions_cost import get_emissions, cost
from gsheets_to_csv import gsheets_to_csv, upload_csv
from datetime import datetime
from google.cloud import storage
import io
from google.cloud import bigquery
import csv
from google.cloud import secretmanager
from get_distance import load_distance_cache
from google.api_core.exceptions import NotFound
from load_to_bigquery import load_to_bq
import pyarrow



#travel_data_uri = "/home/ar    nav/Desktop/git/test/Travel.csv"
PROJECT_NO = 431555079587
project_id = "************"
target_file_name = "google_form_data_processed.csv"
target_bucket_name = "eco-project-bucket-processed"
bq_table_name = "google_forms_emissions"
bq_dataset_name = "eco_project"
source_bucket_name = "eco-project-bucket-raw"
source_file_name = "google_form_data.csv"



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










def main(request):
    """
    This function performs calculations for new records and loads the transformed data to BigQuery.
    It also appends the processed CSV file to the target bucket.
    """
    csv_data = gsheets_to_csv()
    upload_csv(csv_data)
    Api_key = access_secret_version("maps-api-key")
    timestamp = get_last_timestamp(target_file_name)
    delta_records = get_new_records(source_file_name, timestamp)
    df = pd.DataFrame(delta_records, columns=['timeststamp', 'email_id', 'travel_date', 'origin', 'destination', 'journey_type', 'mode_travel'])
    
    if df.empty:
        return f"No new records to add. Function exiting"
    else:
        df['destination'] = df['destination'].apply(get_office_address)
        new_Hmap = check_local_cache(df['origin'].unique())
        df['origin_co_ordinates'] = df['origin'].map(new_Hmap)    
        new_Hmap = check_local_cache(df['destination'].unique())
        df['destination_co_ordinates'] = df['destination'].map(new_Hmap)
        Hmap = load_distance_cache()
        df['distance_time'] = np.where(df['mode_travel'] != 'Plane', df.apply(lambda x: get_distance(x.origin_co_ordinates, x.destination_co_ordinates, x.mode_travel, Api_key, Hmap), axis=1), df.apply(lambda x: get_distance_flight(x.origin_co_ordinates, x.destination_co_ordinates), axis=1))
        df = df[df.astype(str).ne('None').all(1)]
        df['distance_in_km'] = df['distance_time'].str[0] / 1000
        df['Time_in_sec'] = df['distance_time'].str[1]
        df['Emissions_in_kgco2e'] = df.apply(lambda x: get_emissions(x.distance_in_km, x.mode_travel, x.journey_type), axis=1)
        df['Cost'] = df['Emissions_in_kgco2e'].apply(cost)
        df = df.drop(columns=['distance_time','origin_co_ordinates','destination_co_ordinates'])
        load_to_bq(df)
        append_new_records_to_processed_file(df)

        return f"Calculations for new records completed and new transformed data loaded to bigquery. Processed CSV file {target_file_name}loaded to {target_bucket_name} bucket"
        #print(df)
