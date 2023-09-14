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
from load_to_bigquery import load_to_bq, access_secret_version, get_last_timestamp, get_new_records, append_new_records_to_processed_file
import pyarrow

PROJECT_NO = xxxx
project_id = "************"
target_file_name = "xxxxx"
target_bucket_name = "xxxx"
bq_table_name = "xxxx"
bq_dataset_name = "xx"
source_bucket_name = "xxx"
source_file_name = "xxx"

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
