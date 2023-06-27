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




def main(request):

    csv_data = gsheets_to_csv()
    upload_csv(csv_data)
    #df = pd.read_csv(travel_data_uri, nrows=5)
    Api_key = access_secret_version("maps-api-key")
    timestamp = get_last_timestamp(target_file_name)
    delta_records = get_new_records(source_file_name, timestamp)
    df = pd.DataFrame(delta_records, columns=['timeststamp', 'email_id', 'travel_date', 'origin', 'destination', 'journey_type', 'mode_travel'])
    if df.empty:
        return f"No new records to add. Function exiting"
    else:
        for index, row in df.iterrows():
            df.at[index, 'destination'] = get_office_address(row['destination'])


        #df = df.rename(columns={"Timestamp": "timestamp", "Email address": "email", "When are you travelling?":"date", "Where are you travelling from (this can be your postcode or the town you are coming from)?":"origin","Which office are you travelling to (if not listed please enter the postcode of the office you'll be using)?":"destination","Type of journey":"journeytype","What mode of transport are you using?":"mode_travel"})
        # checking if address is availabe in the local cahe (if it is the we map it directly for the cache) if not we make a list of address that are not availabe then we call the api for just those unique addresses and append these new address to the local cache for future use and map it.
        
        
        new_Hmap = check_local_cache(df['origin'].unique())
        df['origin_co_ordinates'] = df['origin'].map(new_Hmap)    
        new_Hmap = check_local_cache(df['destination'].unique())
        df['destination_co_ordinates'] = df['destination'].map(new_Hmap)
        # calculate the distance
        #df['distance_time'] = pd.Series(get_distance(('51.33505, -0.11029'),('51.5073219, -0.1276474'), 'Train' , Api_key))
        Hmap = load_distance_cache()
        #print(Hmap)
        df['distance_time'] = (df.apply(lambda x: (get_distance(x.origin_co_ordinates,x.destination_co_ordinates, x.mode_travel, Api_key, Hmap) if x.mode_travel != 'Plane' else get_distance_flight(x.origin_co_ordinates, x.destination_co_ordinates)),axis=1))
        #print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@", len(df.index))
        df = df[df.astype(str).ne('None').all(1)]
        #print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@", len(df.index))
        df['distance_in_km'] = df['distance_time'].apply(lambda x: x[0]/1000)
        df['Time_in_sec'] = df['distance_time'].apply(lambda x: x[1])
        df['Emissions_in_kgco2e'] = df.apply(lambda x: get_emissions(x.distance_in_km, x.mode_travel, x.journey_type), axis=1)
        df['Cost'] = df['Emissions_in_kgco2e'].apply(lambda x: cost(x))
        df = df.drop(['distance_time','origin_co_ordinates','destination_co_ordinates'], axis=1)
        #print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@", df)
        load_to_bq(df)
        append_new_records_to_processed_file(df)

        return f"Calculations for new records completed and new transformed data loaded to bigquery. Processed CSV file {target_file_name}loaded to {target_bucket_name} bucket"

        #print(df)
