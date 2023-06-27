import gspread
import google.auth
from google.cloud import storage
import requests

SHEET_NAME = "Travel form - Carbon Offsetting (Responses)"
BUCKET_NAME = "eco-project-bucket-raw"
DESTINATION_FILE_NAME = "google_form_data"

def gsheets_to_csv():
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets.readonly", "https://www.googleapis.com/auth/drive"
        ]
    )
    gspread_client = gspread.Client(auth=credentials)
    gsheet = gspread_client.open(SHEET_NAME)
    worksheet = gsheet.get_worksheet(0)
    all_records = worksheet.get_all_records()
    keys = all_records[0].keys()
    csv_data = ",".join(keys) + "\n"

    # Construct the CSV data
    for record in all_records:
        values = [str(record[key]).replace(",", " ") for key in keys]
        csv_data += ",".join(values) + "\n"

    return csv_data

def upload_csv(source_data):
    """Uploads a file to the bucket."""
    blob_name = f"google_forms_data/{DESTINATION_FILE_NAME}.csv"
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(source_data, content_type='text/csv')