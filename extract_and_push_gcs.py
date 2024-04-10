import requests
import csv
from google.cloud import storage
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
import os

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/pankajgurbani/Desktop/cricket_gcp/test-project-cricket-97d5ed55a2d7.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/airflow/gcs/dags/scripts/test-project-cricket-97d5ed55a2d7.json'
url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'
headers = {
    "X-RapidAPI-Key": "561c88d80amsh76591ecc7799153p1f2c4bjsnf35061b882e8",
	"X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}
params = {
    'formatType': 'odi'
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('rank', [])  # Extracting the 'rank' data
    csv_filename = 'batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']  # Specify required field names

        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            # writer.writeheader()
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")

        # Upload the CSV file to GCS
        # storage_client = storage.Client(credentials=google_credentials)
        bucket_name = 'cricket-project'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS
        csv_file_loc = '/Users/pankajgurbani/Desktop/cricket_gcp/batsmen_rankings.csv'
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
    else:
        print("No data available from the API.")
else:
    print("Failed to fetch data:", response.status_code)