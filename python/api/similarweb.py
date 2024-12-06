import requests
import re
import json
import pandas as pd
import pandas_gbq
import time
from google.cloud import bigquery
from google.oauth2 import service_account

class SimilarWebData:
    def __init__(self, api_key, bq_credentials_file, start_date, end_date):
        self.api_key = api_key
        self.url_base = 'https://api.similarweb.com/v1/website/'
        self.url_metric = '/total-traffic-and-engagement/visits?'
        self.start_date = start_date
        self.end_date = end_date
        self.granularity = 'daily'
        self.bq_client = bigquery.Client.from_service_account_json(bq_credentials_file)
        self.credentials = service_account.Credentials.from_service_account_file(bq_credentials_file)
        
    def clean_column(self, text):
        import unicodedata
        text = re.sub('/|en|[()]|\.+|& ', '_', text)
        text = text.replace(" ", "_").replace("&", "").replace("-", "_").replace("+", "")
        return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    
    def fetch_data(self, domain_list, country, mtd='false'):
        data_frames = []
        for domain in domain_list:
            url = f"{self.url_base}{domain}{self.url_metric}api_key={self.api_key}&start_date={self.start_date}&end_date={self.end_date}&country={country}&granularity={self.granularity}&main_domain_only=false&format=json&show_verified=false&mtd={mtd}"
            response = requests.get(url)
            try:
                presence = pd.DataFrame(response.json()['visits'])
                presence.rename(columns={'visits': domain, 'date': f'date_{domain}'}, inplace=True)
                data_frames.append(presence)
                time.sleep(1)  # To avoid rate limiting
            except KeyError:
                print(f"Error fetching data for {domain}: {response.json()}")
                return response.json()
        data = pd.concat(data_frames, axis=1)
        return data

    def clean_data(self, data, country, rename_dict):
        data['country'] = country
        data.rename(columns=rename_dict, inplace=True)
        for col in data.columns:
            if 'date' in col:
                del data[col]
        return data
    
    def melt_data(self, data, country_col="country", timestamp_col="event_timestamp", var_name="competitors", value_name="sessions"):
        return pd.melt(data, id_vars=[timestamp_col, country_col], var_name=var_name, value_name=value_name)
    
    def process_and_upload(self, domain_list, country_code, table_name, rename_dict, mtd='false'):
        data = self.fetch_data(domain_list, country_code, mtd)
        
        # Clean column names
        data.columns = [self.clean_column(col) for col in data.columns]
        
        # Clean data
        data = self.clean_data(data, country_code, rename_dict)
        
        # Reshape (melt) data for uploading
        data_melted = self.melt_data(data)

        # Upload to BigQuery
        self.upload_to_bq(data_melted, table_name)

    def upload_to_bq(self, data, table_name):
        pandas_gbq.to_gbq(
            data,
            table_name,
            if_exists='append',
            credentials=self.credentials
        )
        print(f"Uploaded data to BigQuery table: {table_name}")

similar_web_api = 'YOUR_API_KEY'  # Replace with your SimilarWeb API key
bq_credentials_file = 'path/to/your/key.json'  # Replace with your Google Cloud BigQuery credentials file
start_date = '2023-01-01'  # Set your desired start date
end_date = '2024-02-13'  # Set your desired end date

# Initialize the SimilarWebData class
sw_data = SimilarWebData(similar_web_api, bq_credentials_file, start_date, end_date)

# Define domain lists and country codes for processing
domain_fr = ["sephora.fr", "nocibe.fr", "marionnaud.fr", "notino.fr", "my-origines.com"]

# Define column rename mappings
rename_fr = {'date_sephora_fr': 'event_timestamp'}

# Process and (optionally) upload data for the specified region
sw_data.process_and_upload(domain_fr, 'FR', 'your_project.your_dataset.your_table', rename_fr)

data = sw_data.fetch_data(domain_fr, 'FR')
