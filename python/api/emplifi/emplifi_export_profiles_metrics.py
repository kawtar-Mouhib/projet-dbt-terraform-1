import requests
import base64
import pandas as pd
import datetime
import json
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq

class EmplifiMetricsFetcher:
    def __init__(self, base_url, api_key, credentials_file, project_id):
        self.base_url = base_url
        self.header = {
            'Authorization': f"Basic {base64.b64encode(api_key.encode()).decode()}"
        }
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(credentials=self.credentials, project=project_id)
        self.project_id = project_id

    def get_profiles(self, network):
        """Fetches profile IDs for a given network."""
        api_endpoint = f"/3/{network}/profiles"
        request_url = self.base_url + api_endpoint
        
        response = requests.get(request_url, headers=self.header)
        if response.status_code == 200:
            print(f"Successfully retrieved profiles for {network}")
            return [profile['id'] for profile in response.json().get('profiles', [])]
        else:
            print(f"Failed to retrieve profiles for {network}, status code: {response.status_code}")
            return []

    def delete_past_month_data(self, destination_table):
        """Deletes data from the past month in BigQuery."""
        one_month_ago = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
        query = f"""
        DELETE FROM `{destination_table}`
        WHERE date >= '{one_month_ago}'
        """
        query_job = self.client.query(query)
        query_job.result()  # Wait for the job to complete
        print(f"Deleted data from {destination_table} where date >= '{one_month_ago}'")

    def fetch_metrics(self, network_profiles, network_metrics):
        """Fetches metrics for the past month for each network's profiles."""
        today = datetime.date.today().strftime("%Y-%m-%d")
        one_month_ago = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
        df_all_metrics = pd.DataFrame()

        for network, profiles in network_profiles.items():
            print(f"Processing network: {network}")
            metrics = network_metrics.get(network, [])
            api_endpoint = f"/3/{network}/metrics"
            request_url = self.base_url + api_endpoint

            payload = {
                "date_start": one_month_ago, 
                "date_end": today,
                "profiles": profiles,
                "metrics": metrics,
                "dimensions": [{"type": "date.day"}, {"type": "profile"}]
            }
            
            response = requests.post(request_url, headers=self.header, json=payload)
            
            try:
                data = response.json()
            except Exception as e:
                print(f"Failed to parse JSON for network {network}: {e}")
                continue
            
            print(f"  Data received for network {network}: {data}")
            
            if 'header' not in data or 'data' not in data or len(data['header']) < 2:
                print(f"  No valid data returned for network {network}")
                continue
            
            dates = data["header"][0]["rows"]
            profile_ids = data["header"][1]["rows"]
            metric_names = data["header"][2]["rows"]
            values = data["data"]

            data_records = []
            for date_idx, date in enumerate(dates):
                for profile_idx, profile_id in enumerate(profile_ids):
                    profile_values = values[date_idx][profile_idx]
                    
                    record = {
                        "date": date,
                        "profile_id": profile_id,
                        "network": network
                    }
                    
                    for metric_idx, metric in enumerate(metric_names):
                        record[f"{network}_{metric}"] = profile_values[metric_idx]

                    data_records.append(record)

            df = pd.DataFrame(data_records)
            df_all_metrics = pd.concat([df_all_metrics, df], axis=0, ignore_index=True, sort=False)

        df_all_metrics.fillna(0, inplace=True)
        df_all_metrics = df_all_metrics.reset_index(drop=True)
        
        return df_all_metrics

    def load_to_bigquery(self, df, destination_table):
        """Loads the DataFrame to a BigQuery table."""
        pandas_gbq.to_gbq(
            df,
            destination_table=destination_table,
            project_id=self.project_id,
            if_exists="append",
            credentials=self.credentials,
        )
        print(f"Data successfully loaded to {destination_table}")

# Usage
if __name__ == "__main__":
    base_url = "https://api.emplifi.io"
    api_key = "YOUR_API_KEY"
    credentials_file = "path/to/your/key.json"
    project_id = "your_project"
    destination_table = "your_dataset.your_table"

    fetcher = EmplifiMetricsFetcher(base_url, api_key, credentials_file, project_id)

    # Define profiles and metrics for each network
    network_profiles = {
        "facebook": fetcher.get_profiles("facebook"),
        "instagram": fetcher.get_profiles("instagram"),
        "tiktok": fetcher.get_profiles("tiktok"),
        "youtube": fetcher.get_profiles("youtube")
    }
    
    network_metrics = {
        "facebook": ['fans_change', 'fans_lifetime', 'insights_fans_lifetime', 'insights_reactions', 'insights_reach', 'insights_impressions'],
        "instagram": ['followers_lifetime', 'followers_change', 'insights_reach', 'insights_impressions'],
        "tiktok": ['insights_engagements', 'insights_fans_lifetime', 'insights_fans_change', 'insights_video_views'],
        "youtube": ['interaction_change', 'subscribers_lifetime', 'subscribers_change', 'views_change']
    }

    # Delete past month's data
    fetcher.delete_past_month_data(destination_table)

    # Fetch and load metrics data
    df_result = fetcher.fetch_metrics(network_profiles, network_metrics)
    fetcher.load_to_bigquery(df_result, destination_table)
