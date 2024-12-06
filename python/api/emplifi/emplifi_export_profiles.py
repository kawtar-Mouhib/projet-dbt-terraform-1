import requests
import base64
import pandas as pd
import json
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq

class EmplifiProfileFetcher:
    def __init__(self, base_url, api_key, networks, credentials_file, project_id):
        self.base_url = base_url
        self.header = {
            "Authorization": f"Basic {base64.b64encode(api_key.encode()).decode()}"
        }
        self.networks = networks
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_file,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.project_id = project_id
        self.client = bigquery.Client(credentials=self.credentials, project=project_id)

    def fetch_profiles_for_network(self, network):
        """Fetch profiles for a specific network."""
        api_endpoint = f"/3/{network}/profiles"
        request_url = self.base_url + api_endpoint
        print(f"Fetching profiles for network: {network}")
        
        try:
            response = requests.get(request_url, headers=self.header)
            response.raise_for_status()  # Raise an exception for HTTP errors
            profiles = response.json().get('profiles', [])
            if profiles:
                df_network = pd.DataFrame(profiles)
                df_network['network'] = network
                return df_network
            else:
                print(f"No profiles found for network: {network}")
                return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching profiles for network {network}: {e}")
            return pd.DataFrame()

    def fetch_all_profiles(self):
        """Fetch profiles for all specified networks and concatenate them into a single DataFrame."""
        df_global = pd.DataFrame()
        for network in self.networks:
            df_network = self.fetch_profiles_for_network(network)
            df_global = pd.concat([df_global, df_network], axis=0, ignore_index=True)
        return df_global

    def clean_data(self, df):
        """Clean and prepare the DataFrame before loading to BigQuery."""
        df["profile_labels"] = df["profile_labels"].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else str(x)
        )
        df['community_enabled'] = df['community_enabled'].apply(
            lambda x: None if pd.isna(x) else str(x)
        )
        return df

    def load_to_bigquery(self, df, destination_table):
        """Load the DataFrame to a BigQuery table."""
        pandas_gbq.to_gbq(
            df,
            destination_table=destination_table,
            project_id=self.project_id,
            if_exists="replace",
            credentials=self.credentials,
        )
        print(f"Data successfully loaded to {destination_table}")

# Usage
if __name__ == "__main__":
    base_url = "https://api.emplifi.io"
    api_key = "YOUR_API_KEY"
    networks = ["facebook", "instagram", "tiktok", "youtube"]
    credentials_file = "path/to/your/key.json"
    project_id = "your_project"
    destination_table = "your_dataset.your_table"

    fetcher = EmplifiProfileFetcher(base_url, api_key, networks, credentials_file, project_id)
    
    # Fetch all profiles
    df_profiles = fetcher.fetch_all_profiles()
    
    # Clean data
    df_profiles = fetcher.clean_data(df_profiles)
    
    # Load data to BigQuery
    fetcher.load_to_bigquery(df_profiles, destination_table)
