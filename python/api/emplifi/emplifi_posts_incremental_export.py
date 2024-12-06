import requests
import base64
import pandas as pd
import time
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas_gbq
import json

class DataFetcher:
    def __init__(self, credentials_path, project_id):
        # Setup BigQuery client with service account key
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(credentials=self.credentials, project=project_id)

    def delete_old_data(self, network, profile, bq_table_name, start_date, end_date):
        if network == "facebook":
            profile_id_column = "facebook_profileId"
        elif network == "instagram":
            profile_id_column = "instagram_profileId"
        elif network in ("tiktok", 'youtube'):
            profile_id_column = "profile_id"
        else:
            print(f"Unsupported network: {network}")
            return 0
        
        query = f"""
        DELETE FROM `{bq_table_name}`
        WHERE {profile_id_column} = '{profile}' AND DATE(created_time) >= '{start_date}' AND DATE(created_time) <= '{end_date}'
        """
        
        print(f"Executing query: {query}")  # Debug: Print the query being executed
        
        try:
            query_job = self.client.query(query)
            query_job.result()  # Wait for the job to complete
            rows_deleted = query_job.num_dml_affected_rows
            print(f"Successfully deleted {rows_deleted} rows from {bq_table_name} for profile {profile} between {start_date} and {end_date}")
            return rows_deleted
        except Exception as e:
            print(f"Failed to delete old data from {bq_table_name} for profile {profile}: {e}")
            return 0

    def get_profiles(self, network):
        base_url = "https://api.emplifi.io"
        api_endpoint = f"/3/{network}/profiles"
        request_url = base_url + api_endpoint
        
        header = {
             'Authorization': f"Basic {base64.b64encode('MTA1NjcyMl8yNTA3MDc0XzE3MjY2NDc0MjYzOTVfZGI0Y2RlZjVjNzU3N2YzNDUwODZlZjkwZDg2NWNkYjg=:a3a9e8191aca4a704267d8d1719b9299'.encode()).decode()}"
        }
        
        response = requests.get(request_url, headers=header)
        if response.status_code == 200:
            print(f"Successfully retrieved profiles for {network}")
            profiles = [profile['id'] for profile in response.json().get('profiles', [])]
            return profiles
        else:
            print(f"Failed to retrieve profiles for {network}, status code: {response.status_code}")
            return []

    def expand_columns(self, df, network):
        if 'facebook_reactions_by_type' in df.columns:
            reactions_df = pd.json_normalize(df['facebook_reactions_by_type'])
            reactions_df.columns = [f'facebook_reaction_{col}' for col in reactions_df.columns]
            df = pd.concat([df.drop(columns=['facebook_reactions_by_type']), reactions_df], axis=1)

        attachment_columns = [col for col in df.columns if 'attachment' in col]
        print(f"Found attachment columns: {attachment_columns}")

        if attachment_columns:
            for col in attachment_columns:
                if isinstance(df[col].iloc[0], dict):
                    attachments_df = pd.json_normalize(df[col])
                    attachments_df.columns = [f'{network}_attachment' for col in attachments_df.columns]
                    df = pd.concat([df.drop(columns=attachment_columns), attachments_df], axis=1)
                else:
                    print(f"Warning: Expected a dictionary for {col}, found {type(df[col].iloc[0])} instead.")

        return df

    def convert_complex_columns(self, df):
        """
        Convert columns with complex data types (lists or dicts) to strings.
        Lists are joined with commas, and dicts are converted to JSON strings.
        """
        for col in df.columns:
            if df[col].dtype == 'object':  # Only process object-type columns
                # Apply conversion to handle lists and dicts
                df[col] = df[col].apply(lambda x: ', '.join(
                    [json.dumps(item) if isinstance(item, dict) else str(item) for item in x]
                ) if isinstance(x, list) else json.dumps(x) if isinstance(x, dict) else str(x))

                # Handle NaN or missing values
                df[col] = df[col].fillna('')
        
        return df

    def fetch_all_posts(self, network_profiles, network_posts_fields):
        base_url = "https://api.emplifi.io"
        header = {
            'Authorization': f"Basic {base64.b64encode('MTA1NjcyMl8yNTA3MDc0XzE3MjY2NDc0MjYzOTVfZGI0Y2RlZjVjNzU3N2YzNDUwODZlZjkwZDg2NWNkYjg=:a3a9e8191aca4a704267d8d1719b9299'.encode()).decode()}"
        }

        api_call_count = 0

        today = datetime.utcnow()
        start = (today - timedelta(days=14)).strftime('%Y-%m-%d')  # One weeks ago
        end = today.strftime('%Y-%m-%d')

        for network, profiles in network_profiles.items():
            posts_fields = network_posts_fields.get(network, [])
            bq_table_name = f"emplifi_export.emplifi_export_{network}_posts"

            for profile in profiles:
                print(f"Fetching posts for profile {profile} on {network}")

                df_profile_posts = pd.DataFrame()
                after = None

                while True:
                    api_call_count += 1
                    print(f"API call #{api_call_count} for profile {profile}, date range {start} to {end}")

                    if after:
                        payload = {
                            "after": after
                        }
                    else:
                        payload = {
                            "date_start": start,
                            "date_end": end,
                            "profiles": [profile],
                            "fields": posts_fields,
                            "limit": 100
                        }

                    api_endpoint = f"/3/{network}/page/posts" if network == 'facebook' else f"/3/{network}/profile/videos" if network == 'youtube' else f"/3/{network}/profile/posts"
                    request_url = base_url + api_endpoint

                    response = requests.post(request_url, headers=header, json=payload)

                    if response.status_code == 429:
                        print(f"Rate limit encountered at API call #{api_call_count}. Waiting for 5 minutes...")
                        time.sleep(300)
                        break

                    try:
                        data = response.json()
                    except Exception as e:
                        print(f"Failed to parse JSON for profile {profile}: {e}")
                        break

                    posts = data.get('data', {}).get('posts', [])
                    print(f"Received {len(posts)} posts for profile {profile} (API call #{api_call_count})")
                    remaining = data.get("data", {}).get("remaining", 0)
                    after = data.get('data', {}).get('next', None)
                    print(f"Next token: {after}")

                    if not posts:
                        print(f"No more posts data for profile {profile} from {start} to {end}")
                        break

                    data_records = []
                    for post in posts:
                        record = {
                            "profile_id": post.get('authorId', None),
                            "created_time": post.get('created_time', None),
                            "content_type": post.get('content_type', None),
                            "network": network
                        }
                        
                        if 'post_labels' in post and isinstance(post['post_labels'], list):
                            post_labels_names = [label.get('name', None) for label in post['post_labels']]
                            record['post_labels_names'] = ', '.join(filter(None, post_labels_names))

                        for field, value in post.items():
                            if field not in ['authorId', 'created_time', 'content_type']:
                                record[field] = value

                        data_records.append(record)

                    df = pd.DataFrame(data_records)
                    df_profile_posts = pd.concat([df_profile_posts, df], axis=0, ignore_index=True, sort=False)

                    if remaining == 0 or not after:
                        print("No more pages to fetch for this time range.")
                        break

                # Check if new data was fetched before deleting old data
                if df_profile_posts.empty:
                    print(f"No new data found for profile {profile} between {start} and {end}. Skipping deletion.")
                    continue
                else:
                    rows_deleted = self.delete_old_data(network, profile, bq_table_name, start, end)

                if not df_profile_posts.empty:
                    try:
                        for col in df_profile_posts.columns:
                            if col.startswith('insights_'):
                                df_profile_posts[col] = pd.to_numeric(df_profile_posts[col], errors='coerce').fillna(0)

                        columns_to_rename = {col: f"{network}_{col}" for col in df_profile_posts.columns if not col.startswith(f"{network}_") and col not in ['profile_id', 'created_time', 'content_type', 'network']}
                        df_profile_posts.rename(columns=columns_to_rename, inplace=True)

                        df_profile_posts = self.expand_columns(df_profile_posts, network)
                        df_profile_posts = self.convert_complex_columns(df_profile_posts)

                        inserted_rows = len(df_profile_posts)
                        pandas_gbq.to_gbq(
                            df_profile_posts,
                            destination_table=bq_table_name,
                            project_id="your_project",
                            if_exists="append",
                            credentials=self.credentials,
                        )
                        
                        print(f"Successfully written {inserted_rows} rows for profile {profile} on {network} to {bq_table_name}")

                    except Exception as e:
                        print(f"Failed to write posts for profile {profile} to BigQuery: {e}")

def main():
    fetcher = DataFetcher("path/to/your/key.json", "your_project")

    # Define profiles for each network
    network_profiles = {
        "facebook": fetcher.get_profiles("facebook"),
        "instagram": fetcher.get_profiles("instagram"),
        "tiktok": fetcher.get_profiles("tiktok"),
        "youtube": fetcher.get_profiles("youtube")
    }

    # Define posts for each network
    network_posts_fields = {
        "facebook": ['attachments', 'published', 'profileId', 'id', 'content', 'post_labels', 'comments', 'comments_sentiment', 'sentiment', 'content_type', 'created_time', 'interactions', 'media_type', 'reactions', 'reactions_by_type', 'shares', 'insights_engagements', 'insights_impressions', 'insights_interactions', 'insights_post_clicks', 'insights_reach', 'insights_reactions', 'insights_video_views'],
        "instagram": ['attachments', 'profileId', 'id', 'content', 'post_labels', 'comments' , 'comments_sentiment', 'sentiment', 'content_type' , 'created_time' , 'interactions', 'likes','media_type' , 'insights_engagement' , 'insights_impressions', 'insights_reach', 'insights_saves','insights_video_views'],
        "tiktok": ['authorId', 'id', 'content_type' , 'post_labels', 'created_time' , 'duration', 'insights_comments', 'insights_impressions' , 'insights_likes', 'insights_shares', 'insights_engagements', 'insights_reach', 'insights_video_views'],
        "youtube": ['authorId', 'id', 'post_labels', 'created_time' , 'duration', 'interactions', 'likes', 'comments', 'media_type' , 'insights_engagement', 'video_views'],
    }

    fetcher.fetch_all_posts(network_profiles, network_posts_fields)

if __name__ == "__main__":
    main()