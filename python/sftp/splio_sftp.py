import datetime
import paramiko
from google.cloud import storage
from io import BytesIO
import zipfile
import os


class SFTPToGCS:
    def __init__(self, ftp_host, ftp_port, ftp_user, ftp_pass, gcs_key_path, gcs_bucket_name):
        # SFTP configuration
        self.ftp_host = ftp_host
        self.ftp_port = ftp_port
        self.ftp_user = ftp_user
        self.ftp_pass = ftp_pass

        # GCS configuration
        self.gcs_key_path = gcs_key_path
        self.gcs_bucket_name = gcs_bucket_name

        # SFTP and GCS clients
        self.sftp = None
        self.gcs_client = None

    def connect_sftp(self):
        """Establish an SFTP connection."""
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=self.ftp_host, port=self.ftp_port, username=self.ftp_user, password=self.ftp_pass
        )
        self.sftp = ssh.open_sftp()

    def connect_gcs(self):
        """Initialize GCS client."""
        self.gcs_client = storage.Client.from_service_account_json(self.gcs_key_path)

    def list_files(self, remote_path, keyword):
        """List files on the SFTP server with a specific keyword and today's date."""
        files = self.sftp.listdir_attr(remote_path)
        today_date = datetime.datetime.now().strftime("%Y-%m-%d")
        matching_files = [
            file
            for file in files
            if keyword in file.filename
            and datetime.datetime.fromtimestamp(file.st_mtime).strftime("%Y-%m-%d") == today_date
        ]
        return matching_files

    def upload_to_gcs(self, file_data, destination_blob_name):
        """Upload a file to Google Cloud Storage."""
        bucket = self.gcs_client.get_bucket(self.gcs_bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(file_data, content_type="application/octet-stream")
        print(f"Uploaded {destination_blob_name} to GCS bucket {self.gcs_bucket_name}")

    def process_files(self, remote_path, keyword):
        """Download files from SFTP, process them, and upload them to GCS."""
        files = self.list_files(remote_path, keyword)
        today_date = datetime.datetime.now().strftime("%Y-%m-%d")

        for file in files:
            with self.sftp.open(f"{remote_path}/{file.filename}", "rb") as file_obj:
                with BytesIO(file_obj.read()) as file_data:
                    # Unzip the file
                    with zipfile.ZipFile(file_data) as z:
                        for name in z.namelist():
                            # Rename the unzipped file
                            new_name = f"your_filename_{today_date}.csv"
                            with z.open(name) as f:
                                # Upload the renamed file to GCS
                                self.upload_to_gcs(f, new_name)

    def close_connections(self):
        """Close the SFTP connection."""
        if self.sftp:
            self.sftp.close()


def main():
    # Configuration
    ftp_host = "sftp.splio.com"
    ftp_port = 22
    ftp_user = "your_user"
    ftp_pass = "your_password"
    gcs_key_path = "path_to_your_key"
    gcs_bucket_name = "your_bucket"

    # Create instance of the SFTPToGCS class
    processor = SFTPToGCS(
        ftp_host, ftp_port, ftp_user, ftp_pass, gcs_key_path, gcs_bucket_name
    )

    # Process files
    try:
        processor.connect_sftp()
        processor.connect_gcs()
        processor.process_files(remote_path="export/", keyword="your_keyword")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        processor.close_connections()


if __name__ == "__main__":
    main()
