import os
import time
from minio import Minio
from minio.error import S3Error

# Credenciais do MinIO
endpoint = "172.26.0.3"
port = 9000
access_key = "DqKP9jCEWxoHZOwMeaha"
secret_key = "pu8AfOYua8KDwfdqUDGuwFfzUb1hPIKbD6aMeWQL"

minio_bucket = 'prd-jedi-files'

minio_client = Minio(f"{endpoint}:{port}",
                     access_key=access_key,
                     secret_key=secret_key,
                     secure=False)  # Mude para True se estiver usando HTTPS

file_paths = [
    '/app/cdc/obi-wan.py',
    '/app/jdbc/anakin.py',
    '/app/trusted/ahsoka.py',
    '/app/SparkWarsLib',
    '/app/shared_etls'
]

def sync_etl_files_to_jedi_file_bucket_in_minio(file_path):
    """
    Uploads files to a MinIO bucket periodically.

    This script uploads specified files to a MinIO bucket at regular intervals. It initializes the MinIO client with
    provided credentials and endpoint information, and then continuously uploads a list of files to the specified
    MinIO bucket.

    Modules:
        - os: Provides a portable way of using operating system dependent functionality.
        - time: Provides various time-related functions.
        - minio: Provides an API to interact with MinIO server.
        - minio.error: Provides error handling for MinIO operations.

    Functions:
        - sync_etl_files_to_jedi_file_bucket_in_minio(file_path): Uploads a single file to the MinIO bucket.

    Usage:
        The script initializes the MinIO client and defines the list of file paths to be uploaded. It then enters an
        infinite loop, uploading each file in the list to the specified MinIO bucket every 10 seconds.

    Parameters:
        - endpoint: (str) The MinIO server endpoint.
        - port: (int) The port number on which the MinIO server is running.
        - access_key: (str) The access key for the MinIO server.
        - secret_key: (str) The secret key for the MinIO server.
        - minio_bucket: (str) The name of the MinIO bucket to which files will be uploaded.
        - file_paths: (list of str) A list of file paths to be uploaded to the MinIO bucket.

    Function Details:
        - sync_etl_files_to_jedi_file_bucket_in_minio(file_path):
            Uploads the specified file to the MinIO bucket.
            
            Parameters:
                - file_path: (str) The full path to the file to be uploaded.
            
            Returns:
                None. The function prints a success message if the upload is successful, or an error message if the upload fails.
    """


    if os.path.isdir(file_path):
        for root, dirs, files in os.walk(file_path):
            for file in files:
                full_path = os.path.join(root, file)
                object_name = os.path.relpath(full_path, start=os.path.dirname(file_path))  # Preserve the folder structure
                try:
                    minio_client.fput_object(minio_bucket, object_name, full_path)
                    print(f"File {full_path} successfully sent to the bucket {minio_bucket}")
                except S3Error as err:
                    print(err)
    else:
        file_name = os.path.basename(file_path)
        try:
            minio_client.fput_object(minio_bucket, file_name, file_path)
            print(f"File {file_name} successfully sent to the bucket {minio_bucket}")
        except S3Error as err:
            print(err)


while True:
    for path in file_paths:
        sync_etl_files_to_jedi_file_bucket_in_minio(path)
    time.sleep(10)
