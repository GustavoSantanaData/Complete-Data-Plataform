from minio import Minio
import os
import time 


client = Minio(
    endpoint="172.26.0.3:9000", 
    access_key="DqKP9jCEWxoHZOwMeaha", 
    secret_key="pu8AfOYua8KDwfdqUDGuwFfzUb1hPIKbD6aMeWQL", 
    secure=False
)


def create_local_folders_and_download_files_dags():
    """
    Create local folders and download files from the 'prd-dags-jedi' bucket.

    This function connects to a Minio server and lists all objects in the 'prd-dags-jedi' bucket.
    It creates the necessary local directories and downloads each file from the bucket to the local 
    './dags' directory, preserving the bucket's directory structure.

    The function prints a message each time a file is successfully downloaded.
    """

    bucket_name = "prd-dags-jedi"
    local_directory = "./dags"  

    objects = client.list_objects(bucket_name, recursive=True)
    
    created_folders = set()
    
    for obj in objects:
        object_path = obj.object_name
        

        local_path = os.path.join(local_directory, object_path)
        
        parent_directory = os.path.dirname(local_path)
        
        if parent_directory not in created_folders:
            os.makedirs(parent_directory, exist_ok=True)
            created_folders.add(parent_directory)
        
        client.fget_object(bucket_name, object_path, local_path)
        
        print(f"Downloaded {object_path} to {local_path}")


def create_local_folders_and_download_files_jedi_files():
    """
    Create local folders and download files from the 'prd-jedi-files' bucket.

    This function connects to a Minio server and lists all objects in the 'prd-jedi-files' bucket.
    It creates the necessary local directories and downloads each file from the bucket to the local 
    './dags/jedi_files' directory, preserving the bucket's directory structure.

    The function prints a message each time a file is successfully downloaded.
    """

    bucket_name = "prd-jedi-files"
    local_directory = "./dags/jedi_files"  

    objects = client.list_objects(bucket_name, recursive=True)
    
    created_folders = set()
    
    for obj in objects:
        object_path = obj.object_name
        

        local_path = os.path.join(local_directory, object_path)
        
        parent_directory = os.path.dirname(local_path)
        
        if parent_directory not in created_folders:
            os.makedirs(parent_directory, exist_ok=True)
            created_folders.add(parent_directory)
        
        client.fget_object(bucket_name, object_path, local_path)
        
        print(f"Downloaded {object_path} to {local_path}")
        

while True:
    create_local_folders_and_download_files_dags()
    create_local_folders_and_download_files_jedi_files()
    time.sleep(30)
