import os
import shutil
import boto3
from pathlib import Path

from boto3 import client as boto_client
from botocore.exceptions import ClientError, NoCredentialsError

def download_from_s3(bucket_name, s3_key, dest_dir):
    """Download a file from an S3 bucket
     dest_dir: Destination directory for the downloaded file
     bucket_name: S3 bucket to upload to
     s3_key: S3 object name. If not specified then file_name is used
    """
    s3 = boto_client('s3')
    filename = s3_key.split('/')[3]
    dest = f"{dest_dir}/{filename}"
    if os.path.exists(dest_dir): shutil.rmtree(f"{dest_dir}")
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    print("Downloading file",s3_key,"from bucket",bucket_name, " into dir:", dest_dir)
    s3.download_file(
        Bucket = bucket_name,
        Key = s3_key,
        Filename = dest
    )
    return dest

def upload_folder_to_s3(folder, bucket_name, s3_key):
    """Upload a Folder to an S3 bucket
     folder: folder to upload
     bucket_name: S3 bucket to upload to
     s3_key: S3 object name.
    """
    files = os.listdir(folder)
    for file in files:
        fname = os.path.join(folder, file) # SOURCE
        s3_key_f = f"{s3_key}/{file}" # can have hiecharchical destination as key
        # print(f"uploaded {file} to {s3_key}.")
        upload_to_s3(fname, bucket_name, s3_key_f)
    print("Folder uploaded.")


def upload_to_s3(file_name, bucket, key=None):
    """Upload a file to an S3 bucket
     file_name: File to upload
     bucket: S3 bucket to upload to
     object_name: S3 object name. If not specified then file_name is used
    """
    if key is None: key = file_name

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, key)
    except ClientError as e:
       print(e)
    except NoCredentialsError:
        print("%%Credentials not available")
        
def get_keys(bucket_name, Prefix):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(Prefix=Prefix):
        keys.append(obj.key)

    result = keys
    return result