import os
import shutil

import boto3
from boto3 import client as boto_client
from botocore.exceptions import ClientError, NoCredentialsError

from helper.ingestToZarr import ingest
from helper.pointcloud import generate_point_cloud

class Dropsonde3DTiles:
  def __init__(self):
    # constructor
    pass
  
  def get_files(self, bucket_name="ghrc-fcx-field-campaigns-szg", prefix="CPEX-AW/instrument-raw-data/dropsonde"):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{prefix}/CPEXAW-DROPSONDE_"):
        url = "s3://" + bucket_name + "/" + obj.key
        # url = f"https://{bucket_name}.s3.amazonaws.com" + "/" + prefix + "/" + obj.key
        keys.append(url)
    return keys
    
  def upload_file(self, source_file_path, bucket_name="ghrc-fcx-field-campaigns-szg", prefix="CPEX-AW/instrument-processed-data/dropsonde"):
    s3 = boto3.client('s3')
    try:
      files = os.listdir(source_file_path)
      for file in files:
          fname = os.path.join(source_file_path, file) # SOURCE
          actualprefix = f"{prefix}/{file}" # DESTINATION
          s3.upload_file(fname, bucket_name, actualprefix)
    except ClientError as e:
      print(e)
    except NoCredentialsError:
        print("%%Credentials not available")
  
  def data_reader(self, s3_url):
    ## Open data file
    bucket_name = s3_url.split("/")[2]
    key = s3_url.split(f"{bucket_name}/")[-1] # need key without starting /
    s3 = boto_client('s3')
    fileobj = s3.get_object(Bucket=bucket_name, Key=key)
    file = fileobj['Body'].read()
    return file


def main():
  ds = Dropsonde3DTiles()
  s3_url_list = ds.get_files()
  for s3_url in s3_url_list:
    try:
      print("Generating skewT for: ", s3_url)
      name = s3_url.split('/')[-1]
      date = name.split('_D')[1].split('_')[0]
      time = name.split('_D')[1].split('_')[1]
      # create dir for stroing skewT images
      path = r'/tmp/dropsonde/output/3dtile/' + date + '/' + time
      data = ds.data_reader(s3_url)
      if not os.path.exists(path):
        os.makedirs(path)
      else:
        shutil.rmtree(path)
        os.makedirs(path)
      ingest(path, data, date, time)
      # generaete pointcloud
      point_cloud_folder = f"{path}/point_cloud"
      generate_point_cloud("ref",  0,  1000000000000, path, point_cloud_folder)
      # upload the generated 3dtile
      ds.upload_file(f"{path}/point_cloud", bucket_name="ghrc-fcx-field-campaigns-szg", prefix=f"CPEX-AW/instrument-processed-data/dropsonde/3dtiles/{date}")
      # ds.upload_file(f"{path}/point_cloud", bucket_name="ghrc-fcx-field-campaigns-szg", prefix=f"CPEX-AW/instrument-processed-data/dropsonde/3dtiles/{date}/dropsonde-{time}")
      print("Generated skewT for: ", s3_url)
    except Exception as e:
      print("Error during conversion for: ", s3_url, ". Error on", e)
  print("Done!")
    
main()