from hiwrap_utils.tiles_rad_range import HIWRAPTilesPointCloudDataProcess, get_date_from_url
from hiwrap_utils.s3_updnload import download_from_s3, upload_folder_to_s3, get_keys

def hiwrap():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Hs3"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "hiwrap"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name)

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name):
  Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/HS3_HIWRAP_"
  keys = get_keys(bucket_name, Prefix)
  for s3_raw_file_key in keys:
    try:
      # download
      sdate = get_date_from_url(s3_raw_file_key)
      dest_dir = f"/tmp/{field_campaign}/{instrument_name}/zarr/{sdate}"
      downloaded_dest = download_from_s3(bucket_name, s3_raw_file_key, dest_dir)
      print(f"Downloaded {s3_raw_file_key}")

      # generate 3dtiles
      obj = HIWRAPTilesPointCloudDataProcess()
      data = obj.ingest(downloaded_dest)
      pre_processed_data = obj.preprocess(data)
      point_clouds_tileset_dest = obj.prep_visualization(pre_processed_data)
      print("Generated 3Dtiles")

      print(point_clouds_tileset_dest)
      # upload
      s3_key = f"{field_campaign}/{output_data_dir}/hiwrap/{sdate}" # DESTINATION
      upload_folder_to_s3(point_clouds_tileset_dest, bucket_name, s3_key)
      print(f"Uploaded generated 3Dtiles for {s3_raw_file_key} to {bucket_name}/{s3_key} in s3.")
    except Exception as e:
      print("Error on conversion", e)

  print("All Hiwrap to 3Dtile conversion complete.")
    
hiwrap()