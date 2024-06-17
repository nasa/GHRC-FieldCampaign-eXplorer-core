import os
import zarr
import numpy as np
import shutil
import boto3
from pathlib import Path
from boto3 import client as boto_client
import tarfile
import glob

from npol_utils.point_cloud import generate_point_cloud
from npol_utils.s3_updnload import upload_to_s3
from uf_reader import Reader as UFReader

from npol_czml_writer import NpolCzmlWriter
from helper.conversion_helper import collectAvailabilityDateTimeRange

s3_client = boto3.client('s3')

# META needed for ingest
campaign = 'Olympex'
collection = "AirborneRadar"
dataset = "gpmValidationOlympexcrs"
variables = ["ref"]
renderers = ["point_cloud"]
chunk = 262144
to_rad = np.pi / 180
to_deg = 180 / np.pi

def ingest(folder, filePath):
    """
    Converts Level 1B crs data from s3 to zarr file and then stores it in the provided folder
    Args:
        folder (string): name to hold the raw files.
        file (string): the s3 url to the raw file. WHAT FORMAT IS IT IN in hdf5 format
    """
    store = zarr.DirectoryStore(folder)
    root = zarr.group(store=store)
    
    # Create empty rows for modified data    
    z_chunk_id = root.create_dataset('chunk_id', shape=(0, 2), chunks=None, dtype=np.int64)
    z_location = root.create_dataset('location', shape=(0, 3), chunks=(chunk, None), dtype=np.float32)
    z_time = root.create_dataset('time', shape=(0), chunks=(chunk), dtype=np.int32)
    z_vars = root.create_group('value')
    z_ref = z_vars.create_dataset('atb', shape=(0), chunks=(chunk), dtype=np.float32)
    n_time = np.array([], dtype=np.int64)

    print("Accessing file to convert to zarr ")

    ufr = UFReader(filePath)
    uf_datas = ufr.read_data() # it will return a generator.

    # use plain python array to append, for performance reasons.
    atb = []
    lon = []
    lat = []
    alt = []
    time = []

    # using the generator, populate all the lon, lat, alt and atb values
    for uf_data in uf_datas:
        atb.append(np.float64(uf_data['CZ']))
        lon.append(np.float64(uf_data['lon']))
        lat.append(np.float64(uf_data['lat']))
        alt.append(np.float64(uf_data['height']))
        time.append(np.datetime64(uf_data['timestamp']).astype('timedelta64[s]').astype(np.int64))

    atb = np.array(atb, dtype=np.float64)
    lon = np.array(lon, dtype=np.float64)
    lat = np.array(lat, dtype=np.float64)
    alt = np.array(alt, dtype=np.float64)
    time = np.array(time, dtype=np.int64)

    ## using the values, create a zarr file and return it.
    
    # sort data by time
    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    atb = atb[sort_idx]
    time = time[sort_idx]

    # Now populate (append) the empty rows with modified data.
    z_location.append(np.stack([lon, lat, alt], axis=-1))
    z_ref.append(atb)

    n_time = np.append(n_time, time)

    idx = np.arange(0, n_time.size, chunk)
    chunks = np.zeros(shape=(idx.size, 2), dtype=np.int64)
    chunks[:, 0] = idx
    chunks[:, 1] = n_time[idx]
    z_chunk_id.append(chunks)

    epoch = np.min(n_time)
    n_time = (n_time - epoch).astype(np.int32)
    z_time.append(n_time)

    # save it.
    root.attrs.put({
        "campaign": campaign,
        "collection": collection,
        "dataset": dataset,
        "variables": variables,
        "renderers": renderers,
        "epoch": int(epoch)
    })

def downloadFromS3(bucket_name, s3_key, dest_dir):
    s3 = boto_client('s3')
    filename = s3_key.split('/')[3]
    dest_dir = '/tmp/npol_olympex/raw/'
    dest = dest_dir + filename
    if os.path.exists(dest_dir): shutil.rmtree(f"{dest_dir}")
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    print("Downloading file",s3_key,"from bucket",bucket_name, " into dir:", dest_dir)
    s3.download_file(
        Bucket = bucket_name,
        Key = s3_key,
        Filename = dest
    )
    return dest

def untarr(raw_file_dir, raw_file_path, filename):
    unzipped_file_path = raw_file_dir + filename.split(".")[0] # removing the .tar.gz # this is important
    if raw_file_path.endswith("tar.gz"):
        with tarfile.open(raw_file_path, "r:gz") as t:
            t.extractall(unzipped_file_path)
    elif raw_file_path.endswith("tar"):
        with tarfile.open(raw_file_path, "r:") as t:
            t.extractall(unzipped_file_path)
    return unzipped_file_path

# ------------------START--------------------------------

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name):
    # for s3_raw_file_key in keys:
    # download each input file.
    # unzip it
    # go inside rhi_a dir,
    # list all the files.
    # for each file, run ingest.
    # generate point clouds.
    # upload all of the pointcloud files.

    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/olympex_npol"):
        keys.append(obj.key)

    raw_file_dir = '/tmp/npol_olympex/raw/' # local dir where raw file resides.

    for s3_key in keys:
        # create a czml with all the 3dtiles information for a single day
        czml_writer = NpolCzmlWriter()

        filename = s3_key.split('/')[3]
        raw_file_path = downloadFromS3(bucket_name, s3_key, raw_file_dir) # inc file name
        # the raw file is for a single day. When unzipped, it will contain several data collected every 20 mins
        unzipped_file_path = untarr(raw_file_dir, raw_file_path, filename)
        # unzipped_file_path = '/tmp/npol_olympex/raw/olympex_npol_2015-1203'
        minutely_datas = glob.glob(f"{unzipped_file_path}/*/rhi_a/*.uf.gz")
        # remove  ocean scans with the raw data containing Rhia nn_nn as 20-40 data. Only visualizing nn_nn 00_20 data.
        filtered_files = [filepath for filepath in minutely_datas if "rhi_20-40" not in filepath]
        # sort according to date time.
        filtered_files.sort()
        # for the list of uf files within a single day, find the availability date time for each of them.
        availability_time_range = collectAvailabilityDateTimeRange(filtered_files)
        for index, minute_data_path in enumerate(filtered_files):
        # iterate to create 3d tiles.
            print(f"\n{index}. converting for {minute_data_path}")
            # convert and save.
            # # SOURCE DIR.
            sdate = minute_data_path.split("/")[-1].split("_")[2]
            # CREATE A LOCAL DIR TO HOLD RAW DATA AND CONVERTED DATA
            tileFolder = minute_data_path.split("/")[-1].split(".")[0]
            folder = f"/tmp/npol_olympex/zarr/{sdate}/{tileFolder}" # intermediate folder for zarr file (date + time), time rep by index.
            point_cloud_folder = f"{folder}/point_cloud" # intermediate folder for 3d tiles, point cloud
            if os.path.exists(folder): shutil.rmtree(f"{folder}")
            # os.mkdir(folder)
            Path(folder).mkdir(parents=True, exist_ok=True)
            # LOAD FROM SOURCE WITH NECESSARY PRE PROCESSING. CONVERT LEVEL 1B RAW FILES INTO ZARR FILE.
            ingest(folder, minute_data_path)
            # # CONVERT ZARR FILE INTO 3D TILESET JSON.
            generate_point_cloud("atb",  0,  1000000000000, folder, point_cloud_folder)
            # # UPLOAD CONVERTED 3d Tiles (Pointcloud) FILES.
            files = os.listdir(point_cloud_folder)
            for file in files:
                fname = os.path.join(point_cloud_folder, file) # SOURCE
                s3name = f"{field_campaign}/{output_data_dir}/npol/{sdate}/{tileFolder}/{file}" # DESTINATION
                print(f"uploaded to {s3name}.")
                upload_to_s3(fname, bucket_name, s3_name=s3name)
            # after uploading the 3d tile point cloud, track them in the czml.
            tileLocation = f"https://{bucket_name}.s3.amazonaws.com/{field_campaign}/{output_data_dir}/{instrument_name}/{sdate}/{tileFolder}/tileset.json"
            avail_start = availability_time_range[index][0]
            avail_end = availability_time_range[index][1]
            czml_writer.add3dTiles(index, tileLocation, avail_start, avail_end)
            print(f"NPOL 3d tile conversion for {sdate} done.")
        # upload the czml.
        output_czml = czml_writer.get_string()
        outfile = f"{field_campaign}/{output_data_dir}/{instrument_name}/{sdate}/knit.czml"
        s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)
        print(f"NPOL CZML conversion for {sdate} done.")

def npol():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "npol"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name)


npol()
