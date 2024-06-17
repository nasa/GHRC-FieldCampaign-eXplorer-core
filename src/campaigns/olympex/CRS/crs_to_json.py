import os
import zarr
import numpy as np
import xarray as xr
import shutil
import boto3
from pathlib import Path

from crs_utils.ingest_utils import add24hr,  CRSaccess
from crs_utils.point_cloud import generate_point_cloud
from crs_utils.s3_updnload import upload_to_s3


# META needed for ingest
campaign = 'Olympex'
collection = "AirborneRadar"
dataset = "gpmValidationOlympexcrs"
variables = ["zku"]
renderers = ["point_cloud"]
chunk = 262144
to_rad = np.pi / 180
to_deg = 180 / np.pi

def ingest(folder, file, s3bucket):
    """
    Converts Level 1B crs data from s3 to zarr file and then stores it in the provided folder
    Args:
        folder (string): name to hold the raw files.
        file (string): the s3 url to the raw file.
    """
    store = zarr.DirectoryStore(folder)
    root = zarr.group(store=store)
    
    # Create empty rows for modified data    
    z_chunk_id = root.create_dataset('chunk_id', shape=(0, 2), chunks=None, dtype=np.int64)
    z_location = root.create_dataset('location', shape=(0, 3), chunks=(chunk, None), dtype=np.float32)
    z_time = root.create_dataset('time', shape=(0), chunks=(chunk), dtype=np.int32)
    z_vars = root.create_group('value')
    z_ref = z_vars.create_dataset('ref', shape=(0), chunks=(chunk), dtype=np.float32)
    n_time = np.array([], dtype=np.int64)

    date = file.split("_")[2]
    base_time = np.datetime64('{}-{}-{}'.format(date[:4], date[4:6], date[6:]))

    print("Accessing file from S3 ", file)

    # read from s3 url (file) in s3 bucket.
    fileObj = CRSaccess(file, s3bucket=s3bucket)

    # open dataset.
    with xr.open_dataset(fileObj, decode_cf=False) as ds:
        # added for time correction for over 24h UTC
        hr = add24hr(ds['timed'].values)
        delta = (hr * 3600).astype('timedelta64[s]') + base_time
        # time correction end
        
        # data columns extract
        ref = ds[variables[0]].values #CRS radar reflectivity
        lat = ds['lat'].values
        lon = ds['lon'].values
        alt = ds['altitude'].values # altitude of aircraft in meters
        roll = ds["roll"].values
        pitch = ds["pitch"].values
        head = ds["head"].values
        rad_range = ds["range"].values
    num_col = ref.shape[0] # number of cols
    num_row = ref.shape[1] # number of rows

    # data frame formation
    delta = np.repeat(delta, num_row)
    lon = np.repeat(lon, num_row)
    lat = np.repeat(lat, num_row)
    alt = np.repeat(alt, num_row)
    roll = np.repeat(roll * to_rad, num_row)
    pitch = np.repeat(pitch * to_rad, num_row)
    head = np.repeat(head * to_rad, num_row)
    rad_range = np.tile(rad_range, num_col)
    ref = ref.flatten()

    # time correction.
    time = (delta - np.datetime64('1970-01-01')).astype('timedelta64[s]').astype(np.int64)

    x, y, z = down_vector(roll, pitch, head)
    x = np.multiply(x, np.divide(rad_range, 111000 * np.cos(lat * to_rad)))
    y = np.multiply(y, np.divide(rad_range, 111000))
    z = np.multiply(z, rad_range)

    lon = np.add(-x, lon)
    lat = np.add(-y, lat)
    alt = np.add(z, alt)

    # sort data by time
    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    ref = ref[sort_idx]
    time = time[sort_idx]

    # remove nan and infinite using mask ???
    mask = np.logical_and(np.isfinite(ref), alt > 0)
    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    ref = ref[mask]
    time = time[mask]

    # Now populate (append) the empty rows with modified data.
    z_location.append(np.stack([lon, lat, alt], axis=-1))
    z_ref.append(ref)
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

#UTILS
def down_vector(roll, pitch, head):
    x = np.sin(roll) * np.cos(head) + np.cos(roll) * np.sin(pitch) * np.sin(head)
    y = -np.sin(roll) * np.sin(head) + np.cos(roll) * np.sin(pitch) * np.cos(head)
    z = -np.cos(roll) * np.cos(pitch)
    return (x, y, z)


# ------------------START--------------------------------

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/olympex"):
        keys.append(obj.key)

    result = keys
    for s3_raw_file_key in result:
        # SOURCE DIR.
        sdate = s3_raw_file_key.split('_')[2]
        print(f'processing CRS file {s3_raw_file_key}')

        # CREATE A LOCAL DIR TO HOLD RAW DATA AND CONVERTED DATA
        folder = f"/tmp/crs_olympex/zarr/{sdate}"
        point_cloud_folder = f"{folder}/point_cloud"
        if os.path.exists(folder): shutil.rmtree(f"{folder}")
        # os.mkdir(folder)
        Path(folder).mkdir(parents=True, exist_ok=True)
        # LOAD FROM SOURCE WITH NECESSARY PRE PROCESSING. CONVERT LEVEL 1B RAW FILES INTO ZARR FILE.
        ingest(folder, s3_raw_file_key, bucket_name)
        # CONVERT ZARR FILE INTO 3D TILESET JSON.
        generate_point_cloud("ref",  0,  1000000000000, folder, point_cloud_folder)

        # UPLOAD CONVERTED FILES.
        files = os.listdir(point_cloud_folder)
        s3 = boto3.client('s3')
        for file in files:
            fname = os.path.join(point_cloud_folder, file) # SOURCE
            s3name = f"{field_campaign}/{output_data_dir}/crs/{sdate}/{file}" # DESTINATION
            print(f"uploaded to {s3name}.")
            upload_to_s3(fname, bucket_name, s3_name=s3name)


def crs():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "crs"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name)


crs()