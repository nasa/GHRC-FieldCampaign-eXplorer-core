import os
import zarr
import numpy as np
import xarray as xr
import shutil
import boto3
from pathlib import Path

from cpl_utils.ingest_utils import downloadFromS3
from cpl_utils.point_cloud import generate_point_cloud
from cpl_utils.s3_updnload import upload_to_s3


# META needed for ingest
campaign = 'Hs3'
collection = "AirborneRadar"
dataset = "hs3cpl"
variables = ["ATB_1064"]
renderers = ["point_cloud"]
chunk = 262144
to_rad = np.pi / 180
to_deg = 180 / np.pi


def get_date_from_filename(filename):
  return filename.split('_')[-1].split(".")[0]

def ingest(folder, file, s3bucket):
    """
    Converts Level 1B hiwrap data from s3 to zarr file and then stores it in the provided folder
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

    date = get_date_from_filename(file)
    base_time = np.datetime64('{}-{}-{}'.format(date[:4], date[4:6], date[6:]))

    print("Accessing file from S3 ", file)
    filepath = downloadFromS3(s3bucket, file, f"{folder}/nc")

    # open dataset.
    with xr.open_dataset(filepath, decode_cf=False) as ds:
        # data columns extract
        ref = ds[variables[0]].values #Hiwrap radar reflectivity
        lat = ds['Latitude'].values
        lon = ds['Longitude'].values
        alt = ds['Plane_Alt'].values # altitude of aircraft in meters
        roll = ds["Plane_Roll"].values
        pitch = ds["Plane_Pitch"].values
        head = ds["Plane_Heading"].values
        rad_range = ds["Bin_Alt"].values
        delta = [(base_time + (h*3600+m*60+s).astype('timedelta64[s]')) for (h,m,s) in 
                zip(ds['Hour'].values, ds['Minute'].values, ds['Second'].values)] #delta is in seconds 
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

    # remove nan and infinite using mask
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
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/HS3_CPL_"):
        if (obj.key.split("_")[2] == "ATB" and 'a' in obj.key.split("_")[3]): # for a given date, only process ATB files with a in the sortie. HS3_CPL_<type>_<sortie>_yyyymmdd.nc
          keys.append(obj.key)
    result = keys
    for s3_raw_file_key in result:
        # SOURCE DIR.
        sdate = get_date_from_filename(s3_raw_file_key)        
        print(f'processing CRS file {s3_raw_file_key}')

        # CREATE A LOCAL DIR TO HOLD RAW DATA AND CONVERTED DATA
        folder = f"/tmp/{field_campaign}/{instrument_name}/zarr/{sdate}"
        point_cloud_folder = f"{folder}/point_cloud"
        if os.path.exists(folder): shutil.rmtree(f"{folder}")
        # os.mkdir(folder)
        Path(folder).mkdir(parents=True, exist_ok=True)
        # LOAD FROM SOURCE WITH NECESSARY PRE PROCESSING. CONVERT LEVEL 1B RAW FILES INTO ZARR FILE.
        ingest(folder, s3_raw_file_key, bucket_name)
        # return
        # CONVERT ZARR FILE INTO 3D TILESET JSON.
        generate_point_cloud("ref",  0,  1000000000000, folder, point_cloud_folder)

        # UPLOAD CONVERTED FILES.
        files = os.listdir(point_cloud_folder)
        s3 = boto3.client('s3')
        for file in files:
            fname = os.path.join(point_cloud_folder, file) # SOURCE
            s3name = f"{field_campaign}/{output_data_dir}/{instrument_name}/{sdate}/{file}" # DESTINATION
            print(f"uploaded to {s3name}.")
            upload_to_s3(fname, bucket_name, s3_name=s3name)


def cpl():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Hs3"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "cpl"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name)

cpl()