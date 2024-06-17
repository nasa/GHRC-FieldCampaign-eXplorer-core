import os
import zarr
import numpy as np
import xarray as xr
import shutil
import boto3

from utils.ingest_utils import add24hr,  CRSaccess
from utils.point_cloud import generate_point_cloud
from utils.s3_updnload import upload_to_s3

def ingest(folder, file):

    store = zarr.DirectoryStore(folder)

    root = zarr.group(store=store)

    z_chunk_id = root.create_dataset('chunk_id', shape=(0, 2), chunks=None, dtype=np.int64)
    z_location = root.create_dataset('location', shape=(0, 3), chunks=(chunk, None), dtype=np.float32)
    z_time = root.create_dataset('time', shape=(0), chunks=(chunk), dtype=np.int32)
    z_vars = root.create_group('value')
    z_ref = z_vars.create_dataset('ref', shape=(0), chunks=(chunk), dtype=np.float32)
    n_time = np.array([], dtype=np.int64)

    date = file.split("_")[3]
    base_time = np.datetime64('{}-{}-{}'.format(date[:4], date[4:6], date[6:]))

    print("Accessing file from S3 ", file)

    fileObj = CRSaccess(file, s3bucket=s3bucket)

    with xr.open_dataset(fileObj, decode_cf=False) as ds:
        hr = add24hr(ds['time'].values)  # <--added for time correction for over 24h UTC
        delta = (hr * 3600).astype('timedelta64[s]') + base_time
        ref = ds["ref"].values
        lat = ds['lat'].values
        lon = ds['lon'].values
        alt = ds['height'].values
        roll = ds["roll"].values
        pitch = ds["pitch"].values
        head = ds["head"].values
        rad_range = ds["range"].values
    num_col = ref.shape[0]
    num_row = ref.shape[1]

    delta = np.repeat(delta, num_row)
    lon = np.repeat(lon, num_row)
    lat = np.repeat(lat, num_row)
    alt = np.repeat(alt, num_row)
    roll = np.repeat(roll * to_rad, num_row)
    pitch = np.repeat(pitch * to_rad, num_row)
    head = np.repeat(head * to_rad, num_row)
    rad_range = np.tile(rad_range, num_col)
    ref = ref.flatten()

    time = (delta - np.datetime64('1970-01-01')).astype('timedelta64[s]').astype(np.int64)

    x, y, z = down_vector(roll, pitch, head)
    x = np.multiply(x, np.divide(rad_range, 111000 * np.cos(lat * to_rad)))
    y = np.multiply(y, np.divide(rad_range, 111000))
    z = np.multiply(z, rad_range)

    lon = np.add(-x, lon)
    lat = np.add(-y, lat)
    alt = np.add(z, alt)

    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    ref = ref[sort_idx]
    time = time[sort_idx]

    mask = np.logical_and(np.isfinite(ref), alt > 0)

    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    ref = ref[mask]
    time = time[mask]

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

    root.attrs.put({
        "campaign": campaign,
        "collection": collection,
        "dataset": dataset,
        "variables": variables,
        "renderers": renderers,
        "epoch": int(epoch)
    })


def down_vector(roll, pitch, head):
    x = np.sin(roll) * np.cos(head) + np.cos(roll) * np.sin(pitch) * np.sin(head)
    y = -np.sin(roll) * np.sin(head) + np.cos(roll) * np.sin(pitch) * np.cos(head)
    z = -np.cos(roll) * np.cos(pitch)
    return (x, y, z)


# --------------------------------------------------
campaign = 'GOES-R PLT'
collection = "AirborneRadar"
dataset = "goesrpltcrs"
variables = ["ref"]
renderers = ["point_cloud"]
chunk = 262144
to_rad = np.pi / 180
to_deg = 180 / np.pi


s3bucket = os.getenv('RAW_DATA_BUCKET')  

dates = ['2017-04-11','2017-04-13','2017-04-16','2017-04-18', '2017-04-20', '2017-04-22', '2017-05-07',
         '2017-05-08', '2017-05-12', '2017-05-14', '2017-05-17']

for fdate in dates:
   
    os.environ['FLIGHT_DATE'] = fdate
    sdate = fdate.replace('-', '')

    s3_raw_file_key = f"fieldcampaign/goesrplt/CRS/data/GOESR_CRS_L1B_{sdate}_v0.nc"

    print(f'processing CRS file {s3_raw_file_key}')

    os.environ['CRS_OUTPUT_FLIGHT_PATH'] = f"{os.getenv('CRS_OUTPUT_PATH')}/{sdate}"

    folder = os.environ['CRS_OUTPUT_FLIGHT_PATH']
    point_cloud_folder = f"{folder}/point_cloud"
    if os.path.exists(folder): shutil.rmtree(f"{folder}")

    os.mkdir(folder)

    ingest(folder, s3_raw_file_key)
    generate_point_cloud("ref",  0,  1000000000000,folder, point_cloud_folder)

    files = os.listdir(point_cloud_folder)

    s3 = boto3.client('s3')
    instr = "crs"
    for file in files:
        fname = os.path.join(point_cloud_folder, file)
        s3name = f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{fdate}/{instr}/{file}"
        print(f"s3name={s3name}, fname={fname}")
        upload_to_s3(fname, os.environ['OUTPUT_DATA_BUCKET'], s3_name=s3name)
