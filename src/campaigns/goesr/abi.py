from utils.utils import mkfolder, s3_key_exists
from mk_gdaltif import makeGeoTiff
import os, re, glob
import terracotta as tc
import tqdm
import boto3.session

s3 = boto3.resource('s3')

session = boto3.session.Session()

DB_NAME =  "abi_allflights.sqlite"

if os.path.isfile(DB_NAME):
    os.remove(DB_NAME)

NAME_PATTERN = r'(?P<band>\w{3})_(?P<time>\d{9}).tif'

KEYS = ('band', 'time')

KEY_DESCRIPTIONS = {
    'band': 'Band or index name',
    'time': 'fromIso8601("2000-01-01T12:00:00Z")',
}
driver = tc.get_driver(DB_NAME)

if not os.path.isfile(DB_NAME):
    driver.create(KEYS, KEY_DESCRIPTIONS)

assert driver.key_names == KEYS

dates = ['2017-04-18', '2017-04-20', '2017-04-22', '2017-05-07',
         '2017-05-08', '2017-05-12', '2017-05-14', '2017-05-17']

s3bucket = os.environ['OUTPUT_DATA_BUCKET']

def uploadGeoTiff():
    for fdate in dates:
        os.environ['FLIGHT_DATE'] = fdate
        os.environ['ABI_S3_KEY'] = f"fieldcampaign/goesrplt/ABI/data/{os.environ['FLIGHT_DATE']}/C13"

        os.environ[
            'ABI_INPUT_FLIGHT_PATH'] = f"{os.environ['ABI_INPUT_PATH']}/{os.environ['FLIGHT_DATE']}/C13"
        os.environ['ABI_OUTPUT_FLIGHT_PATH'] = f"{os.environ['ABI_OUTPUT_PATH']}/{os.environ['FLIGHT_DATE']}"

        os.environ[
            'ABI_S3_OUTPUT_KEY'] = f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{os.environ['FLIGHT_DATE']}/abi_tif"

        terracotta_path = f"{os.environ['ABI_OUTPUT_FLIGHT_PATH']}/terracotta"

        mkfolder(terracotta_path)
        print(f"terracotta_path={terracotta_path}")

        makeGeoTiff()

        os.system(
            f"terracotta optimize-rasters --compression deflate -o {terracotta_path} {os.environ['ABI_OUTPUT_FLIGHT_PATH']}/*.tif")

        rasterFolder = os.environ['ABI_S3_OUTPUT_KEY']

        s3_path = f's3://{s3bucket}/{rasterFolder}'

        print(f"s3_path={s3_path}")

        available_datasets = driver.get_datasets()
        raster_files = list(glob.glob(f"{terracotta_path}/*.tif"))
        pbar = tqdm.tqdm(raster_files)

        for raster_path in pbar:
            print(f"raster_path = {raster_path}")

            pbar.set_postfix(file=raster_path)

            raster_filename = os.path.basename(raster_path)

            match = re.match(NAME_PATTERN, raster_filename)
            if match is None:
                raise ValueError(f'Input file {raster_filename} does not match raster pattern')

            keys = match.groups()

            if keys in available_datasets:
                continue

            with driver.connect():
                file_uploaded = s3_key_exists(session.client('s3'), s3bucket, f'{rasterFolder}/{raster_filename}')
                if not file_uploaded:
                    # since the rasters will be served from S3, we need to pass the correct remote path
                    driver.insert(keys, raster_path, override_path=f'{s3_path}/{raster_filename}')
                    print(f"uploadding {rasterFolder}/{raster_filename}")
                    s3.meta.client.upload_file(raster_path, s3bucket,f'{rasterFolder}/{raster_filename}')

    # upload database to S3
    s3.meta.client.upload_file(DB_NAME, s3bucket,
                               f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{DB_NAME}")


uploadGeoTiff()





