import numpy as np
import pandas as pd
import xarray as xr
import glob, os, io
import gzip
import boto3, s3fs
from boto3 import client as boto_client
from datetime import date, time, datetime, timedelta
import shutil
from pathlib import Path


to_rad = np.pi / 180.0
to_deg = 180.0 / np.pi


def regionrad(region):
    return [r * to_rad for r in region]


def sec2Z(t):
    return "{}Z".format(datetime.utcfromtimestamp(t).isoformat())


def DateTime(Sec):
    return (Sec.astype('timedelta64[s]') + np.datetime64('1970-01-01'))


def mkfolder(folder):
    if (not os.path.exists(folder)):
        try:
            os.makedirs(folder)
            print('Success to create folder %s' % folder)
        except OSError:
            print('Failed to create folder %s' % folder)
            quit()
    else:
        print('%s already exists' % folder)


def add24hr(hr):
    """Correction of time in CRS for going over the next day in UTC"""
    b = np.where(hr < hr[0])
    hr[b] = hr[b] + 24
    return hr


def nc_access(fname, s3bucket=False, Verb=False):
    """
    Access the CRS file
    Return CRS filename with path (absolute path) for "local" access
    Return CRS data as object for "cloud access"
    Either way, the return value can be open by Xarray as netcdf file object
    """

    print("\%% Accessing data from Cloud. This may take a little time...\n")
    s3 = boto_client('s3')
    fileobj = s3.get_object(Bucket=s3bucket, Key=fname)
    fileCRS = fileobj['Body'].read()

    return fileCRS

def downloadFromS3(bucket_name, s3_key, dest_dir):
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

def S3list(s3bucket, fdate, instrm, network='OKLMA'):
    """
    get list of files in a s3 bucket for a specific fdate and instrument (prefix)
    fdate: e.g. '2017-05-17'
    instrm: e.g. 'GLM'
    """
    prefix = {'GLM': 'fieldcampaign/goesrplt/GLM/data/L2/' + fdate + '/OR_GLM-L2-LCFA_G16',
              'LIS': 'fieldcampaign/goesrplt/ISS_LIS/data/' + fdate + '/ISS_LIS_SC_V1.0_',
              # 'FEGS': 'fieldcampaign/goesrplt/FEGS/data/goesr_plt_FEGS_' + fdate.replace('-', '') + '_Flash',
              'CRS': 'fieldcampaign/goesrplt/CRS/data/GOESR_CRS_L1B_' + fdate.replace('-', ''),
              'NAV': 'fieldcampaign/goesrplt/NAV_ER2/data/goesrplt_naver2_IWG1_' + fdate.replace('-', ''),
              'LMA': 'fieldcampaign/goesrplt/LMA/' + network + '/data/' + fdate + '/goesr_plt_' + network + '_' + fdate.replace(
                  '-', '')}

    print("S3list searching for ", prefix[instrm])

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3bucket)
    keys = []
    for obj in bucket.objects.filter(Prefix=prefix[instrm]):
        keys.append(obj.key)
    return keys


def s3FileObj(s3bucket, fname, verb=False):
    """
    Return S3 file object to be accessed using xarray or hdf5/netcdf4/txt/csv.
    """
    if (verb): print(f"\%% Accessing {fname.split('/')[-1]} from Cloud...")

    file = s3bucket + '/' + fname
    fs = s3fs.S3FileSystem()  # (anon=True)--> access public buckets
    fileObj = fs.open(file)

    return fileObj
