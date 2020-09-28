import numpy as np
import pandas as pd
import xarray as xr
import glob, os, io
import gzip
import boto3, s3fs
from boto3 import client as boto_client
from datetime import date, time, datetime, timedelta

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


def CRSaccess(fname, s3bucket=False, Verb=False):
    """
    Access the CRS file
    Return CRS filename with path (absolute path) for "local" access
    Return CRS data as object for "cloud access"
    Either way, the return value can be open by Xarray as netcdf file object
    """

    print("\%% Accessing data from Cloud. This may take a little time...\n")
    s3 = boto_client('s3', region_name=os.environ['AWS_REGION'])
    fileobj = s3.get_object(Bucket=s3bucket, Key=fname)
    fileCRS = fileobj['Body'].read()

    return fileCRS


def get_CRS(fdate, s3bucket):
    """ Get CRS data
    call the following functions:
     CRSaccess()
     add24hr()
    """
    fname = 'fieldcampaign/goesrplt/CRS/data/GOESR_CRS_L1B_' + fdate.replace('-', '') + '_v0.nc'
    fileCRS = CRSaccess(fname, s3bucket=s3bucket)
    with xr.open_dataset(fileCRS, decode_cf=False) as ds:
        CRSlat = ds['lat'].values
        CRSlon = ds['lon'].values
        hr = add24hr(ds['time'].values)
        Time = (hr * 3600).astype('timedelta64[s]') + np.datetime64(fdate)
        CRStime = (Time - np.datetime64('1970-01-01')).astype('timedelta64[s]').astype(np.int64)
    return CRSlat, CRSlon, CRStime, Time


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


def LMAfiles(bucket, fdate, tstart, Trange, network='OKLMA'):
    """
    get LMA filename list within Trange [sec] starting from tstart on fdate
    Note that LMA files are every 10min/1hr
    """
    filesLMA = []

    files = S3list(bucket, fdate, 'LMA', network=network)
    print('No. of LMA files for ', fdate, len(files))
    if (len(files) == 0):
        return filesLMA

    for t in range(0, Trange, 600):  # 1):
        ss = (tstart + timedelta(seconds=t)).strftime("%Y%m%d_%H%M%S")
        if (network == 'NALMA'): ss = (tstart + timedelta(seconds=t)).strftime("%Y%m%d_%H")
        for file in files:
            if (ss in file and '.dat' in file and file not in filesLMA):
                filesLMA.append(file)

    print("no. of LMA found for selected period: ", len(filesLMA))
    return filesLMA


def get_LMAheader(bucket, file, slabel='*** data ***'):
    if (bucket == 'local'):
        lines = open(file, "r")
    else:
        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, file)
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
            content = gzipfile.read().decode("utf-8")  # <--convert bytes to string
            lines = content.split('\n')

    n = 1;
    nheader = 0
    for line in lines:
        if (slabel in line):
            nheader = n
            break
        n = n + 1

    if (nheader == 0):
        print("%%Can't find where data starts.",
              "\n%%GO Check start indicator! Is it '%s'?" % slabel)
        return -1

    if (bucket == 'local'): lines.close()
    return nheader


def get_LMA(bucket, file, stns_min=7, nheader=None):
    """
    1. Read LMA data with header excluded
    2. Add column of no. of stations that detected lightning
    3. Cleanse data by filtering out noise
    Note that
     a. nheader: header rows to skip (upon meeting w/ slabel)
     b. column "mask" is hexadecimal, converted to binary for the stations that detected lightning
     c. column "Nstns" added to count no. of stations that detected lightning 
    """
    if (not nheader):
        nheader = get_LMAheader(bucket, file, slabel='*** data ***')

    if (bucket == 'local'):
        DF = pd.read_csv(file, names=['Time', 'Lat', 'Lon', 'Alt', 'chi^2', 'dBW', 'mask'],
                         sep=r"\s+", index_col=None, skiprows=nheader, header=None,
                         compression='gzip')

    else:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=file)
        DF = pd.read_csv(io.BytesIO(obj['Body'].read()),
                         names=['Time', 'Lat', 'Lon', 'Alt', 'chi^2', 'dBW', 'mask'],
                         sep=r"\s+", index_col=None, skiprows=nheader, header=None,
                         compression='gzip')

    ba = [bin(int(a, 16))[2:] for a in DF['mask']]  # <--hexa to bin in str
    DF['Nstns'] = [(sum([int(i) for i in a])) for a in ba]

    # Noise reduction: chi^2 < 1, minimum no. of stations detected lightning
    # print('No. of raw data:',len(DF))
    DF = DF[(DF['chi^2'] < 1) & (DF['Nstns'] >= stns_min)]
    DF.index = range(len(DF))
    DF = DF.drop(['chi^2', 'mask'], 1)
    # print('No. of filtered data:',len(DF))

    return DF, nheader


def LISfiles(s3bucket, fdate, bigbox, start, end, Verb=False):
    """
    Get LIS filename list within range [sec] starting from start to end on fdate
    and within the bigbox domain
    """
    lonW, latS, lonE, latN = bigbox[0:4]
    filesLIS = []

    print("This will take some time....")
    filesALL = S3list(s3bucket, fdate, 'LIS')
    files = [file for file in filesALL if file.split('.')[-1] == 'nc']
    if (Verb): print('No. of all LIS .nc files: ', len(files))

    if (Verb): print("Searching between {} and {}".format(start, end))
    nf = 0
    for file in files:
        fileobj = s3FileObj(s3bucket, file, verb=False)

        ds = xr.open_dataset(fileobj, engine='h5netcdf')
        try:
            nflash = len(ds['flash_dim'].values)
        except KeyError:
            print('%% %s has NO lightning data %%' % file.split('/')[-1])
            continue

        lat, lon, Time = get_LIS('area', ds, bigbox)
        ds.close()

        mask1 = np.where((Time > start) & (Time < end))
        if (len(mask1[0]) > 0):
            mask2 = np.where(
                (lon > lonW) & (lon < lonE) & (lat > latS) & (lat < latN) & (Time > start) & (Time < end))
            if (len(mask2[0]) > 0):
                print("no. of flashes: ", len(mask2[0]))
                nf = nf + 1
                filesLIS.append(file)
                if (Verb): print(file)
        else:
            if (Verb): print("{}-{} not in bounds".format(Time[0], Time[-1]))
            if (nf > 0): break  # <--if prev file in range and this one is not, later ones won't be

    print("No. of LIS files found in domain/range: ", len(filesLIS), nf)
    return filesLIS


def get_LIS(ltype, ds, bigbox):
    """
    get GLM data for point cloud
    Note that Secs is minisecond offset from the file time in the .nc file
    *Using xarray, TAI is automatically converted to np.datetime64[ns]
    *Do not use xr.open_mfdataset. Dimensions in LIS files are not fixed. 
    """
    Lat = ds['lightning_' + ltype + '_lat'].values
    Lon = ds['lightning_' + ltype + '_lon'].values
    TAI = ds['lightning_' + ltype + '_TAI93_time'].values  # <--as datetime64[ns]
    Time = (TAI - np.datetime64('1970-01-01')).astype('timedelta64[s]').astype(np.int64)
    if (ltype == 'area'): return Lat, Lon, Time

    Rad = ds['lightning_' + ltype + '_radiance'].values  # <--"area" doesn't have _radiance

    print('Size of Lon, Time:', len(Lon), len(Time))
    return Lat, Lon, Time, Rad


def matchPatt(Pattern, files, nfile=1):
    """
    Find a file(s) with matching pattern (of date/time) among files
    Return:
    1. pattern matching file(s)
    2. shortened file list starting from the file found
       (so search can start from here next time around)
    """
    result = [];
    ii = None
    for i, fn in enumerate(files):
        if (Pattern in fn):
            if (nfile == 1):  # <--return ONLY the 1st file found
                return fn, files[i:]
            result.append(fn)
            ii = i
    return result, files[ii:]


def GLMfiles(s3bucket, fdate, tstart, Trange, files=None):
    """
    get GLM filename list within Trange [sec] starting from tstart on fdate
    Note that GLM files are every 20 sec
    """
    filesGLM = []

    print("This will take some time....")
    if (files is None): files = S3list(s3bucket, fdate, 'GLM')

    for t in range(0, Trange, 20):  # 1):
        ss = (tstart + timedelta(seconds=t)).strftime("s%Y%j%H%M%S")
        result, newlist = matchPatt(ss, files, 1)
        if (result != []):
            filesGLM.append(result)
            files = newlist

    print("no. of GLM found and the rest", len(filesGLM), len(files))
    return filesGLM, files


def get_GLM(ltype, ds):
    """
    get GLM data for point cloud
    Note that Secs is minisecond offset from the file time in the .nc file
    *Using xarray, Secs is automatically converted to np.datetime64[ns]
    *Do not use xr.open_mfdataset. Dimensions in GLM files are not fixed. 
    """

    if (ltype == 'event'):
        Secs = ds['event_time_offset'].values
    elif (ltype == 'group'):
        Secs = ds['group_time_offset'].values
    elif (ltype == 'flash'):
        Secs = ds['flash_time_offset_of_first_event'].values
    else:
        print('Duh!')

    # Time=np.array([sec.tolist()+sec00 for sec in Sec2])
    Time = (Secs - np.datetime64('1970-01-01')).astype('timedelta64[s]').astype(np.int64)
    Lat = ds[ltype + '_lat'].values
    Lon = ds[ltype + '_lon'].values
    Rad = ds[ltype + '_energy'].values
    return Lat, Lon, Time, Rad
