import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset
from osgeo import gdal, osr
from gdalconst import *
from utils.s3_updnload import downloadBatch_s3
from utils.utils import mkfolder
import time as t
import glob

# =====Below adopted from remap.py
# Define KM_PER_DEGREE
KM_PER_DEGREE = 111.32

# GOES-R Extent (satellite projection) [llx, lly, urx, ury]
GOESr_fullEXTENT = [-5434894.885056, -5434894.885056, 5434894.885056, 5434894.885056]
GOESr_usEXTENT = [-2685383.084, 1523053.138, 2324660.158, 4529079.163]

# GOES-R Spatial Reference System (GOES-16 lon_0=-75.2)
sourcePrj = osr.SpatialReference()
sourcePrj.ImportFromProj4(
    '+proj=geos +h=35786023.0 +a=6378137.0 +b=6356752.31414 +f=0.00335281068119356027 +lat_0=0.0 +lon_0=-89.5 +sweep=x +no_defs')

# Lat/lon WSG84 Spatial Reference System
targetPrj = osr.SpatialReference()
targetPrj.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')


def ZtoT2K(UTC, form=None):
    """
    Get get seconds fromIso8601('2000-01-01T12:00:00Z')
    if UTC is a string, give the time format
    otherwise UTC is a datetime() data type
    """
    if form:
        Z = datetime.strptime(UTC, form)
        return (Z - datetime(2000, 1, 1, 12)).total_seconds()
    return (UTC - datetime(2000, 1, 1, 12)).total_seconds()


def T2KtoZ(secs, form=None):
    """ISO standard format would be '%Y-%m-%dT%H:%M:%SZ'"""
    dytim = datetime(2000, 1, 1, 12) + timedelta(seconds=secs)
    if form:
        return dytim.strftime(form)  # <--return  string if format given
    return dytim  # <--return datetime()


def exportImage(image, path):
    driver = gdal.GetDriverByName('netCDF')
    return driver.CreateCopy(path, image, 0)


def getGeoT(extent, nlines, ncols):
    # Compute resolution based on data dimension
    resx = (extent[2] - extent[0]) / ncols
    resy = (extent[3] - extent[1]) / nlines
    return [extent[0], resx, 0, extent[3], 0, -resy]


def getScaleOffset(path, vname):
    """Use netCDF4 """
    nc = Dataset(path, mode='r')
    scale = nc.variables[vname].scale_factor
    offset = nc.variables[vname].add_offset
    nc.close()
    return scale, offset


def flipLat(arr):
    return np.flipud(arr)


def rescale(arr, notVal, lower, upper, newRange):
    print(lower, upper, arr[arr != notVal].min(), arr.max())
    arr[arr > upper] = notVal

    b = arr[arr != notVal]
    arr[arr != notVal] = ((b - lower) / (upper - lower)) * (newRange[1] - newRange[0]) + newRange[0]
    arr[arr > np.max(newRange)] = np.max(newRange)
    arr[arr < np.min(newRange)] = np.min(newRange)
    arr = arr.astype(np.uint8)
    print(lower, upper, arr.min(), arr.max())
    return arr


def inRaster(path, vname, disk='full', verb=False):
    # Open a dataset/variable in a NetCDF file (GOES-R data)
    connectionInfo = 'NETCDF:' + path + ':' + vname
    raw = gdal.Open(connectionInfo, gdal.GA_ReadOnly)

    # Setup projection and geo-transformation for GOES-16
    if (disk == 'full'):
        GOES_EXTENT = GOESr_fullEXTENT
    elif (disk == 'conus'):
        GOES_EXTENT = GOESr_usEXTENT

    raw.SetProjection(sourcePrj.ExportToWkt())
    raw.SetGeoTransform(getGeoT(GOES_EXTENT, raw.RasterYSize, raw.RasterXSize))

    arr = raw.ReadAsArray()
    arr = flipLat(arr)
    ny, nx = arr.shape
    driver = gdal.GetDriverByName("GTiff")
    flipped = driver.Create('tmp.tif', nx, ny, 1, gdal.GDT_Float32)
    flipped.SetGeoTransform(raw.GetGeoTransform())  ##sets same geotransform as input
    flipped.SetProjection(raw.GetProjection())  ##sets same projection as input
    flipped.GetRasterBand(1).WriteArray(arr)
    flipped.GetRasterBand(1).SetNoDataValue(0)  ##if you want these values transparent

    raw = None
    arr = None

    # Read scale/offset from file
    scale, offset = getScaleOffset(path, vname)
    if (verb): print(' scale,offset:', scale, offset)

    return flipped, scale, offset


def outRaster(raw, scale, offset, setRange, extent, resolution, verb=False):
    # Compute grid dimension
    sizex = int(((extent[2] - extent[0]) * KM_PER_DEGREE) / resolution)
    sizey = int(((extent[3] - extent[1]) * KM_PER_DEGREE) / resolution)

    # Create re-gridded grid [retangular (lat,lon)]
    memDriver = gdal.GetDriverByName('MEM')
    # grid = memDriver.Create('grid', sizex, sizey, 1, gdal.GDT_Float32)
    grid = memDriver.Create('grid', sizex, sizey, 1, gdal.GDT_Float32)

    # Setup projection and geo-transformation
    grid.SetProjection(targetPrj.ExportToWkt())
    grid.SetGeoTransform(getGeoT(extent, grid.RasterYSize, grid.RasterXSize))

    # Perform the projection/resampling/re-gridding
    gdal.ReprojectImage(raw, grid, sourcePrj.ExportToWkt(), targetPrj.ExportToWkt(),
                        gdal.GRA_NearestNeighbour, options=['NUM_THREADS=ALL_CPUS'])

    arr = grid.ReadAsArray()
    arr = arr * scale + offset
    arr[arr < 0] = 0  # <--those are likely NOT in orig domain, set to NoDataValue
    arr = rescale(arr, 0, 8, 80, setRange)

    grid.GetRasterBand(1).SetNoDataValue(0)
    grid.GetRasterBand(1).WriteArray(arr)

    return grid


##################################################################
# ---Run gdal to make geotiff (no optimizer)

def makeGeoTiff():
    s3bucket = os.getenv('RAW_DATA_BUCKET')
    fdate = os.getenv('FLIGHT_DATE')
    input_folder = os.getenv('ABI_INPUT_FLIGHT_PATH')
    output_folder = os.getenv('ABI_OUTPUT_FLIGHT_PATH')

    mkfolder(input_folder)
    mkfolder(output_folder)

    downloadBatch_s3(s3bucket, os.getenv('ABI_S3_KEY'), input_folder)

    # Choose the visualization extent (min lon, min lat, max lon, max lat)
    extent = [-140.6162904788845, 14.000163292174229, -49.179274701919105, 52.76771749693075]

    # Choose the image resolution (the higher the number the faster the processing is)
    resolution = 4.0

    flist = glob.glob(input_folder + "/*")
    for filenc in flist:
        tstart = filenc.split('M3C13_G16_s')[-1][0:13]
        secs = ZtoT2K(tstart, '%Y%j%H%M%S')
        tstamp = T2KtoZ(secs, '%Y-%m-%dT%H:%M:%SZ')
        t_in_sec = str(int(secs))

        tifFile = output_folder + "/C13_" + t_in_sec + ".tif"
        if not os.path.isfile(tifFile):
            print(tstamp, ' is timestamp for ', t_in_sec, tstart)
            print('Remapping', filenc.split('/')[-1])
            start = t.time()

            raw, scale, offset = inRaster(filenc, 'Rad', disk='conus', verb=False)
            grid = outRaster(raw, scale, offset, [255, 0], extent, resolution, verb=False)

            print('- finished! Time:', t.time() - start, 'seconds')

            dst_ds = gdal.Translate(tifFile, grid, outputType=gdal.GDT_Byte)
            dst_ds.GetRasterBand(1).SetNoDataValue(0)

            raw = None  # <-- Close file/dataset
            grid = None
            dst_ds = None
        else:
            pass
            # print(f'file {tifFile} already exists')
