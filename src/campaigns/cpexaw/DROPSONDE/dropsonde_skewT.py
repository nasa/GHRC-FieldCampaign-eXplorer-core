import os
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import metpy.calc as mpcalc
from metpy.plots import SkewT
from metpy.units import units

import boto3
from boto3 import client as boto_client
from botocore.exceptions import ClientError, NoCredentialsError

class DropsondeSkewT:
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
      s3.upload_file(source_file_path, bucket_name, prefix)
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
    with xr.open_dataset(file, decode_cf=False) as ds:
        rh = ds['rh'].values # relative humidity
        dp = ds['dp'].values # dew point
        tdry = ds['tdry'].values # temp
        lat = ds['lat'].values
        lon = ds['lon'].values
        alt = ds['alt'].values
        time = ds['time'].values
        pressure = ds['pres'].values
        u_wind = ds['u_wind'].values
        v_wind = ds['v_wind'].values

    ## Data formation
    
    #1. sort data by time
    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    time = time[sort_idx]
    rh = rh[sort_idx]
    dp = dp[sort_idx]
    tdry = tdry[sort_idx]
    pressure = pressure[sort_idx]
    u_wind = u_wind[sort_idx]
    v_wind = v_wind[sort_idx]

    #2. remove nan and infinite and invalid values using mask
    mask = np.logical_and(alt != -999.0, lon != -999.0, lat != -999.0)
    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    time = time[mask]
    rh = rh[mask]
    dp = dp[mask]
    tdry = tdry[mask]
    pressure = pressure[mask]
    u_wind = u_wind[mask]
    v_wind = v_wind[mask]

    # contd. remove nan and infinite and invalid values using mask
    mask = np.logical_and(rh > -100, rh > -100)
    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    time = time[mask]
    rh = rh[mask]
    dp = dp[mask]
    tdry = tdry[mask]
    pressure = pressure[mask]
    u_wind = u_wind[mask]
    v_wind = v_wind[mask]
    
    return (lon, lat, alt, time, rh, dp, tdry, pressure, u_wind, v_wind)
  
  def generate_skewT(self, file_path, height, pressure, temperature, dewpoint, u_wind, v_wind):
    df = pd.DataFrame(dict(zip(('height','pressure','temperature','dewpoint','u_wind','v_wind'),(height, pressure, temperature, dewpoint, u_wind, v_wind))))

    # Drop any rows with all NaN values for T, Td, winds
    df = df.dropna(subset=('height','pressure','temperature','dewpoint','u_wind','v_wind', 
                          ), how='all').reset_index(drop=True)
    P = df['pressure'].values * units.hPa
    T = df['temperature'].values * units.degC
    Td = df['dewpoint'].values * units.degC
    
    # Change default to be better for skew-T
    plt.rcParams['figure.figsize'] = (9, 9)
    
    skew = SkewT()

    # Plot the data using normal plotting functions, in this case using
    # log scaling in Y, as dictated by the typical meteorological plot
    skew.plot(P, T, 'r')
    skew.plot(P, Td, 'g')
    # # Set some better labels than the default
    skew.ax.set_xlabel('Temperature (\N{DEGREE CELSIUS})')
    skew.ax.set_ylabel('Pressure (mb)')

    ## for barbs
    # Set spacing interval--Every 50 mb from 1000 to 100 mb
    my_interval = np.arange(100, 1000, 50) * units('mbar')
    # Get indexes of values closest to defined interval
    ix = mpcalc.resample_nn_1d(P, my_interval)
    skew.plot_barbs(P[ix], u_wind[ix], v_wind[ix])

    # Add the relevant special lines
    skew.plot_dry_adiabats()
    skew.plot_moist_adiabats()
    skew.plot_mixing_lines()
    skew.ax.set_ylim(1000, 100)

    plt.savefig(file_path)
    plt.close()


def main():
  ds = DropsondeSkewT()
  s3_url_list = ds.get_files()
  for s3_url in s3_url_list:
    try:
      print("Generating skewT for: ", s3_url)
      data = ds.data_reader(s3_url)
      (lon, lat, alt, time, rh, dp, tdry, pressure, u_wind, v_wind) = data
      name = s3_url.split('/')[-1]
      date = name.split('_D')[1].split('_')[0]
      time = name.split('_D')[1].split('_')[1]
      # create dir for stroing skewT images
      path = r'/tmp/dropsonde/output/skewT/' + date
      if not os.path.exists(path):
        os.makedirs(path)
      # generaete skewT
      ds.generate_skewT(f"{path}/{name}.png", alt, pressure, tdry, dp, u_wind, v_wind)
      # upload the generated skewT
      ds.upload_file(f"{path}/{name}.png", bucket_name="ghrc-fcx-field-campaigns-szg", prefix=f"CPEX-AW/instrument-processed-data/dropsonde/skewT/{date}/dropsonde-{time}.png")
      print("Generated skewT for: ", s3_url)
    except Exception as e:
      print("Error during conversion for: ", s3_url, ". Error on", e)
  print("Done!")
    
main()