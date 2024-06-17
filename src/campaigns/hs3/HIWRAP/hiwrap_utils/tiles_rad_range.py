import numpy as np
import pandas as pd
import xarray as xr

from typing import Generator

from fcx_playground.fcx_dataprocess.tiles_rad_range import RadRangeTilesPointCloudDataProcess

class HIWRAPTilesPointCloudDataProcess(RadRangeTilesPointCloudDataProcess):
    def _cleaning(self, data: xr.Dataset) -> xr.Dataset:
      # data extraction
      # scrape necessary data columns 
      extracted_data = data[['time', 'ref', 'lat', 'lon', 'alt', 'roll', 'pitch', 'head', 'range']]
      return extracted_data

    def _transformation(self, data: xr.Dataset) -> pd.DataFrame:
      #  transform the data to a suitable data formatting
      secs = data['time'].values # seconds since 2013-09-22T00:00:00Z
      lat = data['lat'].values
      lon = data['lon'].values
      alt = data['alt'].values # altitude of aircraft in meters
      roll = data["roll"].values
      pitch = data["pitch"].values
      head = data["head"].values
      ref = data['ref'].values #CRS radar reflectivity #2d data
      rad_range = data["range"].values # has lower count than ref
      
      # time correction and conversion:
      base_time = np.datetime64('{}-{}-{}'.format("2013", "09", "22")) # all the datapoints are with respect to 2013-02-22 base point. refer documentation
      delta = secs.astype('timedelta64[s]') + base_time
      time = (delta).astype('timedelta64[s]').astype(np.int64)

      # transform ref to 1d array and repeat other columns to match data dimension

      num_col = ref.shape[0] # number of cols
      num_row = ref.shape[1] # number of rows

      time = np.repeat(time, num_row)
      lon = np.repeat(lon, num_row)
      lat = np.repeat(lat, num_row)
      alt = np.repeat(alt, num_row)
      roll = np.repeat(roll * self.to_rad, num_row)
      pitch = np.repeat(pitch * self.to_rad, num_row)
      head = np.repeat(head * self.to_rad, num_row)
      rad_range = np.tile(rad_range, num_col)
      ref = ref.flatten()

      # curtain creation

      x, y, z = self._down_vector(roll, pitch, head)
      x = np.multiply(x, np.divide(rad_range, 111000 * np.cos(lat * self.to_rad)))
      y = np.multiply(y, np.divide(rad_range, 111000))
      z = np.multiply(z, rad_range)
      lon = np.add(-x, lon)
      lat = np.add(-y, lat)
      alt = np.add(z, alt)

      # sort by time

      sort_idx = np.argsort(time)
      lon = lon[sort_idx]
      lat = lat[sort_idx]
      alt = alt[sort_idx]
      ref = ref[sort_idx]
      time = time[sort_idx]

      # remove nan and infinite using mask (dont use masks filtering for values used for curtain creation)

      mask = np.logical_and(np.isfinite(ref), alt > 0)
      time = time[mask]
      ref = ref[mask]
      lon = lon[mask]
      lat = lat[mask]
      alt = alt[mask]

      # sample one data point for each 20 data points. done as the file has large data rows.
      df = pd.DataFrame(data = {
        'time': time[::20],
        'lon': lon[::20],
        'lat': lat[::20],
        'alt': alt[::20],
        'ref': ref[::20]
      })
      return df

    def _get_date_from_url(self, url: str) -> np.datetime64:
      # get date from url
      # date is in the format of YYYYMMDD
      # eg. 20190801
      date = url.split("HS3_HIWRAP_")[1].split("_")[0]
      np_date = np.datetime64('{}-{}-{}'.format(date[:4], date[4:6], date[6:]))
      return np_date
    
    
def get_date_from_url(url: str) -> np.datetime64:
  # get date from url
  # date is in the format of YYYYMMDD
  # eg. 20190801
  date = url.split("HS3_HIWRAP_")[1].split("_")[0]
  np_date = np.datetime64('{}{}{}'.format(date[:4], date[4:6], date[6:]))
  return np_date