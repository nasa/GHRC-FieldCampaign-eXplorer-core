import numpy as np
import pandas as pd
import gzip
import json
import os
import boto3
from numpy import deg2rad, rad2deg, cos, sin, tan, arctan
from utils.s3_updnload import upload_to_s3

s3 = boto3.resource('s3')


def LMA_stations(file):
    Stations = pd.DataFrame(columns=['ID', 'Name', 'Lat', 'Lon', 'Alt'])

    obj = s3.Object(os.getenv('RAW_DATA_BUCKET'), file)

    i = 0

    with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
        content = gzipfile.read().decode("utf-8")  # <--convert bytes to string
        lines = content.split('\n')
        for line in lines:
            if ('Number of stations' in line):
                nstn = int(line.split(': ')[1]);
                print('No. of stations:', nstn)
            if ('Coordinate center' in line):
                NWcent = [float(s) for s in (line.split(': ')[1].split())]
            if ('Sta_info:' in line):
                Stations.loc[i] = [line[10:11], line[13:31].strip(), line[31:43], line[43:57], line[57:66]]
                i += 1
            if ('*** data ***' in line):
                break

    if len(Stations) != nstn:  # <--check no. of stations
        print("%WARNING, total stations and no. of stations Don't match!",
              len(Stations), nstn)

    return (Stations, NWcent)


def lambt_inv(unt, lon0, lat0, lat1, lat2, x, y):
    # input : (x,y) in [unit length, i.e. assume radius=1]
    # output: (lonx,laty) in give UNT unit, 'arc' or 'deg'

    PI = np.pi

    if (unt.lower() == 'deg'):
        (lon0, lat0) = deg2rad((lon0, lat0))
        (lat1, lat2) = deg2rad((lat1, lat2))

    val_n = np.log(cos(lat1) / cos(lat2)) / np.log(
        tan(PI / 4 + lat2 / 2) / tan(PI / 4 + lat1 / 2))
    F = cos(lat1) * (tan(PI / 4 + lat1 / 2)) ** val_n / val_n

    roh0 = F / (tan(PI / 4 + lat0 / 2)) ** val_n

    sign_n = -1. if (val_n < 0) else 1

    roh = sign_n * np.sqrt(x * x + (roh0 - y) ** 2.)
    theta = arctan(x / (roh0 - y))

    lonx = lon0 + theta / val_n
    laty = 2. * arctan((F / roh) ** (1. / val_n)) - PI / 2

    if (unt.lower() == 'deg'):
        lonx, laty = rad2deg(lonx), rad2deg(laty)

    return lonx, laty


s3path = f"s3://{os.environ['OUTPUT_DATA_BUCKET']}/{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/LMA_asset/"

s3uri = f"https://{os.environ['OUTPUT_DATA_BUCKET']}.s3-{os.environ['AWS_REGION']}.amazonaws.com/fieldcampaign/goesrplt/"

Networks = ['NA', 'OK', 'WTX', 'CO', 'SO', 'KSC']

fdate = '2017-04-29'  # <--any date we have LMA data for

filesLMA = {}

for nw in Networks:
    files = []
    bucket = s3.Bucket(os.getenv('RAW_DATA_BUCKET'))
    keys = []

    for obj in bucket.objects.filter(
            Prefix=f"fieldcampaign/goesrplt/LMA/{nw}LMA/data/{fdate}/goesr_plt_{nw}LMA_{fdate.replace('-', '')}"):
        keys.append(obj.key)
    result = keys
    print(result)
    if (result != []): files.append(result)
    filesLMA.update({nw: result[0]})  # <---has to use {}.update()

print(filesLMA)

Stations = {}  # ---stations locn (lat,lon,alt)
NWcents = {}  # ---Network center (lat,lon,alt)
for nw in Networks:
    file = filesLMA[nw]
    stations, center = LMA_stations(file)
    stations = stations.astype({"Lat": float, "Lon": float, "Alt": float})
    Stations.update({nw: stations})
    NWcents.update({nw: center})

#######################################
# ----Create CZML file for LMA stations
#######################################
for nw in Networks:

    czmlBody = [
        {"id": "document",
         "name": nw + "LMA",
         "version": "1.0", },
        {'id': nw + ' center',
         'position': {'cartographicDegrees': [NWcents[nw][1], NWcents[nw][0], NWcents[nw][2]], },
         'point': {
             'color': {
                 'rgba': [90, 120, 255, 255], },  # <---blue
             # 'rgba': [90,255, 120, 255],      },  #<---grn
             'pixelSize': 5,
         },
         }
    ]

    # ---for radius
    ranges = [100, 200]
    for ii, radius in enumerate(ranges):
        packet = {
            "id": nw + 'LMA ' + str(radius) + 'km range',
            "polyline": {
                "positions": {"cartographicDegrees": [], },
                "material": {
                    'solidColor': {
                        "color": {"rgba": [90, 120, 255, 200 - ii * 90], }}, },  # <--blu
                # "color": { "rgba": [ 90,255,120, 200-ii*90], } },  }, #<--grn
                "width": 2}
        }

        R = 6.371e3  # in [km]
        r = radius / R
        Lon0, Lat0 = NWcents[nw][1], NWcents[nw][0]
        positions = []
        for i in range(101):
            x = r * cos(i * 2 * np.pi / 100)
            y = r * sin(i * 2 * np.pi / 100)
            lon, lat = lambt_inv('deg', Lon0, Lat0, Lat0 - 5, Lat0 + 5, x, y)
            positions = positions + [lon, lat, 0]
        packet["polyline"]["positions"]["cartographicDegrees"] = positions
        czmlBody.append(packet)

    # ---for stations
    stn = Stations[nw]
    for i in range(len(stn)):
        packet = {
            'id': stn.iloc[i].Name + ' station',
            "model": {
                "show": "true",
                "gltf": s3uri + "LMA_asset/LMASensor.glb",
                "scale": 5,
                "minimumPixelSize": 40,
            },
            'position': {'cartographicDegrees': [stn.iloc[i].Lon, stn.iloc[i].Lat, 0], },
        }
        czmlBody.append(packet)

    LMAczml = json.dumps(czmlBody)

    output_folder = os.getenv('LMA_STATIONS_OUTPUT_PATH')

    filename = f"{output_folder}/{nw}LMA_stations.czml"
    CZMLfile = open(filename, "w")
    CZMLfile.write(LMAczml)
    CZMLfile.close()

os.system(f"aws s3 sync {output_folder} {s3path}")

s3name = f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/LMA_asset/LMASensor.glb"

upload_to_s3(f"{os.environ['CURRENT_DIR']}/assets/LMASensor.glb", os.environ['OUTPUT_DATA_BUCKET'], s3_name=s3name)
