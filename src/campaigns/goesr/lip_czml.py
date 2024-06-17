import pandas as pd
import json, glob, os
from datetime import datetime, timedelta
from utils.ingest_utils import mkfolder
from utils.s3_updnload import download_s3, upload_to_s3


########################################################
# LIP polyline czml
# Both I/O are in local disc as preprocessing is needed
########################################################

def makeCZML(fdate):
    s3bucket = os.getenv('RAW_DATA_BUCKET')
    LIPpath = os.getenv('LIP_INPUT_PATH')

    sdate = fdate.replace('-', '')

    s3key = 'fieldcampaign/goesrplt/LIP/data/goesr_plt_lip_' + sdate + '.txt'
    download_s3(s3bucket, s3key, LIPpath)

    ############################################
    # Pre-process LIP data, get rid of "NaN"
    ############################################
    fileLIP = glob.glob(LIPpath + '/goesr_plt_lip_*' + sdate + '*')[0]
    fileLIP2 = fileLIP.split(".")[0] + "_valid.txt"

    with open(fileLIP, "r") as f:
        lines = f.readlines()
    with open(fileLIP2, "w") as f:
        for line in lines:
            if ("NaN" not in line):
                f.write(line)

    ############################################
    # Process LIP data
    ############################################

    df = pd.read_csv(fileLIP2, sep=",", header=None, usecols=[0, 1, 2, 3, 4, 5, 6, 7])
    df.columns = ['Date/Time', 'Ex', 'Ey', 'Ez', 'Etot', 'lat', 'lon', 'alt']

    df['Date'] = [a.split(' ')[0] for a in df['Date/Time']]
    df['Time0'] = [a.split(' ')[1] for a in df['Date/Time']]
    df['Time'] = [a.split('.')[0] for a in df['Time0']]
    df = df.drop(columns=['Date/Time', 'Time0'])

    df = df.groupby(['Time', 'Date'], as_index=False).agg({'Ex': 'mean', 'Ey': 'mean', 'Ez': 'mean', 'Etot': 'mean',
                                                           'lat': 'mean', 'lon': 'mean', 'alt': 'mean'})

    # ---display would last 60 sec
    tform1 = '%Y-%m-%d %H:%M:%S'
    tform2 = '%Y-%m-%dT%H:%M:%SZ'
    df['time2'] = [(datetime.strptime(d + ' ' + h, tform1) +
                    timedelta(seconds=60)).strftime(tform2)
                   for d, h in zip(df['Date'], df['Time'])]  # <--display lasting time

    df = df.reset_index(drop=True)

    #######################################################
    # Making czml file
    #  draw vectors propotional to (ex,ey,ez)
    # Note that 'alt' not used, as display set to ground
    #######################################################
    czmlBody = [{"id": "document",
                 "name": "LIP",
                 "version": "1.0", }]

    LIP = df[['Date', 'Time', 'time2',
              'Ex', 'Ey', 'Ez', 'lat', 'lon']]

    for d, t, t2, ex, ey, ez, lat, lon in zip(LIP.Date, LIP.Time, LIP.time2,
                                              LIP.Ex, LIP.Ey, LIP.Ez,
                                              LIP.lat, LIP.lon):
        xb = ex * .05
        yb = ey * .05
        zb = ez * 2000
        packet = {
            'id': t,
            'availability': d + 'T' + t + 'Z/' + t2,
            'polyline': {
                'positions': {'cartographicDegrees': [lon, lat, 0,
                                                      lon + xb, lat + yb, 0 + zb]},
                'material': {
                    'polylineArrow': {
                        'color': {'rgba': [255, 55, 55, 255], }, }, },
                'width': 5}
        }

        czmlBody.append(packet)

    folder = f"{os.getenv('LIP_OUTPUT_PATH')}/{fdate}"
    mkfolder(folder)

    LIPczml = json.dumps(czmlBody)

    filename = "LIP.czml"
    filepath = f"{folder}/{filename}"

    CZMLfile = open(filepath, "w")
    CZMLfile.write(LIPczml)
    CZMLfile.close()

    instr = "lip"
    s3name = f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{fdate}/{instr}/{filename}"
    print(f"s3name={s3name}, filename={filepath}")
    upload_to_s3(filepath, os.environ['OUTPUT_DATA_BUCKET'], s3_name=s3name)


dates = ['2017-04-11', '2017-04-13', '2017-04-16', '2017-04-18', '2017-04-20', '2017-04-22', '2017-05-07',
         '2017-05-08', '2017-05-12', '2017-05-14', '2017-05-17']

for fdate in dates:
    makeCZML(fdate)
