import pandas as pd
import json, os
from datetime import datetime, timedelta
from utils.ingest_utils import s3FileObj, mkfolder
from utils.s3_updnload import upload_to_s3
from utils.utils import s3_key_exists
import boto3

s3 = boto3.resource('s3')

session = boto3.session.Session()

def makeCZML(fdate):

    s3bucket = os.getenv('RAW_DATA_BUCKET')  # s3bucket if input is in "cloud"

    sdate = fdate.split('-')[0] + fdate.split('-')[1] + fdate.split('-')[2]
    ltype = 'Flash'  # 'Pulse'  #'Flash'

    # ----FEGS flash data and along flight track
    # GPS time was zero at 0h 6-Jan-1980 and since it is not perturbed by leap seconds,
    # GPS in 2012 is ahead of UTC by 18 seconds.
    GPSsec0517 = 1179014418  # <--GPSsec for 2017,05,17 00UTC
    t0517 = datetime(2017, 5, 17)
    t1 = datetime(int(sdate[0:4]), int(sdate[4:6]), int(sdate[6:8]))
    diff = (t1 - t0517).total_seconds()
    GPSsec0 = GPSsec0517 + diff

    if (ltype == 'Flash'):
        typeID = 'FlashID'

    elif (ltype == 'Pulse'):
        typeID = 'PulseID'

    file = 'fieldcampaign/goesrplt/FEGS/data/goesr_plt_FEGS_' + fdate.replace('-', '') + '_Flash_v2.txt'

    file_exists = s3_key_exists(session.client('s3'), s3bucket, file)

    if file_exists is False: return

    print(f" file_exists={file_exists} {file}")

    fileFEGS = s3FileObj(s3bucket, file, verb=False)

    DF = pd.read_csv(fileFEGS, sep=",", index_col=None, usecols=
    [typeID, 'GPSstart', 'SUBstart', 'GPSend', 'SUBend',
     'lat', 'lon', 'alt', 'energy', 'FOVlat1', 'FOVlon1', 'FOVlat2',
     'FOVlon2', 'FOVlat3', 'FOVlon3', 'FOVlat4', 'FOVlon4'])

    DF['secs'] = (DF['GPSstart'] + DF['SUBstart'] - GPSsec0).astype(int)

    # ---display would last for 60 sec
    tform1 = '%Y-%m-%d %H:%M:%S'
    tform2 = '%Y-%m-%dT%H:%M:%SZ'
    tform3 = 'T%H:%M:%SZ'
    time1 = [datetime(2017, int(sdate[4:6]), int(sdate[6:8])) + timedelta(seconds=s) for s in DF['secs']]
    time2 = [t + timedelta(seconds=60) for t in time1]

    DF['Time'] = [t1.strftime(tform2) + '/' + t2.strftime(tform2) for t1, t2 in zip(time1, time2)]

    ####################################
    # ----Create CZML file for FEGS
    ####################################
    czmlBody = [{"id": "document",
                 "name": "FEGS FOV",
                 "version": "1.0", }]

    FEGS = DF[['secs', 'Time',
               'FOVlon1', 'FOVlat1', 'FOVlon2', 'FOVlat2',
               'FOVlon3', 'FOVlat3', 'FOVlon4', 'FOVlat4']]
    FEGS['secs'] = 's' + FEGS['secs'].astype(str)
    FEGS = FEGS.drop_duplicates(subset=["secs"], keep="first")

    for s, t, v0, v1, v2, v3, v4, v5, v6, v7 in zip(FEGS.secs, FEGS.Time,
                                                    FEGS.FOVlon1, FEGS.FOVlat1,
                                                    FEGS.FOVlon2, FEGS.FOVlat2,
                                                    FEGS.FOVlon3, FEGS.FOVlat3,
                                                    FEGS.FOVlon4, FEGS.FOVlat4):
        packet = {
            'id': s,
            'availability': t,
            'polygon': {
                'positions': {'cartographicDegrees': [v0, v1, 0, v2, v3, 0,
                                                      v4, v5, 0, v6, v7, 0]},
                'material': {'solidColor':
                    {'color': {
                        'rgba': [150, 255, 50, 120], }}}}
        }

        czmlBody.append(packet)

    folder = f"{os.getenv('FEGS_OUTPUT_PATH')}/{fdate}"

    mkfolder(folder)

    FEGSczml = json.dumps(czmlBody)
    filename = "FEGS_" + ltype + ".czml"
    filepath = f"{folder}/{filename}"

    CZMLfile = open(filepath, "w")
    CZMLfile.write(FEGSczml)
    CZMLfile.close()

    instr = "fegs"
    s3name = f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{fdate}/{instr}/{filename}"
    print(f"s3name={s3name}, filename={filepath}")
    upload_to_s3(filepath, os.environ['OUTPUT_DATA_BUCKET'], s3_name=s3name)


dates = ['2017-04-16','2017-04-18', '2017-04-20', '2017-04-22', '2017-05-07',
         '2017-05-08', '2017-05-12', '2017-05-14', '2017-05-17']

for fdate in dates:
    print(fdate)
    makeCZML(fdate)