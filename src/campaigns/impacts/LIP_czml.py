#---LIP_czml.py
import numpy as np
import pandas as pd
import json, glob, os
from datetime import datetime, timedelta


########################################################
# LIP polyline czml
# Both I/O are in local disc as preprocessing is needed
########################################################

def makeCZML(path, fdate):
    # s3bucket = os.getenv('RAW_DATA_BUCKET')
    # LIPpath = os.getenv('LIP_INPUT_PATH')
    #
    # sdate = fdate.replace('-', '')
    #
    # s3key = 'fieldcampaign/goesrplt/LIP/data/goesr_plt_lip_' + sdate + '.txt'
    # download_s3(s3bucket, s3key, LIPpath)
    #
    # folder = f"{os.getenv('LIP_OUTPUT_PATH')}/{fdate}"
    # mkfolder(folder)
    # filename = "LIP_info.czml"
    # filepath = f"{folder}/{filename}"


    ############################################
    # Pre-process LIP data, get rid of "NaN"
    ############################################
    print(glob.glob(path + fdate)[0])
    fileLIP = glob.glob(path + fdate)[0]
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
    df=pd.read_csv(fileLIP2, sep=" ",header=0,usecols=[0, 1, 2, 3, 4, 8, 9, 10])
    df.columns = ['Time', 'Ex', 'Ey', 'Ez', 'Eq', 'lat', 'lon', 'alt']
    # print(df)
    df['Date_tmp'] = [datetime.strptime(a.split('T')[0], "%d-%b-%Y")  for a in df['Time']]
    df['Date'] = [datetime.strftime(a, "%Y-%m-%d") for a in df['Date_tmp']]
    df['Time0'] = [a.split('T')[1] for a in df['Time']]
    df['Time'] = [a.split('.')[0] for a in df['Time0']]
    # print(df.columns)
    # print(df['Time0'])
    # print(df['Time'])
    # print(df)
    df = df.drop(columns=['Time0'])
    # print(df)
    df = df.groupby(['Time','Date'], as_index=False).agg({'Ex':'mean', 'Ey':'mean', 'Ez':'mean',
                                                         'lat':'mean', 'lon':'mean','alt':'mean'})

    #---display would last 60 sec
    tform1='%Y-%m-%d %H:%M:%S'
    tform2='%Y-%m-%dT%H:%M:%SZ'
    df['time2'] = [(datetime.strptime(d + ' ' + h, tform1) +
                   timedelta(seconds = 60)).strftime(tform2)
                   for d, h in zip(df['Date'], df['Time']) ]  #<--display lasting time
    # print(df['time2'])
    df = df[(np.abs(df['Ex']) > 0.15) & (np.abs(df['Ey']) > 0.15) &(np.abs(df['Ez']) > 0.15) ]
    df = df.reset_index(drop=True)
    print(df)

    #######################################################
    # Making czml file
    #  draw vectors propotional to (ex,ey,ez)
    # Note that 'alt' not used, as display set to ground
    #######################################################
    czmlBody=[ {"id": "document",
                "name": "LIP",
                "version": "1.0", } ]

    LIP = df[['Date', 'Time', 'time2',
              'Ex', 'Ey', 'Ez', 'lat', 'lon']]

    ic=0
    for d, t, t2, ex, ey, ez, lat, lon in zip(LIP.Date, LIP.Time, LIP.time2,
                                              LIP.Ex, LIP.Ey, LIP.Ez,
                                              LIP.lat, LIP.lon ):
        xb = ex*.05
        yb = ey*.05
        zb = ez*2000
        lonlat = '<p>(Lon, Lat)= (' + str(np.round(lon,4)) + ', ' + str(np.round(lat,4))+')</p> '
        estr = '(' + str(np.round(ex,3)) + ', ' + str(np.round(ey,3)) + ', ' + str(np.round(ez,3)) + ')'
        packet = {
          'id': 'LIP-' + str(ic),
          'name': 'LIP@ ' + t[1:] + 'Z',
          'description': '<p>(Ex,Ey,Ez)= ' + estr + ' kV/m</p>' + lonlat,
          'availability': d + 'T' + t + 'Z/' + t2,
          'polyline':{
              'positions': {'cartographicDegrees':[lon, lat, 0,
                                                  lon + xb, lat + yb,0 + zb]},
              'material': {
                  'polylineArrow': {
                      'color': { 'rgba': [255, 50, 50, 255],  },
                  },
              },
              'width':5 }
        }

        czmlBody.append(packet)
        ic += 1

    LIPczml = json.dumps(czmlBody)

    CZMLfile = open('./CZML/'+'FCX_' + fdate.split(".")[0]+'.czml', "w")
    CZMLfile.write(LIPczml)
    CZMLfile.close()
    
    # instr = "lip"
    # s3name = f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{fdate}/{instr}/{filename}"
    # print(f"s3name={s3name}, filename={filepath}")
    # upload_to_s3(filepath, os.environ['OUTPUT_DATA_BUCKET'], s3_name=s3name)


dates = ['2017-04-11', '2017-04-13', '2017-04-16', '2017-04-18', '2017-04-20', '2017-04-22', '2017-05-07',
         '2017-05-08', '2017-05-12', '2017-05-14', '2017-05-17']
files = [
'IMPACTS_LIP_ER2_01152020.txt',
'IMPACTS_LIP_ER2_01182020.txt',
'IMPACTS_LIP_ER2_01262020.txt',
'IMPACTS_LIP_ER2_02012020.txt',
'IMPACTS_LIP_ER2_02062020.txt',
'IMPACTS_LIP_ER2_02072020.txt',
'IMPACTS_LIP_ER2_02232020.txt',
'IMPACTS_LIP_ER2_02252020.txt',
'IMPACTS_LIP_ER2_02272020.txt',
]

for fdate in files:
    print(f'** Making LIP CZML for {fdate} **')
    makeCZML('/Users/nselvaraj/PycharmProjects/IMPACTS/LIP/' , fdate)

# makeCZML('/Users/nselvaraj/PycharmProjects/IMPACTS/LIP/' , 'IMPACTS_LIP_ER2_02072020.txt')
# IMPACTS_LIP_ER2_02072020.txt
# makeCZML('/Users/nselvaraj/PycharmProjects/IMPACTS/LIP/')