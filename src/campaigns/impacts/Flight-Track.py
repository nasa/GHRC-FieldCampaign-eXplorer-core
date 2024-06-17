import numpy as np
import pandas as pd
from copy import deepcopy
import json
import boto3
import os
import sys
from datetime import datetime, timedelta

sys.path.append("../")
from utils.s3_updnload import backup_file_s3

model = {
    "id": "Flight Track",
    "name": "P3",
    "availability": "{}/{}",
    "model": {
        "gltf":"https://fcx-czml.s3.amazonaws.com/img/p3.gltf",
        "scale": 1.0,
        "maximumScale": 1000.0
    },
    "position": {
        "cartographicDegrees": []
    },
    "path": {
        "material": {
            "solidColor": {
                "color": {
                    "rgba": [0, 255, 128, 255]
                }
            }
        },
        "width": 1,
        "resolution": 5
    },
    "properties": {
        "roll": {},
        "pitch": {},
        "heading": {}
    }
}

czml_head = {
    "id": "document",
    "name": "wall czml",
    "version": "1.0"
}

TrackColor = {'P3B': [0, 255, 128, 255],
              'ER2': [0, 255, 255, 128]}

modelP3B = {
        "gltf":"https://fcx-czml.s3.amazonaws.com/img/p3.gltf",
        "scale": 5.0,
        "maximumScale": 1000.0
}

modelER2 = {
    "gltf":"https://fcx-czml.s3.amazonaws.com/img/er2.gltf",
    "scale": 900.0,
    "minimumPixelSize": 500,
    "maximumScale": 1000.0
}



class FlightTrackCzmlWriter:

    def __init__(self, length, plane):
        self.model = deepcopy(model)
        if (plane == 'P3B') : self.model['model'] = modelP3B
        if (plane == 'ER2') : self.model['model'] = modelER2
        self.length = length
        self.model['name'] = plane
        self.model['path']['material']['solidColor']['color']['rgba'] = TrackColor[plane]
        self.model['position']['cartographicDegrees'] = [0] * 4 * length
        self.model['properties']['roll']['number'] = [0] * 2 * length
        self.model['properties']['pitch']['number'] = [0] * 2 * length
        self.model['properties']['heading']['number'] = [0] * 2 * length

    def set_time(self, time_window, time_steps):
        epoch = time_window[0]
        end = time_window[1]
        self.model['availability'] = "{}/{}".format(epoch, end)
        self.model['position']['epoch'] = epoch
        self.model['position']['cartographicDegrees'][0::4] = time_steps
        self.model['properties']['roll']['epoch'] = epoch
        self.model['properties']['pitch']['epoch'] = epoch
        self.model['properties']['heading']['epoch'] = epoch
        self.model['properties']['roll']['number'][0::2] = time_steps
        self.model['properties']['pitch']['number'][0::2] = time_steps
        self.model['properties']['heading']['number'][0::2] = time_steps

    def set_position(self, longitude, latitude, altitude):
        self.model['position']['cartographicDegrees'][1::4] = longitude
        self.model['position']['cartographicDegrees'][2::4] = latitude
        self.model['position']['cartographicDegrees'][3::4] = altitude

    def set_orientation(self, roll, pitch, heading):
        self.model['properties']['roll']['number'][1::2] = roll
        self.model['properties']['pitch']['number'][1::2] = pitch
        self.model['properties']['heading']['number'][1::2] = heading

    def get_string(self):
        return json.dumps([czml_head, self.model])



# basically, no Na filling in the data, so dropna() check is not necessary
# also removed unique time check as P3B/ER2 met data are recorded every 1s, there's no overlapping accounts. 
class FlightTrackReader:
    def __init__(self,file,plane):
        if (plane=='P3B'): cols=[0,1,2,3,4,12,15,16]
        if (plane=='ER2'): cols=[0,1,2,3,4,10,13,14]
        with open(file) as f:
            lines = f.readlines()
            for il,line in enumerate(lines):
                if('Time_Start,Day_Of_Year,' in line):
                    break
        self.file = file
        self.hlines = il
        self.useCols = cols
        self.plane = plane

    def read_csv(self,nskip=1):
        df = pd.read_csv(self.file,index_col=None,usecols=self.useCols, skiprows=self.hlines)
        df.columns = ['Time_s','Jday', 'lat','lon','alt','heading','pitch','roll']
        headingCorrection = -90 # for both p3B and ER2 model
        pitchCorrection = 0 # initial value
        if (self.plane == 'P3B'):
            pitchCorrection = +90 # pitch correction only for P3B model
        df['heading'] = [ h if h<=180 else h-360 for h in df.heading]
        df['heading'] = [ (h+headingCorrection) * np.pi / 180. for h in df.heading]
        df['pitch'] = [ (p+pitchCorrection) * np.pi / 180. for p in df.pitch]
        df['roll'] = [ r * np.pi / 180. for r in df.roll]
        df['time_steps'] = [(t - df.Time_s[0]) for t in df.Time_s]

        Cdate=datetime.strptime('2020'+str(df.Jday[0]).zfill(3),"%Y%j")
        time = [ Cdate + timedelta(seconds=s) for s in df.Time_s]
        self.twindow = [time[0].strftime('%Y-%m-%dT%H:%M:%SZ'), 
                        time[-1].strftime('%Y-%m-%dT%H:%M:%SZ')]
        
        df = df[df['Time_s']%(nskip+1) == 0]  #keep every nskip+1 s
        df = df.reset_index(drop=True)
        
        return df

def getOutputFile(fdate, plane, output_name):
    if(plane == 'P3B'):
        return f"fieldcampaign/impacts/{fdate}/p3/{output_name}"
    elif(plane == 'ER2'):
        if(fdate == "2020-02-25"):
            # correction on naming convention for some ER2
            return f"fieldcampaign/impacts/{fdate}/er2/{output_name}"
        # general naming convention for ER2
        return f"fieldcampaign/impacts/{fdate}/er2/FCX_{output_name}.czml"

from glob import glob

def process_tracks(fDates, plane):
    s3_client = boto3.client('s3')
    #--------to be modified -----
    #bucketOut = os.environ['OUTPUT_DATA_BUCKET']
    bucketOut = 'ghrc-fcx-viz-output'
    
    # do this for all raw files.
    for fdate in fDates:
        sdate=fdate.split('-')[0]+fdate.split('-')[1]+fdate.split('-')[2]
        # infile is the folder in local, where the raw data sits.
        # the location should be with respect to the Flight-Track.py file
        # while executing it from the dir same as Flight-Track.py
        infile = glob('data/IMPACTS_MetNav_'+plane+'_'+sdate+'*.ict')[0]

    #-----------------------------
        track = FlightTrackReader(infile,plane)
        Nav = track.read_csv()

        writer = FlightTrackCzmlWriter(len(Nav), plane)
        writer.set_time(track.twindow, Nav.time_steps)
        writer.set_position(Nav.lon, Nav.lat, Nav.alt)
        writer.set_orientation(Nav.roll, Nav.pitch, Nav.heading)

        output_name = os.path.splitext(os.path.basename(infile))[0]
        outfile = getOutputFile(fdate, plane, output_name)

        backup_file_s3(bucketOut, outfile)
        print(f'uploading new {outfile} in {bucketOut} bucket.')
        s3_client.put_object(Body=writer.get_string(), Bucket=bucketOut, Key=outfile)
        print(f'Upload complete.\n\n')

fDatesP3B = ['2020-02-25', '2020-02-20', '2020-02-18', '2020-02-13', '2020-02-07', '2020-02-05', '2020-02-01', '2020-01-25', '2020-01-18']
fDatesER2 = ['2020-02-27', '2020-02-25', '2020-02-07', '2020-02-05',  '2020-02-01', '2020-01-25', '2020-01-18']
process_tracks(fDatesP3B, 'P3B')
process_tracks(fDatesER2, 'ER2')