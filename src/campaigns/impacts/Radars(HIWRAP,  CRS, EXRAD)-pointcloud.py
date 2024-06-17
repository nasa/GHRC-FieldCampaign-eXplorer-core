#----mk_RADpcloud.py
import numpy as np
import pandas as pd
import h5py
import json
from glob import glob
from datetime import datetime, timedelta
from pycode.Utils import *
from pycode.pcloud_subs import *


hpref = {'HIWRAP':'IMPACTS_HIWRAP_L1B_RevA_',
         'CRS':'IMPACTS_CRS_L1B_RevA_',
         'EXRAD':'IMPACTS_EXRAD_Nadir_L1B_RevB_'}

Bands = {'HIWRAP': ['Ka','Ku'],
         'CRS': ['W'],
         'EXRAD': ['X'] }

VARs = {'HIWRAP': ['dBZe','Vel','spW','LDR'],
        'CRS': ['dBZe','Vel'],
        'EXRAD': ['dBZe','Vel'], }

units = {'dBZe':'dBZ',
         'LDR': 'dB',
         'Vel': 'm/s',
         'spW': 'm/s' }


def mk_RADpcloud(Radar, fdate, dataDir, outDir0):
                  
    sdate = fdate.replace('-','')
    Hfile = glob(dataDir+hpref[Radar]+sdate+'*.h5')[0]
    
    Vars = VARs[Radar]
    RADs = RADdf(Radar, Hfile,Bands[Radar], Vars)


    #---Product data
    for bandSel in Bands[Radar]:
        print('\n*Processing data for band:',bandSel)
        
        RAD = RADs[bandSel]

        #----------Make pointcloud ----------
        # Cesium use Epoch time (secs since 1970) for visualization
        #        use seconds relative to the Epoch time within a module/tile
        #-------------------------------

        #----Set time, range, steps in pointcloud tileset
        t1970 = datetime(1970,1,1)
        t0 = datetime.strptime(fdate,"%Y-%m-%d")
        tFlight = datetime.strptime(fdate + tInstr[fdate], "%Y-%m-%dT%H:%M:%SZ")

        SecS = (tFlight - t1970).total_seconds()  #to be consistent with across all ER-2 measurements, 
        #SecS = RAD['Time'].min()                   #use RAD['Time'].min() for stand-alone
        SecE = RAD['Time'].max()
        RAD['timeP'] = RAD['Time'] - SecS         #time is counted from SecS in visualization

        lonw, lone = RAD['lon'].min()-0.2, RAD['lon'].max()+0.2
        lats, latn = RAD['lat'].min()-0.2, RAD['lat'].max()+0.2
        altb, altu = RAD['alt'].min(),RAD['alt'].max()
        bigbox = [lonw, lats, lone, latn, altb, altu] #*to_rad

        nPoints = len(RAD)
        Tsize = 500000
        nTile = nPoints//Tsize
        if(nPoints%Tsize > 0): nTile += 1
        print(' Valid data points:',nPoints)

        #----Make pointcloud tiles
        for vname in Vars:
            print(' -Making pointcloud tileset for',vname)
            folder= outDir0+ '/'+bandSel+'_'+vname
            mkfolder(folder)

            tileset = Tileset(bandSel+'_'+vname,bigbox, SecS)

            for tile in range (nTile):
                if(tile ==0):
                    epoch = SecS         #--epoch and end are seconds from (1970,1,1)
                else:
                    epoch =  RAD['Time'][tile*Tsize]   #SecS + tile*Tsize
                end = RAD['Time'][min((tile+1)*Tsize, nPoints-1)]
                subset = RAD[(RAD['Time'] >= epoch) & (RAD['Time'] < end)]
                print(sec2Z(epoch),sec2Z(end),len(subset))

                make_pcloudTile(vname, tile, tileset, subset, epoch, end, folder)

    return RAD


fdate='2020-02-27'
dataDir = '/Storage/Impacts/data/'
for Radar in hpref:
   #Radar ='CRS'  #'CRS', 'EXRAD',' HIWRAP'
    outDir0 = '/Storage/Impacts/VISdata/'+fdate+'/'+Radar.lower()   #head dir for outputs
    
    RAD = mk_RADpcloud(Radar, fdate, dataDir, outDir0)
