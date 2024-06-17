#########################################
#----mk_RADpcloud.py
# Only used ATB
#########################################
import numpy as np
import pandas as pd
import os,io
from glob import glob
from datetime import time,datetime,timedelta
from utils.Utils import *
from utils.pcloud_subs import *



#to_rad = np.pi / 180.0
#to_deg = 180.0 / np.pi

CPLpath= '/Storage/Impacts/data/'
outDir0 = '/Storage/Impacts/VISdata/'   #head dir for outputs

def mk_CPLpcloud(fdate):
    fileobj = glob(CPLpath+'IMPACTS_CPL_ATB*'+fdate.replace('-','')+'*.hdf5')[0]
    print('file:',fileobj)
    folder= outDir0+fdate+'/cpl/atb/'
    mkfolder(folder)

    CPL, _, _ = ER2CPL(fileobj)


    t0 = datetime.strptime(fdate,"%Y-%m-%d")
    t1970 = datetime(1970,1,1)
    tFlight = datetime.strptime(fdate + tInstr[fdate], "%Y-%m-%dT%H:%M:%SZ")

    Sec0 = (t0 - t1970).total_seconds()       #<-- current day's 00Z from (1970,1,1) 00Z
    CPL['Time'] = [Sec0 + s for s in CPL['Secs']]
    SecS = (tFlight - t1970).total_seconds()  #to be consistent with RAD, 
   #SecS = CPL['Time'].min()                  #use CPL['Time'].min() for stand-alone
    SecE = CPL['Time'].max()
    CPL['timeP'] = CPL['Time'] - SecS

    lonw,lone=CPL['lon'].min()-0.2,CPL['lon'].max()+0.2
    lats,latn=CPL['lat'].min()-0.2,CPL['lat'].max()+0.2
    altb,altu=CPL['alt'].min()-0.2,CPL['alt'].max()+0.2
    bigbox=[lonw, lats, lone, latn, altb, altu] #*to_rad

    tileset=Tileset('CPL_atb',bigbox, SecS)

    steps = [32, 16, 8, 4, 2, 1]
    Tsize = 50000
    nPoints = len(CPL)
    nTile = int(nPoints/Tsize) + 1

    for tile in range (nTile):
        #--epoch and end are seconds from (1970,1,1)
        if(tile ==0):
            epoch = SecS
        else:
            epoch =  CPL['Time'][tile*Tsize]   #SecS + tile*Tsize
        end = CPL['Time'][min((tile+1)*Tsize, nPoints-1)]
        subCPL = CPL[(CPL['Time'] >= epoch) & (CPL['Time'] < end)]
        #print(sec2Z(epoch),sec2Z(end),len(subCPL))

        make_pcloudTile('atb',tile, tileset, subCPL, epoch, end, folder)
    
    return CPL


fdate = '2020-02-27'
CPL = mk_CPLpcloud(fdate)
