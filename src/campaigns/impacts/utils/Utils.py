#----pycode/Utils.py
import numpy as np
import pandas as pd
import h5py
import os, io

to_rad = np.pi / 180
to_deg = 180 / np.pi

tInstr = {'2020-01-25':'T18:19:50Z', 
          '2020-02-01':'T11:33:15Z',
          '2020-02-05':'T19:23:20Z',
          '2020-02-07':'T12:22:20Z',
          '2020-02-25':'T20:29:20Z',
          '2020-02-27':'T07:43:30Z',
         }


def down_vector(roll, pitch, head):
    x = np.sin(roll) * np.cos(head) + np.cos(roll) * np.sin(pitch) * np.sin(head)
    y = -np.sin(roll) * np.sin(head) + np.cos(roll) * np.sin(pitch) * np.cos(head)
    z = -np.cos(roll) * np.cos(pitch)
    return (x, y, z)

def proj_LatLonAlt(DF):
    """Zdist is distance from Aircraft"""
    
    x, y, z = down_vector(DF['roll'], DF['pitch'], DF['head'])
    x = np.multiply(x, np.divide(DF['Zdist'], 111000 * np.cos(DF['Lat'] * to_rad)))
    y = np.multiply(y, np.divide(DF['Zdist'], 111000))
    z = np.multiply(z, DF['Zdist'])

    lon = np.add(-x, DF['Lon'])
    lat = np.add(-y, DF['Lat'])
    alt = np.add(z,  DF['Alt'])
    return lon,lat,alt

def S1970toSec(TimeS1970,fdate=None):
    t1970 = datetime(1970,1,1)
    currT = t1970 + timedelta(seconds = TimeS1970)
    if(fdate):
        t0 = datetime.strptime(fdate,"%Y-%m-%d")
        return (currT - t0).total_seconds()
    else: 
        return currT.hour*3600 + currT.minute*60 +currT.second
    
def resetNAN(arrs, reset=-999.):
    arrs2 = []
    for arr in arrs:
        arr[np.isnan(arr)] = reset
        arrs2.append(arr)
    return arrs2
        
def mkfolder(folder):
    if(not os.path.exists(folder)): 
        try:
            os.makedirs(folder)
            print('Success to create folder %s' % folder)    
        except OSError:
            print('Failed to create folder %s' % folder)
            quit()
    else:
        print('%s already exists' % folder)
        
def ER2Radar(Radar,Hfile):
    with h5py.File(Hfile, 'r') as fh5:
        nav = fh5['Navigation/Data']
        Tepoch = fh5['Time/Data/TimeUTC'][()]
        rad_range = fh5['Products/Information/Range'][()]
        rad_range = rad_range.reshape(rad_range.size)
        lat, lon, alt = nav['Latitude'][()], nav['Longitude'][()], nav['Height'][()]
        roll, pitch, head = nav['Roll'][()], nav['Pitch'][()], nav['Heading'][()]
        
        if(Radar=='HIWRAP'):
            Ka = fh5['Products/Ka/Combined/Data']
            Ku = fh5['Products/Ku/Combined/Data']
            data={'Ka':{'dBZe': Ka['dBZe'][()],
                        'LDR': Ka['LDR'][()],
                        'Vel': Ka['Velocity'][()],
                        'spW': Ka['SpectrumWidth'][()] },
                  'Ku':{'dBZe': Ku['dBZe'][()],
                        'LDR': Ku['LDR'][()],
                        'Vel': Ku['Velocity'][()],
                        'spW': Ku['SpectrumWidth'][()] } }
        elif(Radar=='CRS'):
            band = fh5['Products/Data']
            data={'W':{'dBZe': band['dBZe'][()],
                       'LDR': band['LDR'][()],
                       'Vel': band['Velocity'][()],
                       'spW': band['SpectrumWidth'][()]  } }
        elif(Radar=='EXRAD'):
            band = fh5['Products/Data']
            data={'X':{'dBZe': band['dBZe'][()],
                       'Vel': band['Velocity'][()],
                      #'Vel_nubf':  band['Velocity_nubf_fix'][()],
                       'spW':  band['SpectrumWidth'][()]  } }

        print('*{} data obtained.'.format(Radar))

    ncol = Tepoch.size
    nrow = rad_range.size

    #---Track data
    RAD0 = pd.DataFrame()
    RAD0['Time'] = np.repeat(Tepoch, nrow) # Use Epoch time (seconds since 1970)
    RAD0['Lon'] = np.repeat(lon, nrow)
    RAD0['Lat'] = np.repeat(lat, nrow)
    RAD0['Alt'] = np.repeat(alt, nrow)
    RAD0['roll'] = np.repeat(roll * to_rad, nrow)
    RAD0['pitch'] = np.repeat(pitch * to_rad, nrow)
    RAD0['head'] = np.repeat(head * to_rad, nrow)
    RAD0['Zdist'] = np.tile(rad_range, ncol)

    RAD0['lon'], RAD0['lat'], RAD0['alt'] = proj_LatLonAlt(RAD0)

    RAD0 = RAD0.drop(['roll','pitch','head','Zdist','Lon','Lat','Alt'], axis=1 )
    print(' flight data processed.'.format(Radar))
                  
    return RAD0, data


def RADdf(Radar, Hfile, Bands, Vars):

    RAD0, data = ER2Radar(Radar,Hfile)

    #---Product data
    RADs={}
    for bandSel in Bands:
        print('\n*Processing data for band:',bandSel)
        
        RAD =RAD0.copy()
        for vname in Vars: RAD[vname] = data[bandSel][vname].flatten()

        #---initial clean up and processing
        print(' Original data points:',len(RAD))
        RAD.dropna(subset=Vars, how='all', inplace=True)
        RAD = RAD.fillna(-999)
        RAD = RAD[(RAD['alt'] >= 0) & (RAD['alt'] <= 18000)] #<--mid_lat winter storm (12000 would do)
        RAD = RAD.reset_index(drop=True)
        print(' In range data points:',len(RAD))
        
        RADs[bandSel] = RAD
    return RADs


def syncDim(arr,stdArr):
    csize, size = arr.size, stdArr.size
    if(csize>size):
        arr = arr[0:size]
    return arr

def ER2CPL(Hfile):
    with h5py.File(Hfile, 'r') as f1:
        atb1064 = f1['ATB_1064'][()]
        AltBin = f1['Bin_Alt'][()] * 1000    #[km] ==> [m]
        Lon1D  = f1['Longitude'][()]
        Lat1D  = f1['Latitude'][()]
        Alt1D  = f1['Plane_Alt'][()] * 1000   #[km] ==> [m]
        roll1D = f1['Plane_Roll'][()] * to_rad
        head1D = f1['Plane_Heading'][()] * to_rad
        pitch1D = f1['Plane_Pitch'][()] * to_rad
        Sec1D = [h*3600+m*60+s for (h,m,s) in 
                zip(f1['Hour'][()], f1['Minute'][()], f1['Second'][()])]

    Sec1D = np.array(Sec1D)
    Sec1D[Sec1D < Sec1D[0]] = Sec1D[Sec1D < Sec1D[0]] + 86400 #account for time over 00Z

    Alt1D = syncDim(Alt1D, Sec1D)
    roll1D = syncDim(roll1D, Sec1D)
    head1D = syncDim(head1D, Sec1D)
    pitch1D = syncDim(pitch1D, Sec1D)

    atb1064[np.isnan(atb1064)] = -999.
    atb1064[np.isinf(atb1064)] = -999.
    atb1064.shape


    CPL = pd.DataFrame()
    ncol, nrow = atb1064.shape
    CPL['atb'] = atb1064.flatten()
    CPL['lev'] = np.tile(AltBin, ncol)
    CPL['Lon'] = np.repeat(Lon1D, nrow)
    CPL['Lat'] = np.repeat(Lat1D, nrow)
    CPL['Alt'] = np.repeat(Alt1D, nrow)
    CPL['roll']  = np.repeat(roll1D, nrow)
    CPL['head']  = np.repeat(head1D, nrow)
    CPL['pitch'] = np.repeat(pitch1D, nrow)
    CPL['Secs'] = np.repeat(Sec1D, nrow)
    CPL['Zdist'] = CPL['Alt'] - CPL['lev']
    
    atbLbound=0.001              # ignore background (low values)
    CPL=CPL[CPL['atb']>=atbLbound]

    CPL['lon'], CPL['lat'], CPL['alt'] = proj_LatLonAlt(CPL)
    CPL = CPL.drop(['roll','pitch','head','Zdist','Lon','Lat','Alt'], axis=1 )

    CPL=CPL[CPL['alt']>  0]   # ignore near surface signals
    CPL = CPL.reset_index(drop=True)
    nPoints = len(CPL)
    
    return CPL, Sec1D, Alt1D #(for plotting Only)
