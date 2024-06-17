#--- pycode/pcloud_subs.py
import numpy as np
import json
from datetime import datetime, timedelta
from matplotlib import cm

to_rad = np.pi / 180
to_deg = 180 / np.pi

def regionrad(region): 
    return [r*to_rad for r in region]

def sec2Z(t): 
    return "{}Z".format(datetime.utcfromtimestamp(t).isoformat())


class Tileset:
    def __init__(self, Dataset, bigbox, time0):
        self.json = {
            "asset": {"version": "1.0",
                     "type": Dataset },
            "root": {"geometricError": 1000000,
                     "refine" : "REPLACE",
                     "boundingVolume": {"region": regionrad(bigbox)},
                     "children": []  },
            "properties": {"epoch": "{}Z".format(datetime.utcfromtimestamp(time0).isoformat()),
                           "refined": [] }  }
        self.parent=self.json["root"]
        print("{}Z".format(datetime.utcfromtimestamp(time0).isoformat()))


def make_pcloudTile(vname, tile, tileset, DF, epoch, end, folder):

    epochZ = "{}Z".format(datetime.utcfromtimestamp(epoch).isoformat())
    endZ   = "{}Z".format(datetime.utcfromtimestamp(end).isoformat())
   #print(sec2Z(epoch),sec2Z(end),len(DF))
    
    parent_tile = tileset.json["root"]
    cartesian, offset, scale, cartographic, region = cartographic_to_cartesian(DF['lon'],DF['lat'],DF['alt'])
    
    value = DF[vname].to_numpy()
    timep =  DF['timeP'].to_numpy()
    
    csize = 4
    if(vname=='dBZe'):
        ccode = color_encodeDBZ(DF[vname].copy())
    elif(vname=='CPL' or vname.lower()=='atb'):
        ccode = color_encodeATB(DF[vname].copy())
    else:
        ccode = color_encodeOthers(DF[vname].copy(), vname)
    
    header_length = 28
    magic = np.string_("pnts")
    version = 1

    steps = [32, 16, 8, 4, 2, 1]
    for step in steps:
        filename = "{}_{}.pnts".format(tile, step)
        child_tile = {
            "availability": "{}/{}".format(epochZ, endZ),
            "geometricError": step * 500,
            "boundingVolume": {
                "region": region
            },
            "content": {
                "uri": filename
            },
            "refine": "REPLACE"
        }
        if step == 1:
            tileset.json["properties"]["refined"].append(filename)
        else:
            child_tile["children"] = []
        
        parent_tile["children"].append(child_tile)
        parent_tile = child_tile

        tile_length = 0
        feature_table_binary_byte_length = 0
        batch_table_binary_byte_length = 0
        length = value[::step].size

        feature_table_json = {
            "POINTS_LENGTH": length,
            "BATCH_LENGTH": length,
            "BATCH_ID": {
                "byteOffset": 0,
                "componentType": "UNSIGNED_INT"
            },
            "POSITION_QUANTIZED": {
                "byteOffset": length * 4
            },
            "QUANTIZED_VOLUME_OFFSET": offset,
            "QUANTIZED_VOLUME_SCALE": scale,
            "RGBA": {
                "byteOffset": length * 4 + length * 3 * 2
            },
        }

        batch_table_json = {
            "time": {
                "byteOffset": length * 0,
                "componentType": "FLOAT",
                "type": "SCALAR"
            },
            "location": {
                "byteOffset": length * 4,
                "componentType": "SHORT",
                "type": "VEC3"
            }
        }

        tile_length += header_length

        feature_table_json_min = json.dumps(feature_table_json, separators=(",", ":")) + "       "
        feature_table_trim = (tile_length + len(feature_table_json_min)) % 8
        if feature_table_trim != 0:
            feature_table_json_min = feature_table_json_min[:-feature_table_trim]

        tile_length += len(feature_table_json_min)

        feature_table_binary_byte_length = length * 4 + length * 3 * 2 + length * csize
        tile_length += feature_table_binary_byte_length
        feature_table_padding = tile_length % 8
        if feature_table_padding != 0:
            feature_table_padding = 8 - feature_table_padding
        tile_length += feature_table_padding

        batch_table_json_min = json.dumps(batch_table_json, separators=(",", ":")) + "       "
        batch_table_trim = (tile_length + len(batch_table_json_min)) % 8
        if batch_table_trim != 0:
            batch_table_json_min = batch_table_json_min[:-batch_table_trim]

        tile_length += len(batch_table_json_min)

        batch_table_binary_byte_length = length * 4 + length * 2 * 3
        tile_length += batch_table_binary_byte_length
        batch_table_padding = tile_length % 8
        if batch_table_padding != 0:
            batch_table_padding = 8 - batch_table_padding
        tile_length += batch_table_padding

        with open('{}/{}'.format(folder, filename), mode='wb+') as outfile:
            outfile.write(np.string_(magic).tobytes())
            outfile.write(np.uint32(version).tobytes())
            outfile.write(np.uint32(tile_length).tobytes())
            outfile.write(np.uint32(len(feature_table_json_min)).tobytes())
            outfile.write(np.uint32(feature_table_binary_byte_length + feature_table_padding).tobytes())
            outfile.write(np.uint32(len(batch_table_json_min)).tobytes())
            outfile.write(np.uint32(batch_table_binary_byte_length + batch_table_padding).tobytes())
            outfile.write(np.string_(feature_table_json_min).tobytes())
            outfile.write(np.arange(length, dtype=np.uint32).tobytes())
            outfile.write(cartesian[::step, :].tobytes())
            outfile.write(ccode[::step, :].tobytes())
            for _ in range(feature_table_padding):
                outfile.write(np.string_(" ").tobytes())
            outfile.write(np.string_(batch_table_json_min).tobytes())
            outfile.write(timep[::step].astype(np.float32).tobytes())
            outfile.write(cartographic[::step, :].tobytes())
            for _ in range(batch_table_padding):
                outfile.write(np.string_(" ").tobytes())
            outfile.seek(0)
 
    outfile = open(folder+"/tileset.json", "w")
    outfile.write(json.dumps(tileset.json))


def cartographic_to_cartesian(lon,lat,alt):
    size = lon.size

    cartographic = np.zeros(shape=(size, 3), dtype=np.int16)
    cartographic[:, 0] = (lon * 32767 / 180).astype(np.int16)
    cartographic[:, 1] = (lat * 32767 / 180).astype(np.int16)
    cartographic[:, 2] = (alt / 10).astype(np.int16)

    lon = lon * to_rad
    lat = lat * to_rad

    radiiSquared = np.array([40680631590769, 40680631590769, 40408299984661.445], dtype=np.float64)

    N1 = np.multiply(np.cos(lat), np.cos(lon))
    N2 = np.multiply(np.cos(lat), np.sin(lon))
    N3 = np.sin(lat)

    magnitude = np.sqrt(np.square(N1) + np.square(N2) + np.square(N3))

    N1 = N1 / magnitude
    N2 = N2 / magnitude
    N3 = N3 / magnitude

    K1 = radiiSquared[0] * N1
    K2 = radiiSquared[1] * N2
    K3 = radiiSquared[2] * N3

    gamma = np.sqrt(np.multiply(N1, K1) + np.multiply(N2, K2) + np.multiply(N3, K3))

    K1 = K1 / gamma
    K2 = K2 / gamma
    K3 = K3 / gamma

    N1 = np.multiply(N1, alt)
    N2 = np.multiply(N2, alt)
    N3 = np.multiply(N3, alt)

    # x = np.multiply((N1 + K1), np.random.normal(1, .00005, N1.size))
    # y = np.multiply((N2 + K2), np.random.normal(1, .00005, N1.size))
    # z = np.multiply((N3 + K3), np.random.normal(1, .00005, N1.size))

    x = N1 + K1
    y = N2 + K2
    z = N3 + K3

    offset = [float(np.min(x)), float(np.min(y)), float(np.min(z))]

    x = x - offset[0]
    y = y - offset[1]
    z = z - offset[2]

    scale = [float(abs(np.max(x))), float(abs(np.max(y))), float(abs(np.max(z)))]

    cartesian = np.zeros(shape=(size, 3), dtype=np.uint16)
    cartesian[:, 0] = (x / scale[0] * 65535.0).astype(np.uint16)
    cartesian[:, 1] = (y / scale[1] * 65535.0).astype(np.uint16)
    cartesian[:, 2] = (z / scale[2] * 65535.0).astype(np.uint16)

    region = [
        float(np.min(lon)),
        float(np.min(lat)),
        float(np.max(lon)),
        float(np.max(lat)),
        float(np.min(alt)),
        float(np.max(alt))
    ]

    return cartesian, offset, scale, cartographic, region


def color_encodeOthers(var, vname):
    KWS = {'Vel': {'bot':-10, 'top':10, 'intv':1,'cmap': 'bwr'},  #'bwr', 'cool'
           'spW': {'bot':  0, 'top':20, 'intv':1,'cmap': 'cool'}, #'winter', 'cool', 'YlGnBu'
           'LDR': {'bot':-30, 'top': 0, 'intv':1,'cmap': 'YlGnBu'},
           }
    kws = KWS[vname]
    
    if('spW' in vname or 'Vel' in vname or 'DOP' in vname or 'LDR' in vname):
        vbot, vtop, vint, cmap = kws['bot'], kws['top'], kws['intv'], kws['cmap']

    ccode = np.zeros(shape=(var.size, 4), dtype=np.uint8)
    nlvl = int((vtop - vbot)/vint)
   
    cmp  =cm.get_cmap(cmap,nlvl)
    cols = cmp(np.linspace(0, 1, nlvl))*255
   #cols = np.flip(cols,0)  #inverse
   #cols[:,3]=0.8

    rgba = cols.astype(np.uint8)
    for iL in range(nlvl):
        ccode[(var > vbot + iL*vint) & (var <= vbot + (iL+1)*vint), :] = rgba[iL, :]
    
    return ccode

def color_encodeDBZ(var):
    vtop =  50  ; var[var >= vtop] = vtop + .0001
    vbot = -25  ; var[var <= vbot] = vbot - .0001
    vint = 1
    nlvl = int((vtop - vbot)/vint)

    ccode = np.zeros(shape=(var.size, 4), dtype=np.uint8)
    
    if(vtop>40):
        n1, n2 = int((40-vbot)/vint), int((vtop-40)/vint)
        if(n1+n2 < nlvl): n2 = nlvl-n1
        cmp1 = cm.get_cmap('jet',n1); basecols = cmp1(np.linspace(0, 1, n1))
        cmp2 = cm.get_cmap('gist_ncar',n2*6); enhcols = cmp2(np.linspace(0, 1, n2*6))
        cols = np.vstack((basecols, enhcols[n2*5:,:]))*255
    else:
        cmp  =cm.get_cmap('jet',nlvl)
        cols = cmp(np.linspace(0, 1, nlvl))*255

    rgba = cols.astype(np.uint8)
    for iL in range(nlvl):
        ccode[(var > vbot + iL*vint) & (var <= vbot + (iL+1)*vint), :] = rgba[iL, :]
    
    return ccode

def color_encodeATB(var):
    """Color-coding for CPL ATB """
    atbPLvs=[0.005, 0.007, 0.009, 0.015, 0.02, 0.05, 0.1, 5]

    nlvl = len(atbPLvs) - 1
    cbase = 80
    opa = 255
    rgba = np.zeros(shape=(nlvl,4), dtype=np.uint8)
    ccode = np.zeros(shape=(var.size, 4), dtype=np.uint8)
    for iL in range(nlvl):
        cn = cbase + iL*(255-cbase)/(nlvl-1)
        rgba[iL,:] = np.array([cn,cn-50,cn,opa]) #/255
        ccode[(var>atbPLvs[iL]) & (var<=atbPLvs[iL+1]),:] = rgba[iL,:]
    return ccode


