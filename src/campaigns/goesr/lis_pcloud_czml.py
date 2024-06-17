import xarray as xr
from lis_subcode import *

np.random.seed(12345)

def Subset3(Org, start, end):
    items = ['Lon', 'Lat', 'Alt', 'Time', 'Rad']
    DF = pd.DataFrame()
    for itm in items: DF[itm] = Org.__dict__[itm]
    DF = DF.loc[(DF['Time'] >= start) & (DF['Time'] <= end)]

    subs = lightning(Org.ltype, Org.instr)
    for item in items:
        subs.__dict__[item] = DF[item]
    return subs


def makePointCloud(fdate):
    instr = 'LIS'
    Dt_show = 10  # <--dislpay time extended 10 sec for the tile

    s3bucket = os.getenv('RAW_DATA_BUCKET')  # s3bucket if input is in "cloud"

    fdatej = datetime.strptime(fdate, "%Y-%m-%d").strftime("%Y%j")

    folder = os.getenv('LIS_OUTPUT_PATH') + '/' + fdate + '_czml/'
    mkfolder(folder)

    # ----get CRS/flight track & box region
    CRSlat, CRSlon, CRStime, crstime = get_CRS(fdate, s3bucket)

    # ----set outer box region
    lats, latn = CRSlat.min() - 5, CRSlat.max() + 5
    lonw, lone = CRSlon.min() - 5, CRSlon.max() + 5
    altb, altu = 0., 10.
    bigbox = [lonw, lats, lone, latn, altb, altu]  # *to_rad

    # ---get LIS files
    filesLIS = LISfiles(s3bucket, fdate, bigbox, CRStime[0], CRStime[-1], Verb=False)
    print(filesLIS)

    if len(filesLIS) == 0:
        return

    # ----tiles set up
    steps = [16, 4, 1]
    usetype = {32: 'flash', 16: 'flash', 8: 'group', 4: 'group', 2: 'event', 1: 'event'}
    usebox = {32: bigbox, 16: bigbox, 8: bigbox, 4: bigbox, 2: bigbox, 1: bigbox}
    useskip = {32: 1, 16: 1, 8: 1, 4: 1, 2: 1, 1: 1}

    to_rad = np.pi / 180.0
    to_deg = 180.0 / np.pi

    # ---get thru each file

    LTN = {'flash': lightning('flash', 'LIS'),
           'group': lightning('group', 'LIS'),
           'event': lightning('event', 'LIS')}
    types = ['flash', 'group', 'event']

    for filenc in filesLIS:

        file = s3FileObj(s3bucket, filenc, verb=True)

        with xr.open_dataset(file, engine='h5netcdf') as ds:
            for typ in types:
                LTN[typ].Ltndata(ds, bigbox)

    #nTile = 5
    if (fdate == '2017-05-17'):
        nTile = 5
    else:
        nTile = 2

    Tsize = {}  # <-- Different ltype may have different length. So use dict.
    for typ in reversed(types):
        Tsize[typ] = int(np.ceil(LTN[typ].Time.size / nTile))
    Trng = [LTN['event'].Time[0], LTN['event'].Time[-1]]

    log = {}
    for tile in range(nTile):

        folder2 = folder + 'tile' + str(tile) + '/'
        mkfolder(folder2)
        tileset = Tileset(bigbox, LTN['event'].Time[0], instr)

        subLTN = {}
        for typ in types:
            start = Trng[0] + tile * (Trng[1] - Trng[0]) / nTile
            end = min(start + (Trng[1] - Trng[0]) / nTile, Trng[1])
            subLTN[typ] = Subset3(LTN[typ], start, end)
            if (typ == 'flash'): print('typ range: ', start, end)

            subLTN[typ].cartographic_to_cartesian()

        epoch = int(np.min(subLTN[typ].Time))  # <--use event
        end = int(np.max(subLTN[typ].Time) + Dt_show)
        tileset.epoch = "{}Z".format(datetime.utcfromtimestamp(epoch).isoformat())
        tileset.end = "{}Z".format(datetime.utcfromtimestamp(end).isoformat())
        print(sec2Z(epoch), sec2Z(end))
        log['tile' + str(tile)] = sec2Z(epoch) + '/' + sec2Z(end)

        for step in steps:
            Ldata = subLTN[usetype[step]]
            skip = useskip[step]
            if (len(Ldata.Lon) == 0): print('zero length')
            if (len(Ldata.Lon) == 0): continue

            MK_cloud_czml(instr, tile, step, tileset, Ldata, skip, folder2)

        outfile = open(folder2 + "/tileset.json", "w")
        outfile.write(json.dumps(tileset.json))
        outfile.close()

    ##########################
    # LIS tile czml
    ##########################

    s3uri = f"https://{os.environ['OUTPUT_DATA_BUCKET']}.s3-{os.environ['AWS_REGION']}.amazonaws.com/fieldcampaign/goesrplt/"

    s3path = f"s3://{os.environ['OUTPUT_DATA_BUCKET']}/{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{fdate}/iss-lis_czml"

    czmlBody = [{"id": "document",
                 "name": "ISS-LIS Lightning",
                 "version": "1.0", }]

    for tile, tstr in log.items():
        packet = {"id": tile,
                  "availability": tstr,
                  "tileset": {"uri": s3uri + fdate + "/iss-lis_czml/" + tile + "/tileset.json", },
                  }
        czmlBody.append(packet)

    LISczml = json.dumps(czmlBody)

    filename = "LIS_tiles.czml"
    filepath = f"{folder}/{filename}"

    CZMLfile = open(filepath, "w")
    CZMLfile.write(LISczml)
    CZMLfile.close()

    os.system(
        f"aws s3 sync {folder} {s3path}/")


dates = ['2017-04-11', '2017-04-13', '2017-04-16', '2017-04-18', '2017-04-20', '2017-04-22', '2017-05-07', '2017-05-08',
         '2017-05-12', '2017-05-14', '2017-05-17']




for fdate in dates:
    makePointCloud(fdate)
