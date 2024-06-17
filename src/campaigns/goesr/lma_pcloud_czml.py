import pandas as pd
from lma_subcode import *


# ------ LMA CZML----------------------------------------------
# - Use multiple tiles for a file (5/10 min)                  -
# - Each tile is 60 sec                                       -
# - Current display is color-coded with alt                   -
# - point cloud tiles and czml file save to local LMA/ subdir -
# -------------------------------------------------------------

Dt_show = 10


def Subset3(Org, start, end):
    DF = Org.DF
    subs = lightning(Org.network)
    subs.DF = DF[(DF['Time'] >= start) & (DF['Time'] <= end)]
    print("Length:", len(subs.DF['Time']))
    return subs


def makePointCloud(fdate, network):

    fdate = fdate  # Flight date, in yyyy-mm-dd string
    s3bucket = os.getenv('RAW_DATA_BUCKET')

    # ---------------------------------------------------------------

    folder = os.getenv('LMA_OUTPUT_PATH') + '/' + fdate + '_rgba/' + network + "/"
    mkfolder(folder)

    # ----get CRS/flight track & box region
    CRSlat, CRSlon, CRStime, crstime = get_CRS(fdate, s3bucket)

    sec0 = CRStime[0] + (600 - CRStime[0] % 600)  # <---starts on 10-min mark
    secf = CRStime[-1] - CRStime[-1] % 600  # <---ends on 10-min mark
    Trng = int(secf - sec0)  # <---range for data
    tstart = datetime(1970, 1, 1) + timedelta(seconds=int(sec0))

    Tsize = 60  # <-- in [sec]              #<--tile for every Tsize sec
    nTile = int((secf - sec0) / Tsize) + 1

    # ---get LIS files
    filesLMA = LMAfiles(s3bucket, fdate, tstart, Trng, network=network)

    # ----tiles set up
    steps = [1]
    usetype = {32: 'flash', 16: 'flash', 8: 'group', 4: 'group', 2: 'event', 1: 'event'}
    useskip = {32: 4, 16: 4, 8: 2, 4: 2, 2: 1, 1: 1}

    to_rad = np.pi / 180.0
    to_deg = 180.0 / np.pi

    # ---get thru each file
    LTN = lightning(network)
    if (network == 'OKLMA'):
        stns_min = 8
    else:
        stns_min = 7

    nheader = None
    for file in filesLMA:
        print(file)
        df, nheader = get_LMA(s3bucket, file,
                              stns_min=stns_min, nheader=nheader)
        if (len(df) > 0):
            LTN.DF = pd.concat([LTN.DF, df])
        print("No. of filtered data: ", len(df), len(LTN.DF))

    LTN.DF = LTN.DF.reset_index()

    lonw, lone = LTN.DF['Lon'].min(), LTN.DF['Lon'].max()
    lats, latn = LTN.DF['Lat'].min(), LTN.DF['Lat'].max()
    altb, altu = LTN.DF['Alt'].min(), LTN.DF['Alt'].max()
    bigbox = [lonw, lats, lone, latn, altb, altu]  # *to_rad

    t1970 = datetime(1970, 1, 1)
    t1 = datetime(int(fdate[0:4]), int(fdate[5:7]), int(fdate[8:10]))
    dSecs = (t1 - t1970).total_seconds()

    # ---get thru each tile
    log = {}
    for tile in range(nTile):

        epoch = sec0 + tile * Tsize
        end = min(epoch + Tsize, secf)

        subLTN = Subset3(LTN, epoch - dSecs, end - dSecs)

        if (len(subLTN.DF) < 2): continue

        folder2 = folder + 'tile' + str(tile) + '/'
        mkfolder(folder2)

        tileset = Tileset(bigbox, epoch, network)

        end = int(end + Dt_show)
        tileset.epoch = "{}Z".format(datetime.utcfromtimestamp(epoch).isoformat())
        tileset.end = "{}Z".format(datetime.utcfromtimestamp(end).isoformat())
        print(sec2Z(epoch), sec2Z(end), len(subLTN.DF))
        log['tile' + str(tile)] = sec2Z(epoch) + '/' + sec2Z(end)
        subLTN.cartographic_to_cartesian()

        for step in steps:
            skip = useskip[step]
            if (len(subLTN.DF) == 0): print('zero length')
            if (len(subLTN.DF) <= 2): continue  # <--need 2 points to get range

            MK_cloud_czml(network, tile, step, tileset, subLTN, skip, folder2)

        outfile = open(folder2 + "/tileset.json", "w")
        outfile.write(json.dumps(tileset.json))
        outfile.close()

    logfile = open(folder + "/logfile.json", "w")
    logfile.write(json.dumps(log))
    logfile.close()

    lma = 'lma'

    ##########################
    # LMA tile czml
    ##########################

    s3uri = f"https://{os.environ['OUTPUT_DATA_BUCKET']}.s3-{os.environ['AWS_REGION']}.amazonaws.com/fieldcampaign/goesrplt/"

    s3path = f"s3://{os.environ['OUTPUT_DATA_BUCKET']}/{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{fdate}/{lma}/{network}"

    czmlBody = [{"id": "document",
                 "name": network + " Lightning",
                 "version": "1.0", }]

    for tile, tstr in log.items():
        packet = {"id": tile,
                  "availability": tstr,
                  "tileset": {"uri": s3uri + fdate + "/" + lma + "/" + network + "/" + tile + "/tileset.json"},
                  }
        czmlBody.append(packet)

    LMAczml = json.dumps(czmlBody)
    filename = f"{folder}/{network}_tiles.czml"
    CZMLfile = open(filename, "w")
    CZMLfile.write(LMAczml)
    CZMLfile.close()

    os.system(f"aws s3 sync {folder} {s3path}/")

makePointCloud('2017-04-18', "NALMA")
makePointCloud('2017-04-20', "SOLMA")
makePointCloud('2017-04-22', "NALMA")
makePointCloud('2017-05-08', "COLMA")
makePointCloud('2017-05-17', "OKLMA")
makePointCloud('2017-05-17', "WTXLMA")
