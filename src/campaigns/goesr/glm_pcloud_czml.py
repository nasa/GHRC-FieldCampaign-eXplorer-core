import xarray as xr
from glm_subcode import *

np.random.seed(123)

def dictkey(dictlist, val):
    return list(dictlist.keys())[list(dictlist.values()).index(val)]

def makePointCloud(fdate):

    instr = 'GLM'
    Dt_show = 10
    ltype = None  # 'flash'

    s3bucket = os.getenv('RAW_DATA_BUCKET')


    s3uri= f"https://{os.environ['OUTPUT_DATA_BUCKET']}.s3-{os.environ['AWS_REGION']}.amazonaws.com/fieldcampaign/goesrplt/"

    s3path = f"s3://{os.environ['OUTPUT_DATA_BUCKET']}/{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/goesrplt/{fdate}/glm_czml_1min"

    instr_suffix = '_czml_1min'

    folder = os.getenv('GLM_OUTPUT_PATH') + '/' + fdate + instr_suffix + '/'  # <---

    fdatej = datetime.strptime(fdate, "%Y-%m-%d").strftime("%Y%j")

    mkfolder(folder)

    # ----get CRS/flight track location & time
    CRSlat, CRSlon, CRStime, crstime = get_CRS(fdate, s3bucket)

    lats, latn = 20., 55.
    lonw, lone = -115., -70.
    altb, altu = 0., 100.
    bigbox = [lonw, lats, lone, latn, altb, altu]  # *to_rad
    CRSbox = [CRSlon.min() - 1, CRSlat.min() - 1, CRSlon.max() + 1, CRSlat.max() + 1, altb, altu]
    bigbox = CRSbox

    # ----tiles set up
    Tsize = 60  # <-- in [sec]
    sec0 = CRStime[0] + (300 - CRStime[0] % 600)  # <---starts on 10-min mark
    secf = CRStime[-1] - CRStime[-1] % 600  # <---ends on 10-min mark
    nTile = int((secf - sec0) / Tsize)

    steps = [16, 4, 1]
    usetype = {32: 'flash', 16: 'flash', 8: 'group', 4: 'group', 2: 'event', 1: 'event'}
    useskip = {32: 1, 16: 1, 8: 1, 4: 1, 2: 1, 1: 1}
    usebox = {32: bigbox, 16: bigbox, 8: bigbox, 4: bigbox, 2: CRSbox, 1: CRSbox}

    if (ltype):
        for i in usetype: usetype[i] = ltype
    if (ltype == 'flash'): useskip = {32: 1, 16: 1, 8: 1, 4: 1, 2: 1, 1: 1}
    if (ltype == 'group'): useskip = {32: 1, 16: 1, 8: 1, 4: 1, 2: 1, 1: 1}
    if (ltype == 'event'): useskip = {32: 1, 16: 1, 8: 1, 4: 1, 2: 1, 1: 1}

    to_rad = np.pi / 180.0
    to_deg = 180.0 / np.pi
    r, g, b = 102, 0, 255

    # ---get thru each tile
    s3list = None
    log = {}
    itile = 0  # <--use itile, bcs empty tiles will be skipped and not counted
    for tile in range(nTile):
        print("\nTile ", tile, itile)

        sec1, sec2 = sec0 + tile * Tsize, sec0 + tile * Tsize + Tsize
        Trng = 20 * ((sec2 - sec1) // 20 + 1)

        # ---find CRS starting time and range for tile (epoch)
        ts = sec2Z(sec0 + tile * Tsize).split('T')[1]
        T0 = fdatej + ts.split(':')[0] + ts.split(':')[1]
        tstart = datetime.strptime(T0, "%Y%j%H%M")

        # ---reset LTN for the tile
        if (ltype):
            types = [ltype]
            LTN = {ltype: lightning(ltype, 'GLM')}
        else:
            types = ['flash', 'group', 'event']
            LTN = {'flash': lightning('flash', instr),
                   'group': lightning('group', instr),
                   'event': lightning('event', instr)}

        # ---get lightning data and load up LTN
        filesGLM, s3list = GLMfiles(s3bucket, fdate, tstart, Trng, s3list)
        print('No. of GLM files in current tile: ', len(filesGLM))

        if (len(filesGLM) == 0): continue  # <---no data, skip it

        itile += 1
        folder2 = folder + 'tile' + str(itile) + '/'
        mkfolder(folder2)

        tileset = Tileset(bigbox, sec1, instr)

        epoch = int(sec1 - Dt_show)
        end = int(sec2 + Dt_show)
        tileset.epoch = sec2Z(epoch)
        tileset.end = sec2Z(end)

        print('tile' + str(tile) + ', ' + str(itile) + ': "' + sec2Z(epoch) + '/' + sec2Z(end) + '"')

        log['tile' + str(itile)] = sec2Z(epoch) + '/' + sec2Z(end)

        print('Accessing GLM from S3...')

        for filenc in filesGLM:

            file = s3FileObj(s3bucket, filenc, verb=False)

            with xr.open_dataset(file) as ds:
                for typ in types:
                    LTN[typ].Ltndata(ds, usebox[dictkey(usetype, typ)])

        print(f'End of GLM access for tile {itile}')

        # ---Coord. transfer
        for typ in types:
            if len(LTN[typ].Lon) > 0: LTN[typ].cartographic_to_cartesian()

        # ---Making point cloud for each step
        for step in steps:
            Ldata = LTN[usetype[step]]
            skip = useskip[step]

            if (len(Ldata.Lon) == 0): continue

            MK_cloud_czml(itile, step, tileset, Ldata, skip, folder2)

        # print(tileset_json)
        outfile = open(folder2 + "/tileset.json", "w")
        outfile.write(json.dumps(tileset.json))
        outfile.close()

    logfile = open(folder + "/logfile.json", "w")
    logfile.write(json.dumps(log))
    logfile.close()

    ##########################
    # GLM tile czml
    ##########################

    fromLogFile = False
    if (fromLogFile):
        f = open(folder + "/logfile.json")
        log = json.load(f)
        f.close()

    czmlBody = [{"id": "document",
                 "name": "GLM Lightning",
                 "version": "1.0", }]

    print(czmlBody)

    for tile, tstr in log.items():
        packet = {"id": tile,
                  "availability": tstr,
                  "tileset": {"uri": s3uri + fdate + "/glm_czml_1min/" + tile + "/tileset.json", },
                  }
        czmlBody.append(packet)

    GLMczml = json.dumps(czmlBody)
    filename = folder + "GLM_tiles.czml"
    CZMLfile = open(filename, "w")
    CZMLfile.write(GLMczml)
    CZMLfile.close()

    os.system(
        f"aws s3 sync {folder} {s3path}/")


dates = ['2017-04-11', '2017-04-13', '2017-04-16', '2017-04-18', '2017-04-20', '2017-04-22', '2017-05-07',
         '2017-05-08', '2017-05-12', '2017-05-14', '2017-05-17']

for fdate in dates:
    makePointCloud(fdate)
