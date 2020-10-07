from utils.ingest_utils import *
import json

class lightning:
    def __init__(self, ltype, instr):
        self.instr = instr
        self.ltype = ltype
        self.Lon = []
        self.Lat = []
        self.Alt = []
        self.Time = []
        self.Rad = []
        self.offset = []
        self.scale = []
        self.region = []
        self.cartesian = None
        self.cartographic = None

    def Ltndata(self, ds, regionbox):
        lonW, latS, lonE, latN, altb, altu = regionbox

        if (ds['number_of_events'].size > 0):
            lat, lon, time, rad = get_GLM(self.ltype, ds)

            mask = np.where((lon > lonW) & (lon < lonE) & (lat > latS) & (lat < latN))

            self.Time = np.append(self.Time, time[mask])
            self.Lat = np.append(self.Lat, lat[mask])
            self.Lon = np.append(self.Lon, lon[mask])
            self.Rad = np.append(self.Rad, rad[mask])
            self.Alt = np.append(self.Alt, (np.random.random(len(lon[mask]))) * altu)  # random height near ground

    def cartographic_to_cartesian(self):
        lon = self.Lon
        lat = self.Lat
        alt = self.Alt

        size = lon.size

        self.cartographic = np.zeros(shape=(size, 3), dtype=np.int16)
        self.cartographic[:, 0] = (lon * 32767 / 180).astype(np.int16)
        self.cartographic[:, 1] = (lat * 32767 / 180).astype(np.int16)
        self.cartographic[:, 2] = (alt / 10).astype(np.int16)

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

        # ----less rigid display
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

        self.cartesian = np.zeros(shape=(size, 3), dtype=np.uint16)
        self.cartesian[:, 0] = (x / scale[0] * 65535.0).astype(np.uint16)
        self.cartesian[:, 1] = (y / scale[1] * 65535.0).astype(np.uint16)
        self.cartesian[:, 2] = (z / scale[2] * 65535.0).astype(np.uint16)

        self.region = [
            float(np.min(lon)),
            float(np.min(lat)),
            float(np.max(lon)),
            float(np.max(lat)),
            float(np.min(alt)),
            float(np.max(alt))
        ]
        self.offset = offset
        self.scale = scale


class Tileset:
    def __init__(self, bigbox, CRStime0, instr):
        self.epoch = datetime.utcfromtimestamp(CRStime0).isoformat()
        self.end = datetime.utcfromtimestamp(CRStime0).isoformat()
        self.json = {
            "asset": {"version": "1.0",
                      "type": instr},
            "root": {"geometricError": 1000000,
                     "refine": "REPLACE",
                     "boundingVolume": {"region": regionrad(bigbox)},
                     "children": []},
            "properties": {"epoch": "{}Z".format(datetime.utcfromtimestamp(CRStime0).isoformat()),
                           "refined": []}}
        self.parent = self.json["root"]


def MK_cloud_czml(tile, step, tileset, Ldata, skip, folder2):  # ,cartesian,cartographic):
    header_length = 28
    magic = np.string_("pnts")
    version = 1
    rgba = [255, 255, 100, 255]  # --for GLM

    parent_tile = tileset.json["root"]

    filename = "{}_{}.pnts".format(tile, step)
    child_tile = {
        "availability": "{}/{}".format(tileset.epoch, tileset.end),
        "geometricError": step * 500,
        "boundingVolume": {"region": Ldata.region},
        "content": {"uri": filename},
        "refine": "REPLACE"}

    if step == 1:
        tileset.json["properties"]["refined"].append(filename)
    else:
        child_tile["children"] = []

    parent_tile["children"].append(child_tile)

    tile_length = 0
    feature_table_binary_byte_length = 0
    batch_table_binary_byte_length = 0
    length = Ldata.Rad[::skip].size

    feature_table_json = {
        "POINTS_LENGTH": length,
        "BATCH_LENGTH": length,
        "BATCH_ID": {
            "byteOffset": 0,
            "componentType": "UNSIGNED_INT"  # "FLOAT" #"UNSIGNED_INT"
        },
        "POSITION_QUANTIZED": {"byteOffset": length * 4},
        "QUANTIZED_VOLUME_OFFSET": Ldata.offset,
        "QUANTIZED_VOLUME_SCALE": Ldata.scale,
        "CONSTANT_RGBA": rgba
    }

    batch_table_json = {
        "location": {
            "byteOffset": 0,  # length * 4,
            "componentType": "SHORT",
            "type": "VEC3"}
    }

    tile_length += header_length

    feature_table_json_min = json.dumps(feature_table_json, separators=(",", ":")) + "       "
    feature_table_trim = (tile_length + len(feature_table_json_min)) % 8
    if feature_table_trim != 0:
        feature_table_json_min = feature_table_json_min[:-feature_table_trim]

    tile_length += len(feature_table_json_min)

    feature_table_binary_byte_length = length * 4 + length * 3 * 2
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

    batch_table_binary_byte_length = length * 0 + length * 2 * 3
    tile_length += batch_table_binary_byte_length
    batch_table_padding = tile_length % 8
    if batch_table_padding != 0:
        batch_table_padding = 8 - batch_table_padding

    tile_length += batch_table_padding

    with open('{}/{}'.format(folder2, filename), mode='wb+') as outfile:
        outfile.write(np.string_(magic).tobytes())
        outfile.write(np.uint32(version).tobytes())
        outfile.write(np.uint32(tile_length).tobytes())
        outfile.write(np.uint32(len(feature_table_json_min)).tobytes())
        outfile.write(np.uint32(feature_table_binary_byte_length + feature_table_padding).tobytes())
        outfile.write(np.uint32(len(batch_table_json_min)).tobytes())
        outfile.write(np.uint32(batch_table_binary_byte_length + batch_table_padding).tobytes())
        outfile.write(np.string_(feature_table_json_min).tobytes())
        outfile.write(np.arange(length, dtype=np.uint32).tobytes())
        outfile.write(Ldata.cartesian[::skip, :].tobytes())
        for _ in range(feature_table_padding):
            outfile.write(np.string_(" ").tobytes())
        outfile.write(np.string_(batch_table_json_min).tobytes())
        outfile.write(Ldata.cartographic[::skip, :].tobytes())
        for _ in range(batch_table_padding):
            outfile.write(np.string_(" ").tobytes())
        outfile.seek(0)
