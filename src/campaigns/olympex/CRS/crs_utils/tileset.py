import os
import json
import numpy as np
from datetime import datetime
from threading import Thread, Lock

to_rad = np.pi / 180.0
to_deg = 180.0 / np.pi

steps = [32, 16, 8, 4, 2, 1]

class PointCloud:
    def __init__(self, key, lon, lat, alt, value, time, epoch):
        self.key = key
        self.lon = lon
        self.lat = lat
        self.alt = alt
        self.time = time
        self.value = value
        self.epoch = epoch
        self.tasks = []
        self.threads = []
        for i in range(10):
            self.threads.append(Thread(target=self.worker_function))
        self.tileset_lock = Lock()
        self.tileset_json = {
        	"asset": {
        		"version": "1.0",
        		"type": "Airborne Radar"
        	},
        	"root": {
        		"geometricError": 1000000,
        		"refine" : "REPLACE",
        		"boundingVolume": {
                    "region": [
                        float(np.min(lon)) * to_rad,
                        float(np.min(lat)) * to_rad,
                        float(np.max(lon)) * to_rad,
                        float(np.max(lat)) * to_rad,
                        float(np.min(alt)) * to_rad,
                        float(np.max(alt)) * to_rad
                    ]
                },
                "children": []
        	},
            "properties": {
                "epoch": "{}Z".format(datetime.utcfromtimestamp(epoch).isoformat()),
                "refined": []
            }
        }


    def worker_function(self):
        while len(self.tasks) > 0:
                tile, start, end = self.tasks.pop()
                print(tile, start, end)
                self.generate(tile, start, end)


    def start(self):
        for t in self.threads:
            t.start()


    def join(self):
        for t in self.threads:
            t.join()
        with open('{}/tileset.json'.format(self.key), mode='w+') as outfile:
            json.dump(self.tileset_json, outfile)


    def schedule_task(self, tile, start, end):
        self.tasks.append((tile, start, end))


    def generate(self, tile, start, end):
        print(tile, start, end)
        parent_tile = self.tileset_json["root"]
        cartesian, offset, scale, cartographic, region = self.cartographic_to_cartesian(start, end)

        value = self.value[start:end]
        time = self.time[start:end]

        epoch = int(np.min(time) + self.epoch - 300)
        epoch = "{}Z".format(datetime.utcfromtimestamp(epoch).isoformat())
        end = int(np.max(time) + self.epoch + 300)
        end = "{}Z".format(datetime.utcfromtimestamp(end).isoformat())

        header_length = 28
        magic = np.string_("pnts")
        version = 1

        for step in steps:
            self.tileset_lock.acquire()
            try:
                filename = "{}_{}.pnts".format(tile, step)
                child_tile = {
                    "availability": "{}/{}".format(epoch, end),
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
                    self.tileset_json["properties"]["refined"].append(filename)
                else:
                    child_tile["children"] = []
                parent_tile["children"].append(child_tile)
                parent_tile = child_tile
            finally:
                self.tileset_lock.release()

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
                "QUANTIZED_VOLUME_SCALE": scale
            }

            batch_table_json = {
                "value": {
                    "byteOffset": 0,
                    "componentType": "FLOAT",
                    "type": "SCALAR"
                },
                "time": {
                    "byteOffset": length * 4,
                    "componentType": "FLOAT",
                    "type": "SCALAR"
                },
                "location": {
                    "byteOffset": length * 8,
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

            batch_table_binary_byte_length = length * 4 * 2 + length * 2 * 3
            tile_length += batch_table_binary_byte_length
            batch_table_padding = tile_length % 8
            if batch_table_padding != 0:
                batch_table_padding = 8 - batch_table_padding
            tile_length += batch_table_padding

            with open('{}/{}'.format(self.key, filename), mode='wb+') as outfile:
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
                for _ in range(feature_table_padding):
                    outfile.write(np.string_(" ").tobytes())
                outfile.write(np.string_(batch_table_json_min).tobytes())
                outfile.write(value[::step].astype(np.float32).tobytes())
                outfile.write(time[::step].astype(np.float32).tobytes())
                outfile.write(cartographic[::step, :].tobytes())
                for _ in range(batch_table_padding):
                    outfile.write(np.string_(" ").tobytes())
                outfile.seek(0)


    def cartographic_to_cartesian(self, start, end):
        lon = self.lon[start:end]
        lat = self.lat[start:end]
        alt = self.alt[start:end]
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
