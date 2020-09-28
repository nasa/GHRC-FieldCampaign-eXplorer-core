import sys
import os
import zarr
import numpy as np
import json
import datetime as dt
from .tileset import PointCloud



to_rad = np.pi / 180.0
to_deg = 180.0 / np.pi

def generate_point_cloud(variable, epoch, end, zarr_location, point_cloud_folder):

    #out_key = f"{os.getenv('CRS_OUTPUT_FLIGHT_PATH')}/{shortname}"
    #pc_out_key = f"{output_path}/point_cloud"

    '''
    try:
        os.mkdir(out_key)
    except:
        pass
    '''

    try:
        os.mkdir(point_cloud_folder)
    except:
        pass


    store = zarr.DirectoryStore(zarr_location)

    root = zarr.group(store=store)

    chunk_id = root["chunk_id"][:]
    num_chunks = chunk_id.shape[0]
    id = np.argmax(chunk_id[:, 1] > epoch) - 1
    start_id = chunk_id[0 if id < 0 else id, 0]
    id = num_chunks - np.argmax(chunk_id[::-1, 1] < end)
    end_id = chunk_id[id, 0] if id < num_chunks else root["time"].size - 1

    root_epoch = root.attrs["epoch"]
    location = root["location"][start_id:end_id]
    lon = location[:, 0]
    lat = location[:, 1]
    alt = location[:, 2]
    value = root["value"][variable][start_id:end_id]
    time = root["time"][start_id:end_id]

    epoch = epoch - root_epoch
    end = end - root_epoch
    mask = np.logical_and(time >= epoch, time <= end)
    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    value = value[mask]
    time = time[mask]

    point_cloud = PointCloud(point_cloud_folder, lon, lat, alt, value, time, root_epoch)

    for tile in range(int(np.ceil(time.size / 530000))):
        start_id = tile * 530000
        end_id = np.min([start_id + 530000, time.size])
        point_cloud.schedule_task(tile, start_id, end_id)

    point_cloud.start()
    point_cloud.join()

tileset_json = {
	"asset": {
		"version": "1.0",
		"type": "Airborne Radar"
	},
	"root": {
		"geometricError": 1000000,
		"refine" : "REPLACE",
		"boundingVolume": {
            "region": []
        },
        "children": []
	},
    "properties": {
        "epoch": "",
        "refined": []
    }
}

#if __name__ == '__main__':
#    main(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
