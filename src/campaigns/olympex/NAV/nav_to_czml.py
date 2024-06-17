"""
nav_to_czml takes in nav data from various aricrafts (er-2 and dc-8) of olympex camapign.
The generated czml can be used to plot the flight track in the CESIUM.
"""

import boto3
import os

from nav_reader_writer import FlightTrackCzmlWriter 
from nav_reader_writer import FlightTrackReader

def data_pre_process(bucket_name="ghrc-fcx-field-campaigns-szg", field_campaign = "Olympex", input_data_dir = "instrument-raw-data", output_data_dir = "instrument-processed-data", instrument_name = "nav_er2", row_name_index_map={}):
    """
    gets raw file path to s3 defined path.
    converts it to czml.
    puts converted file to s3 defined path.

    Args:
        bucket_name (str, optional): source bucket. Defaults to "ghrc-fcx-field-campaigns-szg".
        field_campaign (str, optional): name of field campaign. Case sensitive. Defaults to "Olympex".
        input_data_dir (str, optional): folder name where raw data sits. Case sensitive. Defaults to "instrument-raw-data".
        output_data_dir (str, optional): folder name where converted data will be stored. Case sensitive. Defaults to "instrument-processed-data".
        instrument_name (str, optional): instrument from which data is collected. Defaults to "nav_er2".
        row_name_index_map (hash): Hash formed by the column name as key and its position in the L1 data as value. Needed to know position of data column to take during read.
    """
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/data/"):
        keys.append(obj.key)

    result = keys

    s3_client = boto3.client('s3')
    for infile in result:
        s3_file = s3_client.get_object(Bucket=bucket_name, Key=infile)
        data = s3_file['Body'].iter_lines()
        reader = FlightTrackReader(row_name_index_map)
        reader.read_csv(data)

        writer = FlightTrackCzmlWriter(reader.length)
        writer.set_time(reader.time_window, reader.time_steps)
        writer.set_position(reader.longitude, reader.latitude, reader.altitude)
        writer.set_orientation(reader.roll, reader.pitch, reader.heading)

        output_czml = writer.get_string()
        output_name = os.path.splitext(os.path.basename(infile))[0]
        output_name_wo_time = output_name.split("-")[0];
        outfile = f"{field_campaign}/{output_data_dir}/{instrument_name}/{output_name_wo_time}.czml"
        s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)
        print(infile+" conversion done.")

def er2():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "nav_er2"
    # modify "row_name_index_map" according to the data manual and the data availability
    row_name_index_map = {
        "time": 1,
        "latitude": 2,
        "longitude": 3,
        "altitude": 4,
        "heading": 14,
        "pitch": 16,
        "roll": 17
    }
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, row_name_index_map)

def dc8():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "nav_dc8"
    # modify "row_name_index_map" according to the data manual and the data availability
    row_name_index_map = {
        "time": 1,
        "latitude": 2,
        "longitude": 3,
        "altitude": 5,
        "heading": 14,
        "pitch": 16,
        "roll": 17
    }
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, row_name_index_map)
    
er2()
dc8()