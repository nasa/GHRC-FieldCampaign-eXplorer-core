import boto3
import os
from nexrad_czml_writer import NexradCzmlWriter
from helper.conversion_helper import group_by_unique_dates, collectDateTimeRange

s3_client = boto3.client('s3')

locations_coordinates = {
    "katx": [-123.197, 48.735, -121.812, 49.653],
    "klgx": [-124.783, 46.674, -123.45, 47.582],
    "krtx": [-123.619, 45.260, -122.321, 46.172]
}

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, instrument_location):
    # get the instrument data list
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    filenames = [] # here filenames represent keys of s3 object
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/{instrument_location}/olympex"):
        filenames.append(obj.key)
    # remove tilt 5 degree (ELEV_02) data. Only visualizing parallel (ELEV_01) data
    filtered_file_names = [filename for filename in filenames if "ELEV_02" not in filename]
    groupedFilenames = group_by_unique_dates(filtered_file_names)

    # for each grouped data, i.e. for each date, create a czml and upload it.
    for fileGroup in groupedFilenames:
        group_date = fileGroup[0].split("_")[3]
        print(f'Started processing NEXRAD for {group_date}')

        # create czml for each group.
            # The browse images available for download show radar reflectivity within a 1 km and 360 degree area around the radar station.
        height = 1000. #meters
        czml_writer = NexradCzmlWriter(locations_coordinates[instrument_location], height)
        date_time_range = collectDateTimeRange(fileGroup)
        for index, filename in enumerate(fileGroup):
            # insert inside czml
            imagery_url = f"https://{bucket_name}.s3.amazonaws.com/{filename}"
            avail_start = date_time_range[index][0]
            avail_end = date_time_range[index][1]
            czml_writer.addTemporalImagery(index, imagery_url, avail_start, avail_end)

        # save the czml in s3.
        print('Uploading file')
        # UPLOAD CONVERTED FILES.
        output_czml = czml_writer.get_string()
        output_name = f"olympex_Level2_{group_date}"
        outfile = f"{field_campaign}/{output_data_dir}/{instrument_name}/{instrument_location}/{output_name}.czml"
        s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)
        print(f"NEXRAD czml conversion for {group_date} done.")
    print(f"***All NEXRAD conversion for {instrument_location} Complete!***")


def nexrad_img_to_czml():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "nexrad"
    locations=["katx", "klgx", "krtx"]
    # iterate the data preprocessing over the data collected accross various locations.
    for location in locations:
        data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, location)

nexrad_img_to_czml()