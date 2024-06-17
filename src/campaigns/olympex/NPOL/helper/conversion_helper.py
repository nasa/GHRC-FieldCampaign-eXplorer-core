def collectAvailabilityDateTimeRange(fileGroup):
    """
    Each filegroup (i.e. nc files, collected in a day) has various npol uf files throughout that particular day.
    A NPOL uf file is collected in certain frequency. (20 mins)
    This function gets the start and end time for each nexrad file.

    Args:
        fileGroup (array): array of string filenames. filenames should be sorted.
                           Note filename has a filename format that contains date and time when the NPOL data was collected
    Returns:
        array: for each mapped filename in a filegroup, returns a array of start and end date time.
        eg: for [['path_to/npol/1105/rhi_a/olympex_NPOL1_20151105_161848_rhi_00-20.uf.gz', 'path_to/npol/1105/rhi_a/olympex_NPOL1_20151105_170127_rhi_00-20.uf.gz']] as input
        returns [['2015-11-05T16:18:48Z', '2015-11-05T17:01:27Z'], ['2015-11-05T17:01:27Z', '2015-11-05T17:21:27Z']]
    """
    result = []
    end_index = len(fileGroup) - 1
    date = fileGroup[0].split("/")[-1].split("_")[2]
    formatted_date = '{}-{}-{}'.format(date[:4], date[4:6], date[6:])
    for index, filename in enumerate(fileGroup):
        starttime = filename.split("/")[-1].split("_")[3]
        if (index == end_index):
            endtime = str((int(starttime) + 2000) if (int(starttime) + 2000) < 235900  else 235900 ) # every npol uf data has temporal resolution of 20 minutes 00 secs
        else:
            endtime = fileGroup[index + 1].split("/")[-1].split("_")[3]
        formatted_start_time = '{}:{}:{}'.format(starttime[:2], starttime[2:4], starttime[4:])
        formatted_end_time = '{}:{}:{}'.format(endtime[:2], endtime[2:4], endtime[4:])
        # finalizing final date time in czml date format i.e.2015-09-22T22:38:00Z
        start_date_time = f"{formatted_date}T{formatted_start_time}Z"
        end_date_time = f"{formatted_date}T{formatted_end_time}Z"
        result.append([start_date_time, end_date_time])
    return result