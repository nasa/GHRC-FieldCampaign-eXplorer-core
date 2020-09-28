import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import os.path

def download_s3(bucket, s3key, dirPath, filename=None):
    """download a file from an S3 bucket to local disk
     bucket: S3 bucket when download file is located
     s3key: name of file (full name with all sub-dirs) to be downloaded
     dirPath: local folder where downloaded file to be saved
     filename: saved filename. If not given, use same name as in S3
    """
    s3 = boto3.client('s3')
    if (not filename):
        filename = s3key.split('/')[-1]
    print("downloading " + s3key.split('/')[-1] + ' ...')
    s3.download_file(bucket, s3key, os.path.join(dirPath, filename))


def downloadBatch_s3(bucket, s3prefix, dirPath):
    """download a file from an S3 bucket to local disk
     bucket: S3 bucket when download file is located
     s3prefix: prefixe of files (full name with sub-dirs) to be downloaded
     dirPath: local folder where downloaded file to be saved
     downloaded files would have same names as in S3
    """

    flist = s3list(bucket, s3prefix)

    s3 = boto3.client('s3')
    for key in flist:
        print("downloading " + key.split('/')[-1] + ' ...')
        destination_file = os.path.join(dirPath, key.split('/')[-1])
        if not os.path.isfile(destination_file):
            s3.download_file(bucket, key, os.path.join(dirPath, key.split('/')[-1]))
        else:
            pass
            #print(f'file {destination_file} already exists')


def upload_to_s3(file_name, bucket, s3_name=None):
    """Upload a file to an S3 bucket
     file_name: File to upload
     bucket: S3 bucket to upload to
     object_name: S3 object name. If not specified then file_name is used
    """
    if s3_name is None: s3_name = file_name

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, s3_name)
    except ClientError as e:
       print(e)
    except NoCredentialsError:
        print("%%Credentials not available")


def s3list(s3bucket, prefix):
    """
    get list of files in our s3 bucket
    boto3.resource('s3') is high level s3 service
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3bucket)
    keys = []
    for obj in bucket.objects.filter(Prefix=prefix):
        keys.append(obj.key)
    return keys


def S3list(s3bucket, fdate, instrm, network='OKLMA'):
    """
    get list of files in a s3 bucket for a specific fdate and instrument (prefix)
    fdate: e.g. '2017-05-17'
    instrm: e.g. 'GLM'
    """
    prefix = {'GLM': 'GLM/data/L2/' + fdate + '/OR_GLM-L2-LCFA_G16',
              'ABI': 'ABI/data/' + fdate + '/C13/OR_ABI-L1b-RadC-M3C13_G16_s' + dthead,
              'LIS': 'ISS_LIS/data/' + fdate + '/ISS_LIS_SC_V1.0_',
              'FEGS': 'FEGS/data/goesr_plt_FEGS_' + fdate.replace('-', '') + '_Flash',
              'CRS': 'CRS/data/GOESR_CRS_L1B_' + fdate.replace('-', ''),
              'NAV': 'NAV_ER2/data/goesrplt_naver2_IWG1_' + fdate.replace('-', ''),
              'LMA': 'LMA/' + network + '/data/' + fdate + '/goesr_plt_' + network + '_' + fdate.replace('-', '')}

    print("S3list searching for ", prefix[instrm])

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3bucket)
    keys = []
    for obj in bucket.objects.filter(Prefix=prefix[instrm]):
        keys.append(obj.key)
    return keys


def s3obj(s3bucket, prefix):
    """use low level boto3.client() to get s3 objects"""
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=s3bucket)
    for content in response['Contents']:
        obj = s3.get_object(Bucket=s3bucket, Key=content['Key'])
        print(content['Key'], obj['LastModified'])


def cpmv_s3(s3bucket, oldFolder, newFolder, action='cp'):
    """
    cp(copy) or mv(rename) an s3 dir
    Note that S3 only has copy or remove/delete functions.
    This function provides a way for rename a file or a dir in s3.
    """

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3bucket)

    for obj in bucket.objects.filter(Prefix=oldFolder):
        srcKey = obj.key
        if not srcKey.endswith('/'):
            print('Copying/Moving from ', srcKey)
            subpath = srcKey[len(oldFolder):]
            destPath = newFolder + subpath
            Source = s3bucket + '/' + srcKey
            s3.Object(s3bucket, destPath).copy_from(CopySource=Source)
            if (action == 'mv'):
                s3.Object(s3bucket, srcKey).delete()
