import os

def mkfolder(folder):
    if (not os.path.exists(folder)):
        try:
            os.makedirs(folder)
            print('Success to create %s' % folder)
        except OSError:
            print('Failed to create %s' % folder)
            quit()
    else:
        print('%s already exists' % folder)

def s3_key_exists(client, bucket, key):
    """return the key's size if it exist, else None"""
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=key,
    )

    for obj in response.get('Contents', []):
        if obj['Key'] == key:
            #print(response)
            return True

    return False