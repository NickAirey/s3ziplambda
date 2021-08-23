import boto3
from io import BytesIO
from zipfile import ZipFile

def lambda_handler(event, context): 

    s3_paginator = boto3.client('s3').get_paginator('list_objects_v2')
    s3 = boto3.resource('s3')

    def getS3Keys(bucket_name, prefix='/', suffix='', delimiter='/', start_after=''):
        prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
        start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
        # get page of results
        for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
            # iterate over page contents
            for content in page.get('Contents', ()):
                # pass the key name downstream if it matches
                key = content['Key']
                if key.endswith(suffix):
                    print("found key "+content['Key']+" "+str(content['Size']))
                    yield key

    def getFileAndWriteToZip(bucketKey):
        object = s3.Object(bucket_name=bucketName, key=bucketKey)
        # create buffer and read object from s3 into buffer
        objectBuffer = BytesIO(object.get()["Body"].read())
        # seek to end of buffer to show size (should match what s3 index claims)
        objectBuffer.seek(0,2)
        print("retrieved "+bucketKey+" "+str(objectBuffer.tell()))
        # reset buffer and write buffer as new entry in zip file
        objectBuffer.seek(0)
        zipFile.writestr(bucketKey, objectBuffer.read())
        # close buffer
        objectBuffer.close()

    print(event)
    bucketName = event["bucketName"]
    keyPrefixToZip = event["keyPrefixToZip"]
    zipFileKey = event["zipFileKey"]
    keySuffix = event["keySuffix"]

    # create zip file attached to in-memory buffer 
    zipBuffer = BytesIO()
    zipFile = ZipFile(zipBuffer, mode="w")

    # get S3 keys, get ecach file and write to zip
    list(map(getFileAndWriteToZip, getS3Keys(bucketName, prefix=keyPrefixToZip, suffix=keySuffix)))

    # close zip file, keep zipbuffer
    zipFile.close()

    # seek to end of zipbuffer to show size 
    zipBuffer.seek(0,2)
    print("writing "+zipFileKey+" "+str(zipBuffer.tell()))
    # reset zipbuffer and write to s3
    zipBuffer.seek(0)
    s3.meta.client.upload_fileobj(zipBuffer, Bucket=bucketName, Key=f'{zipFileKey}')
    # close zip buffer
    zipBuffer.close()


lambda_handler(
    {        
        "bucketName": "nairey-gogo",
        "keyPrefixToZip": "ace-audio-batch-1/",
        "keySuffix": "json",
        "zipFileKey": "ace-audio-batch-1.out.zip"
    }, {}
)

