from zipfile import ZipFile
import boto3
from io import BytesIO
import json
from datetime import datetime
import time

def lambda_handler(event, context):

    def processZipObj(zipObj):
        filename = zipObj.filename
        s3.meta.client.upload_fileobj(zipfile.open(filename), Bucket=bucketName, Key=f'{filename}')
        return {
            "mediaUri": "s3://"+bucketName+"/"+filename,
            "date": str(datetime.fromtimestamp(time.mktime(zipObj.date_time + (0,0,0))))
        }

    def isNotDir(zipObj): 
        return not zipObj.is_dir()
    
    print(event)
    bucketName = event["bucketName"]
    keyInBucket = event["keyInBucket"]  
    s3 = boto3.resource('s3')

    s3zipObj = s3.Object(bucket_name=bucketName, key=keyInBucket)
    zipfile = ZipFile(BytesIO(s3zipObj.get()["Body"].read()), 'r')

    return {
        "audioFiles": list(map(processZipObj, filter(isNotDir, zipfile.infolist())))
    }

print(json.dumps(lambda_handler(
    {        
        "bucketName": "nairey-gogo",
        "keyInBucket": "ace-audio-batch-1.zip"
    }, {}
)))
