from zipfile import ZipFile
import boto3
from io import BytesIO
import json

def lambda_handler(event, context):

    def extractAndUpload(zipObj):
        filename = zipObj.filename
        # s3.meta.client.upload_fileobj(zipfile.open(filename), Bucket=bucketName, Key=f'{filename}')
        return "s3://"+bucketName+"/"+filename

    def isNotDir(zipObj): 
        return not zipObj.is_dir()
    
    # print(event)
    bucketName = event["bucketName"]
    keyInBucket = event["keyInBucket"]  
    s3 = boto3.resource('s3')

    s3zipObj = s3.Object(bucket_name=bucketName, key=keyInBucket)
    zipfile = ZipFile(BytesIO(s3zipObj.get()["Body"].read()), 'r')

    return {
        "files": list(map(extractAndUpload, filter(isNotDir, zipfile.infolist())))
    }

print(json.dumps(lambda_handler(
    {        
        "bucketName": "nairey-gogo",
        "keyInBucket": "ace-audio-batch-1.zip"
    }, {}
)))
