import boto3

s3 = boto3.resource('s3')


def upload(filepath=None, upload_path=None, bucket=None):
  bucket = s3.Bucket(bucket)
  bucket.upload_file(filepath, upload_path)