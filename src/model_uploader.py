import boto3
import os
import shutil
from .definitions import base_dir
from urlparse import urlparse


def upload(cloud_storage_url=None, rel_local_model_path=None, cloud_model_path=None):
  # Configure S3 Bucket for upload.
  s3 = boto3.resource('s3')
  bucket_name = urlparse(cloud_storage_url).netloc
  bucket = s3.Bucket(bucket_name)

  # Get the local path and cloud path for the model before uploading.
  local_model, cloud_model = get_paths_for_upload(rel_local_model_path, cloud_model_path)

  # Upload model file to S3.
  bucket.upload_file(local_model, cloud_model)


def get_paths_for_upload(rel_local_model_path, cloud_model_path):
  print('Finding trained model at path {}'.format(rel_local_model_path))

  # Get the absolute path to the local model and ensure it exists.
  abs_local_model_path = os.path.join(base_dir, rel_local_model_path)

  if not os.path.exists(abs_local_model_path):
    print('Can\'t find trained model at path {}. Not uploading model.'.format(abs_local_model_path))
    exit(1)

  # Check if the model path is actually a directory and needs to be compressed.
  if os.path.isdir(abs_local_model_path):
    model_ext = 'zip'
    path_to_upload = '.'.join((abs_local_model_path.rstrip('/'), model_ext))

    # zip model dir
    shutil.make_archive(abs_local_model_path, model_ext, abs_local_model_path)
  else:
    path_to_upload = abs_local_model_path

    # Get just the model filename with extension.
    model_filename_w_ext = abs_local_model_path.split('/').pop()

    # Parse the extension out.
    if '.' in model_filename_w_ext:
      model_ext = model_filename_w_ext.split('.').pop()
    else:
      model_ext = ''

  upload_path = cloud_model_path

  # Add extension to cloud model path after final decision has been made about model's file type.
  if model_ext:
    upload_path = '.'.join((upload_path, model_ext))

  return path_to_upload, upload_path


