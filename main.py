import os
import importlib
import shutil
import sys
import yaml
from sqlalchemy import create_engine


def read_config(config_path):
  with open(config_path) as f:
    config = yaml.load(f)
  
  if type(config) != dict or 'train' not in config or 'model' not in config:
    print('Not training. Invalid config file: {}'.format(config))
    exit(1)
  
  return config


def get_exported_method(config, key=None):
  module_str, function_str = config.get(key).split(':')

  if not module_str:
    print('No module specified for config method({}={}).'.format(key, config.get(key)))
    exit(1)

  module = importlib.import_module(module_str)

  if not module:
    print('No module to import at destination: {}'.format(module_str))
    exit(1)

  if not hasattr(module, function_str):
    print('No function named {} exists on module {}'.format(function_str, module_str))
    exit(1)

  return getattr(module, function_str)


def get_src_mod(name):
  return importlib.import_module('{}.{}'.format(os.environ.get('REPO_UID'), name))


def prepro_data(prepro_method, envs, log_capture, log_queue):
  try:
    # Connect to database holding dataset records
    engine = create_engine(envs.DATASET_DB_URL)
  except BaseException as e:
    print('Error connecting to DATASET_DB_URL: {}, with error {}.'.format(envs.DATASET_DB_URL, e))
    exit(1)

  try:
    # Get all JSON data records
    print('Extracting dataset...')
    data = [r[0] for r in engine.execute('SELECT data FROM {};'.format(envs.DATASET_TABLE_NAME))]
  except BaseException as e:
    print('Error querying dataset data (db_url={}): {}.'.format(envs.DATASET_DB_URL, e))
    exit(1)

  call_exported_method(log_capture, log_queue, 'prepro_data', prepro_method, data)


def call_exported_method(log_capture, log_queue, log_method_name, method, *args, **kwargs):
  # Store reference to old stdout and stderr
  old_stdout = sys.stdout
  old_stderr = sys.stderr

  # Set up streaming of stdout and stderr to a redis queue
  sys.stdout = log_capture(sys.stdout, name=log_queue, method=log_method_name)
  sys.stderr = log_capture(sys.stderr, name=log_queue, method=log_method_name)

  # Execute the exported method (train, test, etc.)
  method(*args, **kwargs)

  # Revert changes to stdout and stderr
  sys.stdout = old_stdout
  sys.stderr = old_stderr


def get_model_file_info(model_path, repo_slug):
  print('Finding trained model at path {}'.format(model_path))

  # Get the absolute path to the model
  abs_model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), model_path))

  if not os.path.exists(abs_model_path):
    print('No trained model at path {}. Not uploading model.'.format(model_path))
    exit(1)

  # Check to see if the model is a folder and needs to be zipped
  if os.path.isdir(abs_model_path):
    model_ext = 'zip'
    path_to_upload = abs_model_path + '.' + model_ext

    # zip model dir
    shutil.make_archive(abs_model_path, model_ext, abs_model_path)
  else:
    path_to_upload = abs_model_path
    model_filename_w_ext = abs_model_path.split('/').pop()

    if '.' in model_filename_w_ext:
      model_ext = model_filename_w_ext.split('.').pop()
    else:
      model_ext = ''

  upload_path = repo_slug

  if model_ext:
    upload_path += ('.' + model_ext)

  return path_to_upload, model_ext, upload_path


def perform():
  # Get exported internal src modules
  envs = get_src_mod('envs').envs
  uploader = get_src_mod('uploader')
  definitions = get_src_mod('definitions')
  messenger = get_src_mod('messenger')
  redis = get_src_mod('pyredis')

  # Read the config file in the project
  config = read_config(definitions.config_path)

  # Define our log redirects
  log_capture = redis.RedisStream
  log_queue = 'train:{}'.format(envs.DEPLOYMENT_UID)

  # Exported method that preprocesses dataset before training
  prepro_data_method = get_exported_method(config, key='prepro_data')

  # Only preprocess dataset if a table name was provided.
  if envs.DATASET_TABLE_NAME and prepro_data_method:
    prepro_data(prepro_data_method, envs, log_capture, log_queue)

  # Get ref to exported train method and execute it
  train_method = get_exported_method(config, key='train')
  call_exported_method(log_capture, log_queue, 'train', train_method)

  # If test method specified, call that as well
  if config.get('test'):
    test_method = get_exported_method(config, key='test')
    call_exported_method(log_capture, log_queue, 'test', test_method)

  # Get trained model path and proper file ext before uploading it to S3
  local_model_path, model_ext, upload_path = get_model_file_info(config.get('model'), envs.REPO_SLUG)

  # Upload trained model to S3
  uploader.upload(filepath=local_model_path,
                  upload_path=upload_path,
                  bucket=envs.S3_BUCKET_NAME)

  # Tell Core we're done training
  messenger.report_done_training({
    'deployment_uid': envs.DEPLOYMENT_UID,
    'update_prediction_model': envs.UPDATE_PREDICTION_MODEL == 'true',
    'model_ext': model_ext
  })


if __name__ == '__main__':
  perform()