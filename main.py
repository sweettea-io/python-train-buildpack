import os
import importlib
import yaml
import sys
from sqlalchemy import create_engine


def get_envs():
  # {key: <is_param>}
  required_envs = {
    'AWS_ACCESS_KEY_ID': False,
    'AWS_SECRET_ACCESS_KEY': False,
    'AWS_REGION_NAME': False,
    'S3_BUCKET_NAME': True,
    'DATASET_DB_URL': False,
    'DATASET_TABLE_NAME': False,
    'CORE_URL': False,
    'CORE_API_TOKEN': False,
    'PREDICTION': True,
    'PREDICTION_UID': True,
    'DEPLOYMENT_UID': True,
    'REDIS_URL': False,
    'WITH_API_DEPLOY': True
  }

  missing = [k for k in required_envs if k not in os.environ]

  if missing:
    print('Not training. The following env vars not provided: {}'.format(', '.join(missing)))
    exit(1)

  # Only return the envs that we need as params
  return {k.lower(): os.environ.get(k) for k in required_envs if required_envs[k]}


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


def get_src_mod(src, name):
  return importlib.import_module('{}.{}'.format(src, name))


def create_dataset(config):
  create_dataset_method = get_exported_method(config, key='create_dataset')
  db_url = os.environ.get('DATASET_DB_URL')
  table_name = os.environ.get('DATASET_TABLE_NAME')

  try:
    # Connect to database holding dataset records
    engine = create_engine(db_url)
    conn = engine.connect()
  except BaseException as e:
    print('Error connecting to DATASET_DB_URL {} when attempting to create dataset: {}'.format(db_url, e))
    exit(1)

  try:
    # TODO: Prevent SQL Injection here
    data = [r for r in conn.execute('SELECT data FROM {}'.format(table_name))]
  except BaseException as e:
    print('Error querying all records for DB: {}, with error: {}'.format(db_url, e))
    exit(1)

  # Call the provided method and pass in the data
  create_dataset_method(data)


def call_exported_method(method, log_capture=None, log_queue=None):
  # Store reference to old stdout and stderr
  old_stdout = sys.stdout
  old_stderr = sys.stderr

  # Set up streaming of stdout and stderr to a redis queue
  sys.stdout = log_capture(sys.stdout, name=log_queue)
  sys.stderr = log_capture(sys.stderr, name=log_queue)

  # Execute the exported method (train, test, etc.)
  method()

  # Revert changes to stdout and stderr
  sys.stdout = old_stdout
  sys.stderr = old_stderr


def perform(prediction=None, prediction_uid=None, s3_bucket_name=None, deployment_uid=None, with_api_deploy=None):
  # Get refs to the modules inside our src directory
  print('Importing modules...')
  uploader = get_src_mod(prediction_uid, 'uploader')
  definitions = get_src_mod(prediction_uid, 'definitions')
  messenger = get_src_mod(prediction_uid, 'messenger')
  redis = get_src_mod(prediction_uid, 'pyredis')

  # Read the config file in the project
  print('Validating {}...'.format(definitions.config_file))
  config = read_config(definitions.config_path)

  # Fetch all data from DB and pass it into the specified create_dataset method
  print('Pulling data from dataset DB...')
  # create_dataset(config)

  # Define our log redirects
  log_capture = redis.RedisStream
  log_queue = 'train-{}'.format(deployment_uid)

  # Get ref to exported train method and execute it
  print('Executing train method...')
  train_method = get_exported_method(config, key='train')
  call_exported_method(train_method, log_capture=log_capture, log_queue=log_queue)

  # If test method specified, call that as well
  if config.get('test'):
    print('Executing test method...')
    test_method = get_exported_method(config, key='test')
    call_exported_method(test_method, log_capture=log_capture, log_queue=log_queue)

  # Get trained model path and proper file ext before uploading it to S3
  model_path = config.get('model')

  print('Finding trained model at path {}'.format(model_path))

  if '.' in model_path:
    model_file_type = model_path.split('.').pop()
    upload_path = prediction + '.' + model_file_type
  else:
    upload_path = prediction

  # We need the absolute path to model file
  abs_model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), model_path))

  if not os.path.exists(abs_model_path):
    print('No trained model at path {}. Not uploading model.'.format(model_path))
    exit(1)

  # Upload trained model to S3
  print('Uploading trained model...')
  uploader.upload(filepath=abs_model_path,
                  upload_path=upload_path,
                  bucket=s3_bucket_name)

  print('Done training.')

  # Tell Core we're done training
  messenger.report_done_training({
    'deployment_uid': deployment_uid,
    'with_api_deploy': with_api_deploy == 'true'
  })


if __name__ == '__main__':
  params = get_envs()
  perform(**params)