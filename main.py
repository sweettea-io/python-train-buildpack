import os
import importlib
import yaml
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


def read_config(config_path, logger=None):
  with open(config_path) as f:
    config = yaml.load(f)
  
  if type(config) != dict or 'train' not in config or 'model' not in config:
    logger.log('Not training. Invalid config file: {}'.format(config))
    exit(1)
  
  return config


def get_exported_method(config, key=None, logger=None):
  module_str, function_str = config.get(key).split(':')

  if not module_str:
    logger.log('No module specified for config method({}={}).'.format(key, config.get(key)))
    exit(1)

  module = importlib.import_module(module_str)

  if not module:
    logger.log('No module to import at destination: {}'.format(module_str))
    exit(1)

  if not hasattr(module, function_str):
    logger.log('No function named {} exists on module {}'.format(function_str, module_str))
    exit(1)

  return getattr(module, function_str)


def get_src_mod(src, name):
  return importlib.import_module('{}.{}'.format(src, name))


def create_dataset(config, logger=None):
  create_dataset_method = get_exported_method(config, key='create_dataset')
  db_url = os.environ.get('DATASET_DB_URL')
  table_name = os.environ.get('DATASET_TABLE_NAME')

  try:
    # Connect to database holding dataset records
    engine = create_engine(db_url)
    conn = engine.connect()
  except BaseException as e:
    logger.log('Error connecting to DATASET_DB_URL {} when attempting to create dataset: {}'.format(db_url, e))
    exit(1)

  try:
    # TODO: Prevent SQL Injection here
    data = [r for r in conn.execute('SELECT data FROM {}'.format(table_name))]
  except BaseException as e:
    logger.log('Error querying all records for DB: {}, with error: {}'.format(db_url, e))
    exit(1)

  # Call the provided method and pass in the data
  create_dataset_method(data)


def perform(prediction=None, prediction_uid=None, s3_bucket_name=None, deployment_uid=None, with_api_deploy=0):
  # Get refs to the modules inside our src directory
  logger = get_src_mod(prediction_uid, 'logger')
  logger.log('Importing modules...')
  uploader = get_src_mod(prediction_uid, 'uploader')
  definitions = get_src_mod(prediction_uid, 'definitions')
  messenger = get_src_mod(prediction_uid, 'messenger')

  # Read the config file in the project
  logger.log('Validating {}...'.format(definitions.config_file))
  config = read_config(definitions.config_path, logger=logger)

  # Fetch all data from DB and pass it into the specified create_dataset method
  logger.log('Pulling data from dataset DB...')
  # create_dataset(config, logger=logger)

  # Get ref to exported train method and execute it
  logger.log('Executing train method...')
  train_method = get_exported_method(config, key='train', logger=logger)
  train_method()

  # If test method specified, call that as well
  if config.get('test'):
    logger.log('Executing test method...')
    test_method = get_exported_method(config, key='test', logger=logger)
    test_method()

  # Get trained model path and proper file ext before uploading it to S3
  model_path = config.get('model')

  logger.log('Finding trained model at path {}'.format(model_path))

  if '.' in model_path:
    model_file_type = model_path.split('.').pop()
    upload_path = prediction + '.' + model_file_type
  else:
    upload_path = prediction

  # We need the absolute path to model file
  abs_model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), model_path))

  if not os.path.exists(abs_model_path):
    logger.log('No trained model at path {}. Not uploading model.'.format(model_path))
    exit(1)

  # Upload trained model to S3
  logger.log('Uploading trained model...')
  uploader.upload(filepath=abs_model_path,
                  upload_path=upload_path,
                  bucket=s3_bucket_name)

  logger.log('Done training.')

  # Tell Core we're done training
  messenger.report_done_training({
    'deployment_uid': deployment_uid,
    'with_api_deploy': bool(with_api_deploy)
  })


if __name__ == '__main__':
  params = get_envs()
  perform(**params)