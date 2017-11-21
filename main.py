import os
import importlib
import yaml


def get_envs():
  # {key: <is_param>}
  required_envs = {
    'AWS_ACCESS_KEY_ID': False,
    'AWS_SECRET_ACCESS_KEY': False,
    'AWS_REGION_NAME': False,
    'DATASET_DB_URL': False,
    'CORE_URL': False,
    'CORE_API_TOKEN': False,
    'TEAM': True,
    'TEAM_UID': True,
    'PREDICTION': True,
    'PREDICTION_UID': True
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
  split_path_info = config.get(key).split('.')
  function_str = split_path_info.pop()
  module_str = '.'.join(split_path_info)

  if not module_str:
    print('No module specified for config method({}={}).'.format(key, config.get(key)))
    exit(1)

  module = importlib.import_module(module_str)

  if not module:
    print('No module to import at destination: {}'.format(module_str))
    exit(1)

  if not hasattr(module_str, function_str):
    print('No function named {} exists on module {}'.format(module_str, function_str))
    exit(1)

  return getattr(module_str, function_str)


def get_src_mod(src, name):
  return importlib.import_module('{}.{}'.format(src, name))


def perform(team=None, team_uid=None, prediction=None, prediction_uid=None):
  # We need to use importlib.import_module to access our src/ files since src/ will
  # be renamed to <prediction_uid>/ to avoid conflicts with user's project files

  # Get refs to the modules inside our src directory
  uploader = get_src_mod(prediction_uid, 'uploader')
  definitions = get_src_mod(prediction_uid, 'definitions')
  messenger = get_src_mod(prediction_uid, 'messenger')

  # Read the config file in the project
  config_path = getattr(definitions, 'config_path')
  config = read_config(config_path)

  # Get ref to exported train method and execute it
  train_method = get_exported_method(config, key='train')
  train_method()

  # If test method specified, call that as well
  if config.get('test'):
    test_method = get_exported_method(config, key='test')
    test_method()

  # Get trained model path and proper file ext before uploading it to S3
  model_path = config.get('model')

  if '.' in model_path:
    model_file_type = model_path.split('.').pop()
    upload_path = prediction + '.' + model_file_type
  else:
    upload_path = prediction

  bucket = '{}-{}'.format(team, team_uid)

  # We need the absolute path to model file
  abs_model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), model_path))

  # Upload trained model to S3
  uploader.upload(filepath=abs_model_path, upload_path=upload_path, bucket=bucket)

  # Tell Core we're done training
  messenger.send_message({
    'prediction_uid': prediction_uid,
    'status': 'training_done'
  })


if __name__ == '__main__':
  params = get_envs()
  perform(**params)