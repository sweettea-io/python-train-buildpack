import os
import importlib
import yaml


def get_envs():
  env = os.environ

  # Required ENV vars
  required = ['TEAM', 'TEAM_UID', 'PREDICTION', 'PREDICTION_UID']
  missing = [k for k in required if k not in env]

  if missing:
    print('Not training. The following env vars not provided: {}'.format(', '.join(missing)))
    exit(1)

  return {k.lower(): env.get(k) for k in required}


def read_config(config_path):
  with open(config_path) as f:
    config = yaml.load(f)
  
  if type(config) != dict or 'train' not in config or 'model' not in config:
    print('Not training. Invalid config file: {}'.format(config))
    exit(1)
  
  return config


def get_train_method(config):
  split_path_info = config.get('train').split('.')
  train_func_str = split_path_info.pop()
  train_mod_str = '.'.join(split_path_info)

  if not train_mod_str:
    print('No module specified for training. Only the function was specified.')
    exit(1)

  train_mod = importlib.import_module(train_mod_str)

  if not train_mod:
    print('No module to import at destination: {}'.format(train_mod_str))
    exit(1)

  if not hasattr(train_mod, train_func_str):
    print('No function named {} exists on module {}'.format(train_func_str, train_mod_str))
    exit(1)

  return getattr(train_mod, train_func_str)


def get_src_mod(src, name):
  return importlib.import_module('{}.{}'.format(src, name))


def perform(team=None, team_uid=None, prediction=None, prediction_uid=None):
  # Get refs to the modules inside our src directory
  uploader = get_src_mod(prediction_uid, 'uploader')
  definitions = get_src_mod(prediction_uid, 'definitions')
  messenger = get_src_mod(prediction_uid, 'messenger')

  # Read the provided config file
  config = read_config(getattr(definitions, 'config_path'))

  # Get exported train method and call it
  train_method = get_train_method(config)
  train_method()

  # Upload trained model to S3
  model_path = config.get('model')

  if '.' in model_path:
    model_file_type = model_path.split('.').pop()
    upload_path = prediction + '.' + model_file_type
  else:
    upload_path = prediction

  bucket = '{}-{}'.format(team, team_uid)

  abs_model_path = os.path.abspath(os.path.join(os.path.dirname(__file__), model_path))
  uploader.upload(filepath=abs_model_path, upload_path=upload_path, bucket=bucket)

  # Tell Core we're done building
  messenger.send_message({
    'done': True,
    'from': getattr(definitions, 'TRAIN_CLUSTER'),
    'prediction_uid': prediction_uid
  })


if __name__ == '__main__':
  params = get_envs()
  perform(**params)