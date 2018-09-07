import importlib
import os
import sys


def ensure_internal_modules_accessible():
  if not os.environ.get('PROJECT_UID'):
    print('PROJECT_UID must be provided as an environment variable in order to run this buildpack.')
    exit(1)


def get_internal_modules():
  try:
    config = get_src_mod('config')
    definitions = get_src_mod('definitions')
    envs = get_src_mod('envs')
    model_uploader = get_src_mod('model_uploader')
    pyredis = get_src_mod('pyredis')

    return config, definitions, envs, model_uploader, pyredis
  except BaseException as e:
    print('Internal module reference failed: {}'.format(e))
    exit(1)


def get_src_mod(name):
  return importlib.import_module('{}.{}'.format(os.environ.get('PROJECT_UID'), name))


def get_validated_config(config_module, path):
  try:
    cfg = config_module.Config(path)

    if not cfg.validate_train_config():
      raise BaseException('Invalid SweetTea configuration file.')

    return cfg
  except BaseException as e:
    print(e)
    exit(1)


def get_config_func(func_path):
  # Split function path into ['<module_path>', '<function_name>']
  module_path, func_name = func_path.rsplit(':', 1)

  if not module_path:
    print('No module included in config function path: {}.'.format(func_path))
    exit(1)

  # Import module by path.
  mod = importlib.import_module(module_path)

  # Ensure module exists.
  if not mod:
    print('No module to import at destination: {}'.format(module_path))
    exit(1)

  # Ensure function exists on module.
  if not hasattr(mod, func_name):
    print('No function named {} exists on module {}'.format(func_name, module_path))
    exit(1)

  # Return reference to module function.
  return getattr(mod, func_name)


def call_config_func(action=None, func_path=None, redis=None, stream_capture=None, log_stream_key=None):
  # Get reference to function from path.
  func = get_config_func(func_path)

  # Store reference to old stdout and stderr.
  old_stdout = sys.stdout
  old_stderr = sys.stderr

  # Set up streaming of stdout and stderr to a redis stream.
  sys.stdout = stream_capture(sys.stdout, redis, stream_key=log_stream_key, action=action, level='info')
  sys.stderr = stream_capture(sys.stderr, redis, stream_key=log_stream_key, action=action, level='error')

  # Execute the config function.
  result = func()

  # Revert changes to stdout and stderr
  sys.stdout = old_stdout
  sys.stderr = old_stderr

  return result


def perform():
  """
  Perform the following ML actions:

    1. Fetch dataset (if specified)
    2. Preprocess dataset (if specified)
    3. Train model
    4. Test model
    5. Evaluate model against custom criteria (if specified)
    6. Upload model to cloud storage
  """
  # Get reference to internal src modules.
  config, definitions, envs, model_uploader, pyredis = get_internal_modules()

  # Create EnvVars instance from environment variables.
  env_vars = envs.EnvVars()

  # Create Redis instance for log streaming.
  redis = pyredis.new_redis(address=env_vars.REDIS_ADDRESS,
                            password=env_vars.REDIS_PASSWORD)

  # Create Config instance from SweetTea config file.
  cfg = get_validated_config(config, definitions.config_path)

  # Create dict of generic kwargs for each config function call.
  func_agnostic_kwargs = {
    'redis': redis,
    'stream_capture': pyredis.RedisStream,
    'log_stream_key': env_vars.LOG_STREAM_KEY
  }

  # Get function paths from config.
  fetch_dataset = cfg.dataset_fetch_func()
  prepro_dataset = cfg.dataset_prepro_func()
  train_model = cfg.train_func()
  test_model = cfg.test_func()
  eval_model = cfg.eval_func()

  # Fetch dataset (if configured to do so).
  if fetch_dataset:
    call_config_func(action='fetch dataset', func_path=fetch_dataset, **func_agnostic_kwargs)

  # Preprocess dataset (if configured to do so).
  if prepro_dataset:
    call_config_func(action='preprocess dataset', func_path=prepro_dataset, **func_agnostic_kwargs)

  # Train model.
  call_config_func(action='train', func_path=train_model, **func_agnostic_kwargs)

  # Test model (if configured to do so).
  if test_model:
    call_config_func(action='test', func_path=test_model, **func_agnostic_kwargs)

  # Evaluate model against custom criteria (if configured to do so).
  if eval_model:
    passed_eval = call_config_func(action='eval', func_path=eval_model, **func_agnostic_kwargs)

    # Exit if evaluation failed and that's the criteria used to determine if model should be uploaded.
    if not passed_eval and cfg.eval_determines_model_upload():
      print('Model did not pass evalution. Not uploading model.')
      exit(1)

  # Upload model to cloud storage.
  model_uploader.upload(cloud_storage_url=env_vars.MODEL_STORAGE_URL,
                        rel_local_model_path=cfg.model_path(),
                        cloud_model_path=env_vars.MODEL_STORAGE_FILE_PATH)


if __name__ == '__main__':
  ensure_internal_modules_accessible()
  perform()
