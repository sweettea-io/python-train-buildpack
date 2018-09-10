import importlib
import os
import sys


def get_internal_modules(env_prefix):
  # Construct key for project uid env var.
  project_uid_key = '{}PROJECT_UID'.format(env_prefix)
  project_uid = os.environ.get(project_uid_key)

  if not project_uid:
    print('{} must be provided as an environment variable in order to run this buildpack.'.format(project_uid_key))
    exit(1)

  try:
    config = get_src_mod(project_uid, 'config')
    definitions = get_src_mod(project_uid, 'definitions')
    envs = get_src_mod(project_uid, 'envs')
    logger = get_src_mod(project_uid, 'logger')
    model_uploader = get_src_mod(project_uid, 'model_uploader')
    pyredis = get_src_mod(project_uid, 'pyredis')

    return config, definitions, envs, logger, model_uploader, pyredis
  except BaseException as e:
    print('Internal module reference failed: {}'.format(e))
    exit(1)


def get_src_mod(parent_mod, name):
  return importlib.import_module('{}.{}'.format(parent_mod, name))


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


def call_action_func(func_path, redis, action=''):
  if not func_path: return
  redis.set_stream_attr('action', action)
  return get_config_func(func_path)()


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
  # Get internal environment variable prefix.
  env_prefix = os.environ.get('SWEET_TEA_INTERNAL_ENV_PREFIX', '')

  # Get reference to internal src modules.
  config, definitions, envs, logger, model_uploader, pyredis = get_internal_modules(env_prefix)

  # Create EnvVars instance from environment variables.
  env_vars = envs.EnvVars(prefix=env_prefix)

  # Create Redis stream client for log streaming.
  redis = pyredis.RedisStreamClient(address=env_vars.REDIS_ADDRESS,
                                    password=env_vars.REDIS_PASSWORD,
                                    stream_key=env_vars.LOG_STREAM_KEY,
                                    action='setup')

  # Create custom log streams connected to Redis.
  sys.stdout = logger.Logger(sys.stdout, on_write=redis.stream)
  sys.stderr = logger.Logger(sys.stderr, on_write=redis.stream)

  # Create Config instance from SweetTea config file.
  cfg = get_validated_config(config, definitions.config_path)

  # Perform main actions.
  call_action_func(cfg.dataset_fetch_func(), redis, action='fetch dataset')
  call_action_func(cfg.dataset_prepro_func(), redis, action='preprocess dataset')
  call_action_func(cfg.train_func(), redis, action='train')
  call_action_func(cfg.test_func(), redis, action='test')
  eval_result = call_action_func(cfg.eval_func(), redis, action='eval')

  # Exit if evaluation failed and that's the criteria used to determine if model should be uploaded.
  if not eval_result and cfg.eval_determines_model_upload():
    print('Model did not pass evalution. Not uploading model.')
    return

  redis.set_stream_attr('action', 'upload model')

  # Upload model to cloud storage.
  model_uploader.upload(cloud_storage_url=env_vars.MODEL_STORAGE_URL,
                        rel_local_model_path=cfg.model_path(),
                        cloud_model_path=env_vars.MODEL_STORAGE_FILE_PATH,
                        region_name=env_vars.AWS_REGION_NAME,
                        aws_access_key_id=env_vars.AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=env_vars.AWS_SECRET_ACCESS_KEY)


if __name__ == '__main__':
  perform()
