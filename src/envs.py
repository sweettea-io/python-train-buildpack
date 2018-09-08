import os


class EnvVars(object):
  required_envs = (
    'AWS_ACCESS_KEY_ID',
    'AWS_REGION_NAME',
    'AWS_SECRET_ACCESS_KEY',
    'LOG_STREAM_KEY',
    'MODEL_STORAGE_URL',
    'MODEL_STORAGE_FILE_PATH',
    'PROJECT_UID',
    'REDIS_ADDRESS',
    'REDIS_PASSWORD'
  )

  def __init__(self, prefix=''):
    self.prefix = prefix
    self.set_envs()

  def set_envs(self):
    for env in self.required_envs:
      key = self.prefix + env

      # Ensure env var exists.
      if key not in os.environ:
        print('{} must be provided as an environment variable in order to run this buildpack.'.format(key))
        exit(1)

      # Make env var accessible as an attribute.
      setattr(self, env, os.environ[key])
