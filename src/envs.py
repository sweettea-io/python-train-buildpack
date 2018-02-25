import os


class Envs(object):
  required_envs = (
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_REGION_NAME',
    'S3_BUCKET_NAME',
    'DATASET_DB_URL',
    'DATASET_TABLE_NAME',
    'CORE_URL',
    'CORE_API_TOKEN',
    'REPO_SLUG',
    'REPO_UID',
    'DEPLOYMENT_UID',
    'REDIS_URL',
    'UPDATE_PREDICTION_MODEL'
  )

  def __init__(self):
    self.set_envs()

  def set_envs(self):
    for env in self.required_envs:
      # Ensure env var exists
      if env not in os.environ:
        print('Not starting API. Required env var {} not provided.'.format(env))
        exit(1)

      # Make env var accessible as an attribute
      setattr(self, env, os.environ[env])


envs = Envs()