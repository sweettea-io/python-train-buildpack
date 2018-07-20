import os

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

config_file = '.sweettea.yml'
config_path = base_dir + '/' + config_file

TRAIN_CLUSTER = 'train'
API_CLUSTER = 'api'
BUILD_SERVER_CLUSTER = 'build'

deploy_update_queue = 'deploy-update-queue'