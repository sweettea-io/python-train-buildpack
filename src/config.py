import os
import yaml


class Config(object):

  def __init__(self, path):
    self.path = path
    self.config = self._read_config()

  def validate_train_config(self):
    validate_truthy = [
      self.train_func,
      self.model_path,
      self.model_upload_criteria,
      self._validate_eval()
    ]

    for validation in validate_truthy:
      if not validation():
        return False

    return True

  def training_section(self):
    return self.config.get('training')

  def train_func(self):
    return (self.training_section() or {}).get('train')

  def test_func(self):
    return (self.training_section() or {}).get('test')

  def eval_func(self):
    return (self.training_section() or {}).get('eval')

  def dataset_section(self):
    return (self.training_section() or {}).get('dataset')

  def dataset_fetch_func(self):
    return (self.dataset_section() or {}).get('fetch')

  def dataset_prepro_func(self):
    return (self.dataset_section() or {}).get('prepro')

  def model_section(self):
    return (self.training_section() or {}).get('model')

  def model_path(self):
    return (self.model_section() or {}).get('path')

  def model_upload_criteria(self):
    return (self.model_section() or {}).get('upload_criteria')

  def eval_determines_model_upload(self):
    return self.model_upload_criteria() == 'eval'

  def _validate_eval(self):
    return not self.eval_determines_model_upload() or self.eval_func()

  def _read_config(self):
    if not os.path.exists(self.path):
      return OSError('No SweetTea config file found.')

    with open(self.path) as f:
      return yaml.load(f)