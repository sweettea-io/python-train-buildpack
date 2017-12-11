import os
from abstract_api import AbstractApi, AbstractApiException

messenger = AbstractApi(base_url=os.environ.get('CORE_URL'),
                        base_headers={'Core-Api-Token': os.environ.get('CORE_API_TOKEN')})


def report_done_training(payload):
  try:
    messenger.put('/deployment/trained', payload=payload)
  except AbstractApiException as e:
    print(e.message)
  except BaseException as e:
    print('Unknown error while reporting to core: {}'.format(e))