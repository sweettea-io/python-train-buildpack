import os
import json
from pyredis import redis

deployment_uid = os.environ.get('DEPLOYMENT_UID')


def log(text, user_facing=True):
  print(text)

  if redis and user_facing:
    redis.rpush(deployment_uid, json.dumps({'text': text}))