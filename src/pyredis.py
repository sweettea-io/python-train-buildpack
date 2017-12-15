import os
from redis import StrictRedis

redis_url = os.environ.get('REDIS_URL')

if redis_url:
  redis = StrictRedis.from_url(url=redis_url)
else:
  redis = None


class RedisStream(object):
  def __init__(self, stream, name=None):
    self.stream = stream
    self.name = name

  def write(self, data):
    self.stream.write(data)

    if data != '\n':
      redis.xadd(self.name, text=data)

    self.stream.flush()

  def writelines(self, datas):
    self.stream.writelines(datas)
    self.stream.flush()

  def __getattr__(self, attr):
    return getattr(self.stream, attr)