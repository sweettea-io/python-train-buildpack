from redis import StrictRedis


class RedisStreamClient(object):

  def __init__(self, address='', password=None, stream_key='', **stream_attrs):
    self.address = address
    self.password = password
    self.stream_key = stream_key
    self.stream_attrs = stream_attrs or {}

    # Split address into host/post
    self.host, self.port = address.rsplit(':', 1)

    # Create redis instance.
    self.redis = StrictRedis(host=self.host,
                             port=int(self.port),
                             password=password)

  def set_stream_attr(self, key, val):
    self.stream_attrs[key] = val

  def stream_info(self, data):
    self.stream_with_level(data, 'info')

  def stream_error(self, data):
    self.stream_with_level(data, 'error')

  def stream_with_level(self, data, level):
    if data == '\n':
      return

    attrs = self.stream_attrs
    attrs['level'] = level

    self.stream(data, **attrs)

  def stream(self, data, **attrs):
    self.redis.xadd(self.stream_key, msg=data, **attrs)
