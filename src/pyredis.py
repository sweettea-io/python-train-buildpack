from redis import StrictRedis


def new_redis(address='', password=None):
  # Split address into host/post
  host, port = address.rsplit(':', 1)

  # Assign new StrictRedis instance to global redis var.
  return StrictRedis(host=address, port=int(port), password=password)


class RedisStream(object):

  def __init__(self, sys_stream, redis_instance, stream_key=None, **kwargs):
    self.sys_stream = sys_stream
    self.redis_instance = redis_instance
    self.stream_key = stream_key
    self.kwargs = kwargs

  def write(self, data):
    # Write data to system stream that's being wrapped.
    self.sys_stream.write(data)

    # Ignore newlines only
    if data != '\n':
      # XADD log entry to Redis stream.
      self.redis_instance.xadd(self.stream_key, msg=data, **self.kwargs)

    self.stream.flush()

  def writelines(self, datas):
    self.stream.writelines(datas)
    self.stream.flush()

  def __getattr__(self, attr):
    return getattr(self.stream, attr)
