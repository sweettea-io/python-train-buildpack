
class Logger(object):

  def __init__(self, sys_stream, on_write=None):
    self.sys_stream = sys_stream
    self.on_write = on_write

  def write(self, data):
    self.sys_stream.write(data)

    if self.on_write:
      self.on_write(data)

    self.sys_stream.flush()

  def writelines(self, data):
    self.sys_stream.writelines(data)
    self.sys_stream.flush()

  def __getattr__(self, attr):
    return getattr(self.sys_stream, attr)