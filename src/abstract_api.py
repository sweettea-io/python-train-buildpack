import requests


class AbstractApiException(BaseException):

  def __init__(self, message):
    self.message = message


class AbstractApi(object):

  def __init__(self, base_url=None, base_headers=None):
    self.base_url = base_url
    self.base_headers = base_headers or {}

  def get(self, route, **kwargs):
    return self.make_request('get', route, **kwargs)

  def post(self, route, **kwargs):
    return self.make_request('post', route, **kwargs)

  def put(self, route, **kwargs):
    return self.make_request('put', route, **kwargs)

  def delete(self, route, **kwargs):
    return self.make_request('delete', route, **kwargs)

  def make_request(self, method, route, payload=None, headers=None, err_message='Abstract API Response Error'):
    # Get the proper method (get, post, put, or delete)
    request = getattr(requests, method)

    # Combine base_headers with any custom headers provided for this specific request
    all_headers = self.base_headers

    if headers:
      for k, v in headers.items():
        all_headers[k] = headers[k]

    # Build up args for the request
    args = {'headers': all_headers}

    if method in ['get', 'delete']:
      args['params'] = payload or {}
    else:
      args['data'] = payload or {}

    print('Sending request: {}, {} to {}'.format(payload, args, self.base_url + route))

    # Make the request
    # response = request(self.base_url + route, **args)

    # Return the JSON response
    # return self.handle_response(response, err_message)

  @staticmethod
  def handle_response(response, err_message):
    if response.status_code == requests.codes.ok:
      return response.json()
    else:
      raise AbstractApiException('{} (status={})'.format(err_message, response.status_code))