import http.server
import logging
import socket

from dist_zero import settings, errors, messages

logger = logging.getLogger(__name__)


class HttpServer(object):
  '''
  DistZero wrapper around a web server object.
  '''

  def __init__(self, address, on_request):
    '''
    Initialize a server with its address and the handler for get requests.

    :param object address: The `server_address` object.
    :param on_request: A function that will take a `http.server.BaseHTTPRequestHandler` instance
      as its argument, and return the output HTML as a python string.
    '''
    self._address = address

    class handler(http.server.BaseHTTPRequestHandler):
      def _send_disallow_robots_header(self):
        self.send_header('X-Robots-Tag', 'none')

      def _send_body(self, body):
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

      def do_GET(self):
        logger.info("Handling GET request: {requestline}", extra={'requestline': self.requestline})
        if 'favicon' in self.path:
          self.send_response(404)
          self.send_header('Content-Type', 'text/plain')
          self._send_disallow_robots_header()
          self._send_body(bytes('File not found', encoding='UTF-8'))
        else:
          self.send_response(200)
          self.send_header('Content-Type', 'text/html')
          self._send_disallow_robots_header()
          response = on_request(self)
          self._send_body(response)

    self._server = http.server.HTTPServer(('', address['port']), handler)

  def socket(self):
    return self._server.fileno()

  def address(self):
    return self._address

  def receive(self):
    self._server.handle_request()
