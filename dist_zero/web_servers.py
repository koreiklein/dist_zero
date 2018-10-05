from dist_zero import settings, errors


class HttpServer(object):
  def __init__(self, address, socket, on_request):
    self._address = address
    self._socket = socket
    self._on_request = on_request

  def address(self):
    return self._address

  def receive(self):
    client_sock, client_addr = self._socket.accept()
    buf = client_sock.recv(settings.MSG_BUFSIZE)
    self._on_request(buf)
