''' 
For transporting messages across the network.
'''

import socket
import json
import logging

from dist_zero import messages

logger = logging.getLogger(__name__)


def send_udp(message, dst):
  '''
  Send a message via UDP

  :param object message: A json seralizable message.
  :param tuple dst: A pair (host, port) where host is a `str` and port an `int`

  :return: `None`
  '''
  binary = bytes(json.dumps(message), messages.ENCODING)
  with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
    sock.sendto(binary, dst)
    return None


def send_tcp(message, dst):
  binary = bytes(json.dumps(message), messages.ENCODING)
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect(dst)
    sock.send(binary)
    logger.debug("message sent to MachineControllre tcp API on {dst_host}", extra={'dst_host': dst[0]})
    response = sock.recv(settings.MSG_BUFSIZE)
    logger.debug("received MachineController API response message from {dst_host}", extra={'dst_host': dst[0]})
    msg = json.loads(response.decode(messages.ENCODING))
    if msg['status'] == 'ok':
      return msg['data']
    else:
      raise errors.InternalError("Failed to communicate over TCP api to MachineController. reason: {}".format(
          msg.get('reason', '')))
