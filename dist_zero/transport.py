''' 
For transporting messages across the network.
'''

import socket
import json

from dist_zero import messages


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
