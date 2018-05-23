'''
This simple module will read a message from a local file, send it to a dist_zero.machine_init
process running on the same host, and print the results to stdout.

This behavior is useful for simulating a message send for a dist_zero.machine_init process when it's running
in a container.
'''
import json
import logging
import os
import socket
import sys

from dist_zero import errors, settings, transport
from dist_zero.spawners import docker

logger = logging.getLogger(__name__)


def _run():
  if len(sys.argv) != 3:
    raise errors.DistZeroError("send_local_msg_from_file should be called with exactly 2 argvs")
  filename = sys.argv[1]
  sock_type = sys.argv[2]

  full_path = os.path.join(docker.DockerSpawner.CONTAINER_MESSAGE_DIR, filename)

  logger.info("reading message from file {} and forwarding to localhost".format(full_path), extra={'path': full_path})
  with open(full_path, 'r') as f:
    message = json.load(f)

  if sock_type == 'udp':
    host = 'localhost'
    dst = (host, settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT)
    transport.send_udp(message, dst)
    logger.info("local file message sent to localhost")
  elif sock_type == 'tcp':
    host = 'localhost'
    dst = (host, settings.MACHINE_CONTROLLER_DEFAULT_TCP_PORT)
    response = transport.send_tcp(message, dst)
    with open(docker.DockerSpawner.CONTAINER_MESSAGE_RESPONSE_TEMPLATE.format(full_path), 'w') as f_out:
      json.dump(response, f_out, indent=2)
  else:
    logger.error(
        "Unrecognized socket type {unrecognized_sock_type} should be 'udp' or 'tcp'",
        extra={'unrecognized_sock_type': sock_type})


if __name__ == '__main__':
  _run()
