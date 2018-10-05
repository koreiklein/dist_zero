import os

from collections import defaultdict

from dist_zero import errors


class LoadBalancer(object):
  '''
  Load balancer class.
  '''

  def __init__(self):
    self._frontend_by_port = {}
    self._started = False
    self._frontends_by_domain = defaultdict(list)

  def _start(self):
    if self._started:
      raise errors.InternalError("LoadBalancer is already started.")

    self._start_and_configure_proxy_process()

    self._started = True

  def new_frontend(self, server_address, height):
    return LoadBalancerFrontend(server_address=server_address, height=height, load_balancer=self)

  def _install_frontend(self, frontend):
    if not self._started:
      self._start()
    self._frontend_by_port[frontend._port] = frontend
    self._frontends_by_domain[frontend._domain].append(frontend)

  def _remove_frontend(self, frontend):
    del self._frontend_by_port[frontend._port]
    self._frontends_by_domain[frontend._domain].remove(frontend)

  def _reconfigure(self):
    if not self._started:
      self._start()
    else:
      self._reconfigure_proxy_process()

  def _haproxy_template_string(self):
    return '''
global
  stats socket /var/run/haproxy.sock mode 600 level admin
  maxconn 4096
  ssl-server-verify none

defaults
  mode http
  stats enable
  stats uri /stats
  balance roundrobin
  option redispatch
  option forwardfor
  option log-health-checks

  log     global

  timeout connect 100ms
  timeout check 40ms
  timeout queue 5s
  timeout client 700ms
  timeout server 330ms
  timeout tunnel  1h

{backends}

{frontends}

{default_frontends}

'''

  def _haproxy_config_filename(self):
    return '/etc/haproxy/haproxy.cfg'

  def _default_frontend_strings(self):
    result = []
    for frontends in self._frontends_by_domain.values():
      if frontends:
        frontends.sort(key=lambda frontend: -frontend._height)
        best = frontends[0]
        result.append(best._default_frontend_string())
    return result

  def _write_haproxy_config(self):
    backend_strings = []
    frontend_strings = []
    for frontend in self._frontend_by_port.values():
      frontend_string, backend_string = frontend._generate_haproxy_strings()
      backend_strings.append(backend_string)
      frontend_strings.append(frontend_string)

    text = self._haproxy_template_string().format(
        backends='\n'.join(backend_strings),
        frontends='\n'.join(frontend_strings),
        default_frontends='\n'.join(self._default_frontend_strings()),
    )
    with open(self._haproxy_config_filename(), 'w') as f:
      f.write(text)

  def _start_and_configure_proxy_process(self):
    enable_command = 'sudo systemctl enable rh-haproxy18-haproxy'
    start_command = 'sudo systemctl start rh-haproxy18-haproxy'

    self._write_haproxy_config()

    os.system(enable_command)
    os.system(start_command)

  def _reconfigure_proxy_process(self):
    reload_command = 'sudo systemctl reload rh-haproxy18-haproxy'

    self._write_haproxy_config()

    os.system(reload_command)


class LoadBalancerFrontend(object):
  '''
  Represents one frontend of many within a load balancer, routing between a set of backends.
  '''

  def __init__(self, server_address, height, load_balancer):
    self._server_address = server_address
    self._height = height
    self._load_balancer = load_balancer

    self._domain = self._server_address['domain']
    self._ip = self._server_address['ip']
    self._port = self._server_address['port']

    self._backends_by_ip = {}
    '''
    Maps ip_address to a pair (server_address, weight)
    '''

    self._changed = False

    self._started = False

  def _start(self):
    if self._started:
      raise errors.InternalError("LoadBalancerFrontend is already started.")

    self._load_balancer._install_frontend(self)

    self._started = True

  def address(self):
    '''
    :return: The server_address for connecting to this frontend.
    '''
    if not self._started:
      self._start()

    return self._server_address

  def __setitem__(self, address, weight):
    '''
    Set or update the weight of a backend server_address.
    sync() must be called for the changes to take effect.
    '''
    self._changed = True
    self._backends_by_ip[address['ip']] = (address, weight)

  def __delitem__(self, address):
    '''
    Remove a server_address as a backend.
    sync() must be called for the changes to take effect.
    '''
    self._changed = True
    del self._backends_by_ip[address['ip']]

  def sync(self):
    '''Sync the recent changes to the actual load balancer process.'''
    if not self._started:
      self._start()

    if self._changed:

      self._load_balancer._reconfigure()

      self._changed = False

  def remove(self):
    '''Remove this entire frontend from the load balancer.'''
    self._load_balancer._remove_frontend(self)

  def _server_strings(self):
    result = []
    for server_address, weight in self._backends_by_ip.values():
      result.append('server {host}:{port} weight {weight}'.format(
          host=server_address['ip'],
          port=server_address['port'],
          weight=weight,
      ))
    return result

  def _backend_name(self):
    return '{}_backend'.format(self._port)

  def _frontend_name(self):
    return '{}_frontend'.format(self._port)

  def _backend_string(self):
    return '''
backend {backend_name}
  balance roundrobin
  {servers}
'''.format(
        backend_name=self._backend_name(),
        servers='\n  '.join(self._server_strings()),
    )

  def _frontend_string(self):
    return '''
frontend {frontend_name}
  bind {domain_name}:{port}
  default_backend {backend_name}
'''.format(
        frontend_name=self._frontend_name(),
        domain_name=self._domain,
        port=self._port,
        backend_name=self._backend_name(),
    )

  def _default_frontend_string(self):
    return '''
frontend {frontend_name}
  bind {domain_name}:80
  default_backend {backend_name}
'''.format(
        frontend_name='{}_default_frontend'.format(self._port),
        domain_name=self._domain,
        backend_name=self._backend_name())

  def _generate_haproxy_strings(self):
    '''
    Generate parts of the haproxy config for this `LoadBalancerFrontend` instance.

    :return: A pair of strings (haproxy_frontend, haproxy_backend) that configure an haproxy frontend and 
      backend respectively for this `LoadBalancerFrontend` instance.

    :rtype: tuple[string]
    '''

    return (self._frontend_string(), self._backend_string())
