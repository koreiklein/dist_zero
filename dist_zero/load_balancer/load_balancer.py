import os

from collections import defaultdict

from dist_zero import errors, settings


class LoadBalancer(object):
  '''
  A class for managing a load balancer.  The current implementation manages the single
  `haproxy <http://cbonte.github.io/haproxy-dconv/1.9/intro.html>`_ process on the parent operating system.

  Each `MachineController` instance ``M`` provides its `nodes <Node>` access to a single `LoadBalancer` instance
  which they can use to set up `LoadBalancerFrontends <LoadBalancerFrontend>` for routing internet traffic
  to the operating system on which ``M`` runs.

  Given the current architecture, there is no meaningful way to use the `LoadBalancer` class in simulated mode.
  '''

  def __init__(self):
    self._frontend_by_port = {}
    self._started = False
    self._frontends_by_domain = defaultdict(list)
    '''
    Map each domain name to the list of frontends matching on that domain.
    '''

  def _start(self):
    if self._started:
      raise errors.InternalError("LoadBalancer is already started.")

    self._start_and_configure_proxy_process()

    self._started = True

  def new_frontend(self, server_address, height):
    '''
    Return a new `LoadBalancerFrontend` that will bind the given ``server_address``.

    :param object server_address: The address that the new `LoadBalancerFrontend` should bind.
      See `messages.machine.server_address`.
    :param int height: A kind of priority for frontends.  For a given ``server_address``, only the frontend
      with greatest height will receive all incomming messages.
    '''
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

{default_frontend}

# Stats endpoint
listen stats
  bind *:5000
  mode http
  stats uri /stats
  stats auth {haproxy_stats_username}:{haproxy_stats_password}
'''

  def _haproxy_config_filename(self):
    return '/etc/haproxy/haproxy.cfg'

  def _domain_to_default_frontend(self):
    '''
    Return a dict ``result`` such that for each domain ``d`` that has a frontend installed,
    ``result[d]`` is the frontend of greatest height installed on that domain.
    '''
    result = {}
    for domain, frontends in self._frontends_by_domain.items():
      if frontends:
        frontends.sort(key=lambda frontend: frontend._height)
        result[domain] = frontends[-1]
    return result

  def _default_frontend_string(self):
    '''
    haproxy string configuring the frontend for port 80
    This frontend should map each domain to the servers of LoadBalancerFrontend with
    greatest height for that domain.
    '''

    return '''
frontend default
  bind *:80

  {acls}
'''.format(acls='  \n'.join(
        frontend._map_domain_default_string() for frontend in self._domain_to_default_frontend().values()))

  def _write_haproxy_config(self):
    '''Write the current frontend configuration in self to the system haproxy.cfg file'''
    backend_strings = []
    frontend_strings = []
    for frontend in self._frontend_by_port.values():
      frontend_string, backend_string = (frontend._frontend_string(), frontend._backend_string())
      backend_strings.append(backend_string)
      frontend_strings.append(frontend_string)

    text = self._haproxy_template_string().format(
        backends='\n'.join(backend_strings),
        frontends='\n'.join(frontend_strings),
        default_frontend=self._default_frontend_string(),
        haproxy_stats_username=settings.HAPROXY_STATS_USERNAME,
        haproxy_stats_password=settings.HAPROXY_STATS_PASSWORD,
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
    `sync() <LoadBalancerFrontend.sync>` must be called for the changes to take effect.
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

  def _backend_server_strings(self):
    '''the haproxy 'server ...' lines that go in the backend configuration for this `LoadBalancerFrontend` insteance.'''
    result = []
    for i, (server_address, weight) in enumerate(self._backends_by_ip.values()):
      result.append('server {name} {host}:{port} weight {weight}'.format(
          name='server_{}'.format(i),
          host=server_address['ip'],
          port=server_address['port'],
          weight=weight,
      ))
    return result

  def _backend_name(self):
    return '{}_backend'.format(self._port)

  def _frontend_name(self):
    return '{}_frontend'.format(self._port)

  def _map_domain_default_string(self):
    '''
    return the line(s) in the haproxy config to put in the default frontend
    to map this frontend's domain to its backends
    '''
    return 'acl {acl_name} hdr_dom(host) -i {domain}\n  use_backend {backend_name} if {acl_name}'.format(
        acl_name='for_{}'.format(self._port), domain=self._domain, backend_name=self._backend_name())

  def _backend_string(self):
    '''haproxy backend configuration for the `LoadBalancerFrontend` instance.'''
    return '''
backend {backend_name}
  balance roundrobin
  {servers}
'''.format(
        backend_name=self._backend_name(),
        servers='\n  '.join(self._backend_server_strings()),
    )

  def _frontend_string(self):
    '''haproxy frontend configuration for the `LoadBalancerFrontend` instance.'''
    return '''
frontend {frontend_name}
  bind *:{port}
  default_backend {backend_name}
'''.format(
        frontend_name=self._frontend_name(),
        port=self._port,
        backend_name=self._backend_name(),
    )
