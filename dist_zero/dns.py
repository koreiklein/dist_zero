class DNSController(object):
  '''
  The interface that nodes will use to manage the DNS for a single domain name.
  '''

  def __init__(self, domain_name, spawner):
    self._domain_name = domain_name
    self._spawner = spawner

  def set_all(self, ip_address):
    '''
    Map all requests for self._domain_name to ip_address.

    :param str ip_address: The ip address to route to.
    '''
    self._spawner.map_domain_to_ip(domain_name=self._domain_name, ip_address=ip_address)
