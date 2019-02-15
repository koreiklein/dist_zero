'''
Objects to represent various infinities.
'''


def key_to_json(key):
  if key == Min:
    return str(Min)
  elif key == Max:
    return str(Max)
  else:
    return key


def json_to_key(j):
  if j == str(Min):
    return Min
  elif j == str(Max):
    return Max
  else:
    return j


def interval_json(interval):
  return (key_to_json(interval[0]), key_to_json(interval[1]))


def parse_interval(interval_json):
  left, right = interval_json
  return [json_to_key(left), json_to_key(right)]


class _Inf(object):
  def __le__(self, other):
    return other == Max

  def __ge__(self, other):
    return True

  def __lt__(self, other):
    return False

  def __gt__(self, other):
    return other != Max

  @property
  def start(self):
    return 'inf'

  def __str__(self):
    return 'inf'


class _MinusInf(object):
  def __le__(self, other):
    return True

  def __ge__(self, other):
    return other == Min

  def __lt__(self, other):
    return other != Min

  def __gt__(self, other):
    return False

  @property
  def start(self):
    return '-inf'

  def __str__(self):
    return '-inf'


Max = _Inf()
'''Inifinity.  A special object greater than everything else.'''
Min = _MinusInf()
'''Minus Inifinity.  A special object less than everything else.'''
