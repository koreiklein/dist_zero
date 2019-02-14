'''
Objects to represent various infinities.
'''


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


Max = _Inf()
'''Inifinity.  A special object greater than everything else.'''
Min = _MinusInf()
'''Minus Inifinity.  A special object less than everything else.'''
