from dist_zero import recorded, types, concrete_types
from . import expression


def _expr_class_by_name(name):
  result = recorded.__dict__.get(name, None)
  if result is None:
    return expression.__dict__[name]
  else:
    return result


def _type_class_by_name(name):
  result = types.__dict__.get(name, None)
  if result is None:
    return concrete_types.__dict__[name]
  else:
    return result


class ConcreteExpressionDeserializer(object):
  def __init__(self):
    self._type_json_by_id = {}
    self._type_by_id = {}

    self._json_by_id = {}
    self._expr_by_id = {}

  def deserialize_types(self, type_jsons):
    for type_json in type_jsons:
      self._type_json_by_id[type_json['id']] = type_json

    for type_json in type_jsons:
      self.get_type_by_id(type_json['id'])

  def get_type_by_id(self, type_id):
    result = self._type_by_id.get(type_id)
    if result is None:
      result = self._deserialize_type_json(self._type_json_by_id[type_id])
      self._type_by_id[type_id] = result
    return result

  def deserialize(self, expr_jsons):
    for expr_json in expr_jsons:
      self._json_by_id[expr_json['id']] = expr_json

    return [self.get_by_id(expr_json['id']) for expr_json in expr_jsons]

  def _deserialize_expr_json(self, expr_json):
    expr_class = _expr_class_by_name(expr_json['type'])
    result = expr_class.deserialize_json(expr_json['value'], self)
    for spy_key in expr_json['spy_keys']:
      result.spy(spy_key)
    return result

  def _deserialize_type_json(self, type_json):
    type_class = _type_class_by_name(type_json['type'])
    return type_class.deserialize_json(type_json['value'], self)

  def get_by_id(self, expr_id):
    result = self._expr_by_id.get(expr_id, None)
    if result is None:
      result = self._deserialize_expr_json(self._json_by_id[expr_id])
      self._expr_by_id[expr_id] = result

    return result


class ConcreteExpressionSerializer(object):
  '''For serializing a set of related `ConcreteExpression` instances.'''

  def __init__(self):
    self._get_cache = {}
    self._get_type_cache = {}

    self._n_ids = 0

  def _next_id(self):
    self._n_ids += 1
    return self._n_ids

  def type_jsons(self):
    return self._get_type_cache.values()

  def get_type(self, t):
    result = self._get_type_cache.get(t, None)
    if result is None:
      result = {
          'type': t.__class__.__name__,
          'id': self._next_id(),
          'value': t.serialize_json(self),
      }
      self._get_type_cache[t] = result

    return result

  def get_type_id(self, t):
    return self.get_type(t)['id']

  def get(self, expr):
    result = self._get_cache.get(expr, None)
    if result is None:
      result = {
          'type': expr.__class__.__name__,
          'id': self._next_id(),
          'spy_keys': list(expr.spy_keys),
          'value': expr.serialize_json(self),
      }
      self._get_cache[expr] = result

    return result

  def get_id(self, expr):
    return self.get(expr)['id']
