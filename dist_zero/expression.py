class Expression(object):
  def __repr__(self):
    return str(self)


class Prim(Expression):
  def __init__(self, op):
    self.op = op

  def __str__(self):
    return str(self.op)


class Applied(Expression):
  def __init__(self, func, arg):
    self.func = func
    self.arg = arg

  def __str__(self):
    return f"{self.func}({self.arg})"


class Product(Expression):
  def __init__(self, items):
    self.items = items

  def __str__(self):
    items = ', '.join(f"{key}: {value}" for key, value in self.items)
    return f"{{{items}}}"


class Input(Expression):
  def __init__(self, name, type):
    self.name = name
    self.type = type

  def __str__(self):
    return f"Input_{self.name}"
