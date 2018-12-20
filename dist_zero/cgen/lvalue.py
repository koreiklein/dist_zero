class Lvalue(object):
  def add_includes(self, program):
    raise RuntimeError(f"Abstract superclass {self.__class__}")

  def Deref(self):
    return self.to_component_lvalue().Deref()

  def Dot(self, name):
    return self.to_component_lvalue().Dot(name)

  def Arrow(self, name):
    return self.to_component_lvalue().Arrow(name)

  def Sub(self, i):
    return self.to_component_lvalue().Sub(i)

  def to_component_lvalue(self):
    return ComponentLvalue(self, [])


class ComponentLvalue(Lvalue):
  def __init__(self, base_var, accessors):
    self.base_var = base_var
    self.accessors = accessors

  def add_includes(self, program):
    self.base_var.add_includes(program)

  def to_c_string(self):
    result = self.base_var.to_c_string()
    for accessor in self.accessors:
      result = accessor.access_variable(result)

    return result

  def _extend(self, accessor):
    accessors = list(self.accessors)
    accessors.append(accessor)
    return ComponentLvalue(base_var=self.base_var, accessors=accessors)

  def Deref(self):
    return self._extend(Deref)

  def Dot(self, name):
    return self._extend(Dot(name))

  def Arrow(self, name):
    return self._extend(Arrow(name))

  def Sub(self, name):
    return self._extend(Sub(name))


class Accessor(object):
  '''For projecting into a part of an lvalue or expression.'''

  def access_variable(self, var):
    '''
    :param str var: A string giving some expression
    :return: The accessed version of var.
    :rtype: str
    '''
    raise RuntimeError(f"Abstract superclass {self.__class__}")


class Dot(Accessor):
  def __init__(self, name):
    self.name = name

  def access_variable(self, var):
    return f"{var}.{self.name}"


class Arrow(Accessor):
  def __init__(self, name):
    self.name = name

  def access_variable(self, var):
    return f"{var}->{self.name}"


class Sub(Accessor):
  def __init__(self, index):
    self.index = index

  def access_variable(self, var):
    return f"{var}[{self.index.to_c_string(root=True)}]"


class _Deref(Accessor):
  def access_variable(self, var):
    return f"*({var})"


Deref = _Deref()


class CreateVar(Lvalue):
  def __init__(self, var):
    self.var = var

  def add_includes(self, program):
    self.var.add_includes(program)

  def to_c_string(self):
    return self.var.type.wrap_variable(self.var.name)


class UpdateVar(Lvalue):
  def __init__(self, var):
    self.var = var

  def add_includes(self, program):
    self.var.add_includes(program)

  def to_c_string(self):
    return self.var.name
