class Lvalue(object):
  def add_includes(self, program):
    raise NotImplementedError()


class CreateVar(Lvalue):
  def __init__(self, var):
    self.var = var

  def add_includes(self, program):
    self.var.add_includes(program)

  def to_c_string(self):
    return self.var.type.wrap_variable(self.var.name)
