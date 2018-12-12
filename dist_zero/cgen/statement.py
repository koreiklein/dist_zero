from .common import INDENT, escape_c


class Block(object):
  def __init__(self, program, root=False):
    self.root = root
    self.program = program
    self._statements = []

  def to_c_string(self, lines, indent):
    if len(self._statements) == 1 and not self.root:
      self._statements[0].to_c_string(lines, indent + INDENT)
    else:
      lines.append(f"{indent * ' '}{{\n")
      for statement in self._statements:
        statement.to_c_string(lines, indent + INDENT)
      lines.append(f"{indent * ' '}}}\n")

  def AddReturn(self, rvalue):
    self._statements.append(Return(rvalue))

  def AddIf(self, condition):
    result = If(condition, program=self.program)
    self._statements.append(result)
    return result

  def AddDeclaration(self, lvalue):
    lvalue.add_includes(self.program)
    self._statements.append(Declaration(lvalue))

  def AddAssignment(self, lvalue, rvalue):
    lvalue.add_includes(self.program)
    rvalue.add_includes(self.program)
    self._statements.append(Assignment(lvalue, rvalue))


class Statement(object):
  def to_c_string(self, lines, indent):
    raise NotImplementedError()


class If(Statement):
  def __init__(self, condition, program):
    self.program = program
    self.condition = condition
    self._consequent = None
    self._alternate = None

  @property
  def consequent(self):
    if self._consequent is None:
      self._consequent = Block(self.program)
    return self._consequent

  @property
  def alternate(self):
    if self._alternate is None:
      self._alternate = Block(self.program)
    return self._alternate

  def to_c_string(self, lines, indent):
    lines.append(f"{indent * ' '}if {self.condition.to_c_string()}\n")
    if self._consequent is not None:
      self._consequent.to_c_string(lines, indent)
    if self._alternate is not None:
      self._alternate.to_c_string(lines, indent)


class Return(Statement):
  def __init__(self, rvalue):
    self.rvalue = rvalue

  def to_c_string(self, lines, indent):
    lines.append(f'{indent * " "}return {self.rvalue.to_c_string(root=True)};\n')


class Declaration(Statement):
  def __init__(self, lvalue):
    self.lvalue = lvalue

  def to_c_string(self, lines, indent):
    lines.append(f"{indent * ' '}{self.lvalue.to_c_string()};\n")


class Assignment(Statement):
  def __init__(self, lvalue, rvalue):
    self.lvalue = lvalue
    self.rvalue = rvalue

  def to_c_string(self, lines, indent):
    lines.append(f"{indent * ' '}{self.lvalue.to_c_string()} = {self.rvalue.to_c_string(root=True)};\n")
