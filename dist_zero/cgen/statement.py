from . import expression
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

  def AddSwitch(self, switch_on):
    result = Switch(switch_on, program=self.program)
    self._statements.append(result)
    return result

  def AddIf(self, condition):
    result = If(condition, program=self.program)
    self._statements.append(result)
    return result

  def AddContinue(self):
    self._statements.append(Continue)

  def AddBreak(self):
    self._statements.append(Break)

  def AddWhile(self, condition):
    result = While(condition, program=self.program)
    self._statements.append(result)
    return result.block

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


class _Continue(Statement):
  def to_c_string(self, lines, indent):
    lines.append(f"{indent * ' '}continue;\n")


Continue = _Continue()


class _Break(Statement):
  def to_c_string(self, lines, indent):
    lines.append(f"{indent * ' '}break;\n")


Break = _Break()


class Switch(Statement):
  def __init__(self, switch_on, program):
    self.program = program
    self.switch_on = switch_on
    self._cases = []
    self._default_case_block = None

  def AddDefault(self):
    if self._default_case_block is not None:
      raise RuntimeError("The default case was already added to this switch statement.")

    self._default_case_block = Block(self.program)
    return self._default_case_block

  def AddCase(self, value):
    if not (isinstance(value, expression.Constant) or isinstance(value, expression.StrConstant)):
      raise RuntimeError(f"Case in switch statement must use a constant value.  Got {value}")
    case_block = Block(self.program)
    self._cases.append((value, case_block))
    return case_block

  def to_c_string(self, lines, indent):
    lines.append(f"{indent * ' '}switch ({self.switch_on.to_c_string(root=True)}) {{\n")
    big_indent = indent + INDENT + INDENT
    for value, case_block in self._cases:
      lines.append(f"{(indent + INDENT) * ' '}case {value.to_c_string(root=True)}:\n")
      case_block.to_c_string(lines, big_indent)
    if self._default_case_block is not None:
      lines.append(f"{(indent + INDENT) * ' '}default:\n")
      self._default_case_block.to_c_string(lines, big_indent)

    lines.append(f"{indent * ' '}}}\n")


class While(Statement):
  def __init__(self, condition, program):
    self.program = program
    self.condition = condition
    self.block = Block(self.program)

  def to_c_string(self, lines, indent):
    lines.append(f"{indent * ' '}while ({self.condition.to_c_string(root=True)})\n")
    self.block.to_c_string(lines, indent)


class If(Statement):
  def __init__(self, condition, program):
    self.program = program
    self.condition = condition
    self._consequent = Block(self.program)
    self._alternate = None

  @property
  def consequent(self):
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
      lines.append(f"{indent * ' '}else\n")
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
