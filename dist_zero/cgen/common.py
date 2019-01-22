import json


def escape_c_string(s):
  return json.dumps(s)


global_i = [0]


def inc_i():
  global_i[0] += 1
  return global_i[0]


INDENT = '  '
INDENT_TWO = INDENT + INDENT
INDENT_THREE = INDENT + INDENT + INDENT
