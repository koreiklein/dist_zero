import json


def escape_c_string(s):
  return json.dumps(s)


INDENT = '  '
INDENT_TWO = INDENT + INDENT
INDENT_THREE = INDENT + INDENT + INDENT
