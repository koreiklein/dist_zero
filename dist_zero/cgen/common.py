import json


def escape_c(s):
  return json.dumps(s)


INDENT = '  '
INDENT_TWO = INDENT + INDENT
INDENT_THREE = INDENT + INDENT + INDENT
