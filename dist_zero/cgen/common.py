import json

escape_c = lambda s: json.dumps(s).strip('"')

INDENT = '  '
INDENT_TWO = INDENT + INDENT
INDENT_THREE = INDENT + INDENT + INDENT
