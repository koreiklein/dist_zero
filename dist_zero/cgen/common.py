import json

escape_c = lambda s: json.dumps(s).strip('"')

INDENT = 2
