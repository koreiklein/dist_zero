'''
Custom logging setup for DistZero
'''

import logging
import json

from collections import OrderedDict

from pythonjsonlogger import jsonlogger

JsonFormatter = jsonlogger.JsonFormatter


class LoggerAdapter(logging.LoggerAdapter):
  '''
  A LoggerAdapter that merges context instead of replacing it.
  '''

  def process(self, msg, kwargs):
    kwargs['extra'] = kwargs.get('extra', {})
    extra = kwargs['extra']

    for key, value in self.extra.items():
      if key not in extra:
        extra[key] = value

    return (msg, kwargs)


class ContextFilter(logging.Filter):
  def __init__(self, context, spawner):
    self.context = context
    self.spawner = spawner

  def filter(self, record):
    record.dz_time = self.spawner.dz_time
    for key, value in self.context.items():
      setattr(record, key, value)

    return True


class StrFormatFilter(logging.Filter):
  '''
  A filter that runs format on the message string

  With this filter, the messages can use the str.format syntax with the keys in the 'extra' dict.

  e.g.

    logger.info("Send to node {node_id}", extra={ "node_id": "a82Bnte8" })
  '''

  def filter(self, record):
    if hasattr(record, '_str_formatted_by_filter'):
      return True

    try:
      record.msg = record.msg.format(**record.__dict__)
      record._str_formatted_by_filter = True
    except Exception as e:
      record.msg = '!!! LOG FILTER FORMAT FAILURE !!! ' + record.msg

    return True


HUMAN_FORMATTER = logging.Formatter(
    fmt='%(asctime)s.%(msecs)03d %(levelname)s %(name)-22s| %(message)s', datefmt='%M:%S')
'''
A formatter for human readable output.
'''


def set_handlers(logger_a, handlers):
  '''
  Remove all existing handlers on a logger and set new ones
  '''
  for handler in list(logger_a.handlers):
    logger_a.removeHandler(handler)
  for handler in handlers:
    logger_a.addHandler(handler)
