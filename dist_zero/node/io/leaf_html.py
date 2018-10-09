import os

import xml.etree.ElementTree as ET


def from_kid_config(kid_config):
  '''
  Generate the html to bootstrap a new leaf node.

  :param object kid_config: The leaf node configuration object.
  :return: The html string to generate the leaf.
  :rtype: str
  '''
  html = ET.Element('html')
  head = ET.SubElement(html, 'head')
  title = ET.SubElement(head, 'title')
  title.text = 'Dist Zero'
  body = ET.SubElement(html, 'body')
  demo_message = ET.SubElement(body, 'div', id='demo')
  demo_message.text = 'before js'
  message = ET.SubElement(body, 'p')
  message.text = 'Hello HTML World'
  _populate_script_tag(kid_config, ET.SubElement(body, 'script'))
  return ET.tostring(html, encoding='UTF-8')


_LeafJSSingleton = [None]


def leaf_js():
  if _LeafJSSingleton[0] is None:
    with open(os.path.join(os.path.dirname(__file__), 'leaf.js'), 'r') as f:
      _LeafJSSingleton[0] = f.read()

  return _LeafJSSingleton[0]


def _populate_script_tag(kid_config, script):
  script.text = leaf_js()
