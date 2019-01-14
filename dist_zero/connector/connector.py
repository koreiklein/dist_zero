class Connector(object):
  '''
  Abstract superclass for classes that a single manager computation node (not a leaf) can use
  to determine which children to spawn, how to connect them to each other with the appropriate network toploogy
  and how to modify that topology as things change.
  '''
  pass
