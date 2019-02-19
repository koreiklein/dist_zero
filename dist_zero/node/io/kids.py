import blist
from dist_zero import errors, infinity


class DataNodeKids(object):
  def __init__(self, left, right, controller):
    self._left = left
    self._right = right
    self._interval = [left, right]

    self._controller = controller

    self._kid_to_interval = None
    self._kid_intervals = None
    self._handles = None
    self._summaries = None

    self.clear()

  def clear(self):
    self._kid_to_interval = {}
    self._kid_intervals = blist.sortedlist([], key=lambda item: item[0])
    self._handles = {}
    self._summaries = {}

  def left_endpoint(self, kid_id):
    start, stop = self._kid_to_interval[kid_id]
    return start

  def right_endpoint(self, kid_id):
    start, stop = self._kid_to_interval[kid_id]
    return stop

  def grow_left(self, key):
    self._left = key
    self._interval[0] = key

  def shrink_right(self):
    '''
    Shrink the interval by reducing the right endpoint.
    Updates self to manage a smaller interval, possibly removing kids in the process.

    :return: A pair (new_right_endpoint, removed_kids)  where new_right_endpoint is the new right endpoint of
      self and removed_kids is the ordered list of kids that were store after the right endpoint.
      new_right_endpoint is guaranteed not to fall inside the interval managed by any one kid.
    '''
    n_to_keep = len(self._kid_intervals) // 2
    if n_to_keep == len(self._kid_intervals):
      mid = self._truncate_interval_right()
      return mid, []
    else:
      leaving_kid_ids = list(self)[n_to_keep:]
      mid = self._kid_intervals[n_to_keep][0]
      self._right = mid
      self._interval[1] = mid

      kids = []
      for kid_id in leaving_kid_ids:
        self._summaries.pop(kid_id, None)
        kids.append(self._handles.pop(kid_id))
        start, stop = self._kid_to_interval.pop(kid_id)
        self._kid_intervals.remove([start, stop, kid_id])

      return mid, kids

  def add_kid(self, kid, interval, summary=None):
    start, stop = interval
    kid_id = kid['id']
    self._kid_to_interval[kid_id] = [start, stop]
    self._kid_intervals.add([start, stop, kid_id])
    self._handles[kid_id] = kid
    if summary:
      self._summaries[kid_id] = summary

  def interval_json(self):
    return infinity.interval_json(self._interval)

  def interval(self):
    return list(self._interval)

  def __iter__(self):
    return (kid_id for a, b, kid_id in self._kid_intervals)

  def __contains__(self, kid_id):
    return kid_id in self._handles

  def get(self, kid_id, default=None):
    return self._handles.get(kid_id, default)

  def __getitem__(self, kid_id):
    return self._handles[kid_id]

  def __bool__(self):
    if self._handles:
      return True
    else:
      return False

  def __len__(self):
    return len(self._handles)

  def get_proxy(self):
    if len(self._handles) == 1:
      return next(iter(self._handles.values()))
    else:
      return None

  def set_summary(self, kid_id, summary):
    if kid_id in self._handles:
      self._summaries[kid_id] = summary

  @property
  def summaries(self):
    return self._summaries

  def merge_right(self, kid_id):
    self._handles.pop(kid_id)
    self._summaries.pop(kid_id, None)
    start, mid = self._kid_to_interval.pop(kid_id)
    index = self._kid_intervals.index([start, mid, kid_id])
    self._kid_intervals.pop(index)

    # We can't update self._kid_intervals[index], as blist does not allow modifying the key of an item, and
    # we are modifying item[0] (it's key).
    # Instead, we pop the item we'd like to modify and reinsert it.
    mid, stop, right_kid_id = self._kid_intervals.pop(index)
    self._kid_intervals.add([start, stop, right_kid_id])

    self._kid_to_interval[right_kid_id] = [start, stop]

  def split(self, kid_id, mid, new_kid, new_summary):
    new_id = new_kid['id']
    start, stop = self._kid_to_interval[kid_id]
    index = self._kid_intervals.index([start, stop, kid_id])

    # Update the existing interval
    self._kid_intervals[index][1] = mid
    self._kid_to_interval[kid_id][1] = mid

    # Add the new kid
    self._kid_intervals.add([mid, stop, new_id])
    self._kid_to_interval[new_id] = [mid, stop]
    self._handles[new_id] = new_kid
    self._summaries[new_id] = new_summary

  def remove_kid(self, kid_id):
    self._handles.pop(kid_id)
    start, stop = self._kid_to_interval.pop(kid_id)
    self._kid_intervals.remove([start, stop, kid_id])
    self._summaries.pop(kid_id, None)

  @property
  def left(self):
    return self._left

  @property
  def right(self):
    return self._right

  def _random_key(self):
    left = 0.0 if self._left == infinity.Min else self._left
    right = 1.0 if self._right == infinity.Max else self._right
    return self._controller.random.uniform(left, right)

  def _truncate_interval_right(self):
    key = self.new_kid_key()
    self._right = key
    self._interval[1] = key
    return key

  def new_kid_key(self):
    result = self._random_key()

    while self._key_occurs_in_kid_intervals(result) or result == self._right:
      result = self._random_key()

    return result

  def _key_occurs_in_kid_intervals(self, key):
    if not self._kid_intervals:
      return False
    else:
      i = self._kid_intervals.bisect_left((key, None))
      return not i == len(self._kid_intervals) and self._kid_intervals[i][0] == key
