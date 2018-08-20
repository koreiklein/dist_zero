def partition(items, n_buckets):
  '''
  Partition a list of items into buckets.

  :param list items: Any list
  :param int n_buckets: Any number
  :return: A list of lists L such that
    - len(L) == n_buckets
    - [ x for inner in L for x in inner ] is a permutation of items
    - abs(len(L[i]) - len(L[j])) <= 1 for all i and j
  '''
  floor_items_per_bucket = len(items) // n_buckets

  result = []
  for i in range(n_buckets):
    result.append(items[i * floor_items_per_bucket:(i + 1) * floor_items_per_bucket])

  for j, extra in enumerate(items[n_buckets * floor_items_per_bucket:]):
    result[j].append(extra)

  return result
