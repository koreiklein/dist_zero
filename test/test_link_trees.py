import pytest

from dist_zero import ids

#async def _spawn_tree


class TestLinkTrees(object):
  async def _create_new_leaf(self, name, root_id, machine):
    self.leaf_ids.append(
        self.demo.system.create_descendant(data_node_id=root_id, new_node_name=name, machine_id=machine))
    await self.demo.run_for(ms=400)

  def _assert_unique_paths(self, link_root_id, link_key):
    demo = self.demo
    input_ids = demo.system.get_senders(link_root_id)
    assert 1 == len(input_ids)
    output_ids = demo.system.get_receivers(link_root_id)
    assert 1 == len(output_ids)

    leftmost_leaves = set(demo.get_leftmost_leaves(link_root_id))
    output_leaves = set(demo.get_leaves(output_ids[0]))

    n_outgoing_paths = {}

    def _get_n_outgoing_paths(node_id):
      if node_id not in n_outgoing_paths:
        n_outgoing_paths[node_id] = (1 if node_id in output_leaves else sum(
            _get_n_outgoing_paths(receiver) for receiver in demo.system.get_receivers(node_id)))

      return n_outgoing_paths[node_id]

    for input_leaf in demo.get_leaves(input_ids[0]):
      adjacent = demo.system.get_output_link(input_leaf, link_key)
      assert adjacent is not None
      assert adjacent in leftmost_leaves
      assert len(output_leaves) == _get_n_outgoing_paths(adjacent)

  @pytest.mark.asyncio
  async def test_link_one_one(self, demo):
    await self._link_n_m(demo, n=1, m=1)

  @pytest.mark.asyncio
  async def test_link_small_one(self, demo):
    await self._link_n_m(demo, n=3, m=1)

  async def _link_n_m(self, demo, n, m):
    self.leaf_ids = []
    self.demo = demo
    machine = await self.demo.new_machine_controller()
    link_key = 'my_link'
    root_input_id = self.demo.create_dataset(
        machine=machine, name='DataInputRoot', height=0 if n == 1 else 2, output_link_keys=[link_key])
    root_output_id = self.demo.create_dataset(
        machine=machine, name='DataOutputRoot', height=0 if m == 1 else 2, input_link_keys=[link_key])
    await self.demo.run_for(ms=500)

    if n > 1:
      for i in range(n):
        await self._create_new_leaf(name=f"InputLeaf_{i}", root_id=root_input_id, machine=machine)

    if m > 1:
      for i in range(m):
        await self._create_new_leaf(name=f"OutputLeaf_{i}", root_id=root_output_id, machine=machine)

    demo.render_network(root_input_id) # FIXME(KK): Remove

    link_id = self.demo.link_datasets(
        root_input_id=root_input_id, root_output_id=root_output_id, machine=machine, name='LinkRoot', link_key=link_key)
    await self.demo.run_for(ms=1000)
    self._assert_unique_paths(link_id, link_key)
