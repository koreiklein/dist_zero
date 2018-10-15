import pytest

from dist_zero import messages

from .common import Utils


class TestCollect(Utils):
  @pytest.mark.asyncio
  async def test_simple_collect(self, demo):
    self.demo = demo
    self.machine_ids = await self.demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_dns')

    root_input = await self.root_io_tree(
        machine=self.machine_ids[0], variant='input', leaf_config=messages.io.collect_leaf_config())
    root_output = await self.root_io_tree(
        machine=self.machine_ids[0], variant='input', leaf_config=messages.io.collect_leaf_config())

    # FIXME(KK): Finish this!
    raise RuntimeError("Not Yet Implemented")
