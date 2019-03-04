import pytest

from dist_zero import types


@pytest.mark.asyncio
async def test_single_node(dz, demo):
  mainExpr = dz.RecordedUser(
      'recording',
      start=3,
      type=types.Int32,
      time_action_pairs=[
          (1400, [('inc', -2)]),
          (1700, [('inc', 1)]),
          (1950, [('inc', 4)]),
      ]).Spy('out')

  program = dz.compiler().compile(mainExpr)
  out_dataset = program.GetDatasetId(spy_key='out')
  demo.start_program(program)
  demo.run_for(ms=1000)
  assert 0 == demo.system.get_height(out_dataset)
  spy_result = demo.system.spy(out_dataset, 'out')
  import ipdb
  ipdb.set_trace()
