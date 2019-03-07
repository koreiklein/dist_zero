import pytest

from dist_zero import types


@pytest.mark.asyncio
async def test_single_node(dz, demo):
  machine = await demo.new_machine_controller()
  await demo.run_for(ms=200)

  mainExpr = dz.RecordedUser(
      'recording', start=3, type=types.Int32, time_action_pairs=[
          (4400, [('inc', -2)]),
      ]).Spy('out')

  program = dz.compiler('test_single_node').compile(mainExpr)
  demo.system.spawn_node(on_machine=machine, node_config=program.to_program_node_config())

  await demo.run_for(ms=1000)

  out_dataset = demo.system.get_spy_roots(program.id)['out']
  assert 0 == demo.system.get_height(out_dataset)
  spy_result = demo.system.spy(out_dataset, 'out')
  assert 1 == len(spy_result)
  assert 3 == spy_result.pop(out_dataset)['basicState']

  await demo.run_for(ms=5000)
  assert 1 == demo.system.spy(out_dataset, 'out').pop(out_dataset)['basicState']
