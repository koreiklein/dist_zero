import asyncio
import json
import random
import pytest
import time

from collections import defaultdict

import dist_zero.ids

from dist_zero import spawners, messages, types, transaction
from dist_zero.recorded import RecordedUser
from dist_zero.system_controller import SystemController
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.cloud.aws import Ec2Spawner


@pytest.fixture(params=[
    pytest.param(spawners.MODE_SIMULATED, marks=pytest.mark.simulated),
    pytest.param(spawners.MODE_VIRTUAL, marks=pytest.mark.virtual),
    pytest.param(spawners.MODE_CLOUD, marks=pytest.mark.cloud),
])
async def demo(request):
  result = Demo(mode=request.param)
  result.start()
  yield result
  await result.tear_down()


@pytest.fixture(params=[
    pytest.param(spawners.MODE_SIMULATED, marks=pytest.mark.simulated),
    pytest.param(spawners.MODE_VIRTUAL, marks=pytest.mark.virtual),
])
async def no_cloud_demo(request):
  result = Demo(mode=request.param)
  result.start()
  yield result
  await result.tear_down()


@pytest.fixture(params=[
    pytest.param(spawners.MODE_SIMULATED, marks=pytest.mark.simulated),
])
async def simulated_demo(request):
  result = Demo(mode=request.param)
  result.start()
  yield result
  await result.tear_down()


@pytest.fixture(params=[
    pytest.param(spawners.MODE_CLOUD, marks=pytest.mark.cloud),
])
async def cloud_demo(request):
  result = Demo(mode=request.param)
  result.start()
  yield result
  await result.tear_down()


class Demo(object):
  '''
  For running a Demo of a full distributed system.

  Demos may run in simulated, virtual, or cloud mode.
  '''

  def __init__(self, mode=spawners.MODE_SIMULATED, random_seed=None):
    '''
    :param str mode: The mode in which to run the demo.
    '''
    self.mode = mode
    self.nodes = 0

    self._recorded_users = []

    self.system = None
    self.spawner = None
    self.simulated_spawner = None
    self.virtual_spawner = None
    self.cloud_spawner = None

    self.random_seed = 'TestSimulatedSpawner' if random_seed is None else random_seed
    self._rand = random.Random(self.random_seed + '_for__demo')

    self.total_simulated_amount = 0

  def start(self):
    '''Start the demo.'''
    self._set_system_by_mode()

    self.system.configure_logging()

    if self.simulated_spawner:
      self.simulated_spawner.start()
    elif self.virtual_spawner:
      self.virtual_spawner.start()

  async def tear_down(self):
    '''Remove any resources created as part of the demo.'''
    if self.simulated_spawner:
      await self.simulated_spawner.clean_all()

    if self.virtual_spawner and self.virtual_spawner.started:
      await self.virtual_spawner.clean_all()

    if self.cloud_spawner:
      await self.cloud_spawner.clean_all()

  def _new_unique_system_id(self):
    '''
    Generate an id to use for a new system.
    These ids should actually be distinct each time, and so will use actualy unique ids or novel randomness.
    '''
    return dist_zero.ids.new_id(
        'System', random_id=''.join(random.choice(dist_zero.ids.RAND_CHARS) for i in range(dist_zero.ids.RAND_LENGTH)))

  def _set_system_by_mode(self):
    # Use the
    self.system_id = self._new_unique_system_id()
    if self.mode == spawners.MODE_SIMULATED:
      self.spawner = self.simulated_spawner = SimulatedSpawner(system_id=self.system_id, random_seed=self.random_seed)
    elif self.mode == spawners.MODE_VIRTUAL:
      self.spawner = self.virtual_spawner = DockerSpawner(system_id=self.system_id, inside_container=False)
    elif self.mode == spawners.MODE_CLOUD:
      self.spawner = self.cloud_spawner = Ec2Spawner(system_id=self.system_id)
    else:
      raise RuntimeError("Unrecognized mode {}".format(self.mode))

    self.system = SystemController(system_id=self.system_id, spawner=self.spawner)

  @property
  def simulated(self):
    '''True iff this demo is simulated'''
    return self.spawner.mode() == 'simulated'

  def now_ms(self):
    ''':return: The current time in milliseconds'''
    if self.simulated:
      return self.spawner.now_ms()
    else:
      return time.time()

  def sleep_ms(self, ms):
    return self.spawner.sleep_ms(ms)

  def run_for(self, ms):
    '''Run for ms milliseconds, in either real or simulated time depending on the current mode.'''
    if self.simulated:
      return self.spawner.run_for(int(ms))
    else:
      return asyncio.sleep(ms / 1000)

  async def new_machine_controllers(self, n, base_config=None, random_seed=None):
    '''
    Create n new machine controllers

    :param int n: The number of new `MachineController` instances to create.
    :param dict base_config: A dictionary of extra parameters to add to the configs for all the newly created machines or `None`.
    :return: The list of the new handles.
    :rtype: list
    '''
    configs = []
    for i in range(n):
      name = 'machine {}'.format(self.nodes)
      self.nodes += 1

      machine_config = json.loads(json.dumps(base_config)) if base_config else {}
      machine_config['machine_name'] = name
      machine_config['machine_controller_id'] = dist_zero.ids.new_id('Machine')
      machine_config['mode'] = self.mode
      machine_config['system_id'] = self.system_id
      machine_config['random_seed'] = "{}:{}".format(random_seed if random_seed is not None else self.random_seed, n)

      configs.append(messages.machine.machine_config(**machine_config))

    result = await self.system.create_machines(configs)
    return result

  async def new_machine_controller(self):
    '''Like `Demo.new_machine_controllers` but only creates and returns one.'''
    controllers = await self.new_machine_controllers(1)
    return controllers[0]

  def link_datasets(self, root_input_id, root_output_id, machine, link_key, name=None):
    return self._connect_trees_with_link_network(
        node_id=dist_zero.ids.new_id(name if name is not None else 'LinkRoot'),
        root_input_id=root_input_id,
        root_output_id=root_output_id,
        machine=machine,
        link_key=link_key)

  def _connect_trees_with_link_network(self, node_id, root_input_id, root_output_id, machine, link_key):
    node_config = transaction.add_participant_role_to_node_config(
        node_config=messages.link.link_node_config(
            node_id=dist_zero.ids.new_id('LinkNode_root'), left_is_data=True, right_is_data=True, link_key=link_key),
        transaction_id=dist_zero.ids.new_id('NewLink'),
        participant_typename='CreateLink',
        args=dict(
            src=self.system.generate_new_handle(new_node_id=node_id, existing_node_id=root_input_id),
            tgt=self.system.generate_new_handle(new_node_id=node_id, existing_node_id=root_output_id),
        ))
    self.system.spawn_node(on_machine=machine, node_config=node_config)
    return node_config['id']

  def create_dataset(self, machine, name, height, input_link_keys=None, output_link_keys=None):
    return self.system.spawn_dataset(
        on_machine=machine,
        node_config=messages.data.data_node_config(
            node_id=dist_zero.ids.new_id(name),
            parent=None,
            height=height,
            dataset_program_config=messages.data.demo_dataset_program_config(
                input_link_keys=input_link_keys if input_link_keys is not None else [],
                output_link_keys=output_link_keys if output_link_keys is not None else [],
            )))

  def get_leftmost_leaves(self, link_root_id):
    def _loop(node_id):
      if 0 == self.system.get_height(node_id):
        yield node_id
      else:
        for kid_id in self.system.get_leftmost_kids(node_id):
          yield from _loop(kid_id)

    return list(_loop(link_root_id))

  def get_leaves(self, root_id):
    def _loop(node_id):
      if 0 == self.system.get_height(node_id):
        yield node_id
      else:
        for kid_id in self.system.get_kids(node_id):
          yield from _loop(kid_id)

    return list(_loop(root_id))

  def new_recorded_user(self, name, ave_inter_message_time_ms, send_messages_for_ms, send_after=0):
    time_message_pairs = []
    times_to_send = sorted(send_after + self._rand.random() * send_messages_for_ms
                           for x in range(send_messages_for_ms // ave_inter_message_time_ms))

    for t in times_to_send:
      amount_to_send = int(self._rand.random() * 20)
      self.total_simulated_amount += amount_to_send
      time_message_pairs.append((t, messages.data.input_action(amount_to_send)))

    result = RecordedUser(name, 0, types.Int32, time_message_pairs)
    self._recorded_users.append(result)
    return result

  def all_recorded_actions(self):
    return [action for user in self._recorded_users for action in user.actions]

  def render_network(self, start_node_id, filename='network', view=False):
    from graphviz import Digraph
    dot = Digraph(comment='Network Graph of system "{}"'.format(self.system.id), graph_attr={'rankdir': 'LR'})

    by_height = defaultdict(list)
    main_subgraph = Digraph()
    right_subgraph_visited = set()

    nodes = set()

    def _add_node(graph, node_id):
      if node_id not in nodes:
        nodes.add(node_id)
        if node_id.startswith('DataNode') or node_id.startswith('LeafNode'):
          kwargs = {'shape': 'ellipse', 'color': 'black', 'fillcolor': '#c7faff', 'style': 'filled'}
        else:
          kwargs = {'shape': 'diamond', 'color': 'black'}
        graph.node(node_id, **kwargs)

    edges = set()

    def _add_edge(graph, left, right, *args, **kwargs):
      _add_node(graph, left)
      _add_node(graph, right)
      pair = (left, right)
      if pair not in edges:
        edges.add(pair)
        graph.edge(left, right, *args, **kwargs)

    def _go_down(node_id):
      if node_id not in right_subgraph_visited:
        right_subgraph_visited.add(node_id)
        height = self.system.get_stats(node_id)['height']
        by_height[height].append(node_id)
        _add_node(main_subgraph, node_id)
        for kid in self.system.get_kids(node_id):
          _add_edge(main_subgraph, node_id, kid, label='kid')
          _go_down(kid)

    _go_down(start_node_id)
    dot.subgraph(main_subgraph)

    heights = list(reversed(sorted(by_height.keys())))

    visited = set()
    for height in heights:
      subgraph = Digraph('cluster_{}'.format(height), graph_attr={'label': 'height {}'.format(height)})
      subgraph_visited = set()

      def _go_left(node_id):
        if node_id not in subgraph_visited:
          visited.add(node_id)
          subgraph_visited.add(node_id)
          _add_node(subgraph, node_id)
          for sender in self.system.get_senders(node_id):
            _add_edge(subgraph, sender, node_id, label='sends')
            _go_left(sender)

      def _go_right(node_id):
        if node_id not in subgraph_visited:
          visited.add(node_id)
          subgraph_visited.add(node_id)
          _add_node(subgraph, node_id)
          for receiver in self.system.get_receivers(node_id):
            _add_edge(subgraph, node_id, receiver, label='sends')
            _go_right(receiver)

      for node_id in by_height[height]:
        _go_left(node_id)

      subgraph_visited = set()
      for node_id in by_height[height]:
        _go_right(node_id)

      dot.subgraph(subgraph)

    # Add kid edges
    for node_id in visited:
      if node_id not in right_subgraph_visited:
        for kid in self.system.get_kids(node_id):
          _add_edge(dot, node_id, kid, label='kid')

    dot.render(filename, view=view, cleanup=True)
