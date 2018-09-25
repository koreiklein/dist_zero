import logging

from collections import defaultdict

from dist_zero import messages, errors, ids, deltas, settings, misc
from dist_zero.migration.right_configuration import ConfigurationReceiver
from .node import Node

logger = logging.getLogger(__name__)


class SumNode(Node):
  '''
  An internal node for summing all increments from its senders and forwarding the total to its receivers.

  Each `SumNode` is of one of three types

  * input `SumNode`, which receive as the adjacent node to an `LeafNode` or `InternalNode` and send to ``receivers``.
    These nodes have an ``input_node`` but no ``output_node`` and some nonempty list of ``receivers``.
  * output `SumNode`, which send as the adjacent node to an `LeafNode` or `InternalNode` and receive from ``senders``.
    These nodes have an ``output_node`` but no ``input_node`` and some nonempty list of ``senders``.
  * internal `SumNode`, which receive from senders and send to receivers.  These nodes have ``input_node is None``
    and ``output_node is None``

  Note that input/output `SumNode` could be for either `LeafNode` or `InternalNode`.  A `SumNode` adjacent to an 
  `InternalNode` is primarily responsible for helping to spin up new leaves, whereas a `SumNode` adjacent to a
  `LeafNode` will actually receive input messages from (or send output messages to) its adjacent leaf.
  '''

  SEND_INTERVAL_MS = 30
  '''The number of ms between sends to receivers.'''

  def __init__(self,
               node_id,
               left_is_data,
               right_is_data,
               senders,
               receivers,
               parent,
               controller,
               is_mid_node,
               configure_right_parent_ids,
               migrator_config=None):
    '''
    :param str node_id: The node id for this node.
    :param list senders: A list of :ref:`handle` of the nodes sending increments
    :param list receivers: A list of :ref:`handle` of the nodes to receive increments

    :param parent: The :ref:`handle` of the parent node that spawned this node.
    :type parent: :ref:`handle`

    :param object migrator_config: Configuration for an initializing migrator, or None if the node
      is not being initialized as part of a migration.

    :param `MachineController` controller: the controller for this node's machine.
    '''
    self._controller = controller

    self.id = node_id
    self.parent = parent

    self._is_mid_node = is_mid_node

    self._hourglass_data = defaultdict(lambda: {'terminal_sequence_number': {}, 'mid_node': None, 'n_hourglass_senders': None})

    self.left_is_data = left_is_data
    self.right_is_data = right_is_data

    self.height = 0

    self._added_sender_respond_tos = {}

    if settings.IS_TESTING_ENV:
      self._TESTING_total_before_first_swap = 0
      self._TESTING_swapped_once = False
      self._TESTING_total_after_first_swap = 0

    # Invariants:
    #   At certain points in time, a increment message is sent to every receiver.
    #   self._unsent_time_ms is the number of elapsed milliseconds since the last such point in time
    #   self._current_state is the total amount of increment sent to receivers as of that point in time
    #     (note: the amonut is always identical for every receiver)
    #   self._deltas is the complete set of updates received since that point in time.  None of the deltas
    #     have been added to self._current_state or sent to receivers.
    self._current_state = 0
    # Map from sender_id to a list of pairs (remote_sequence_number, message)
    self._unsent_time_ms = 0
    self._now_ms = 0

    self._time_since_had_enough_receivers_ms = 0

    self._configuration_receiver = ConfigurationReceiver(
        node=self, configure_right_parent_ids=configure_right_parent_ids, left_ids=[sender['id'] for sender in senders])

    super(SumNode, self).__init__(logger)

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    self.left_ids = [sender['id'] for sender in senders]

    self._exporters = {}
    self._importers = {}

    for sender in senders:
      self.import_from_node(sender)

  def has_left_and_right_configurations(self, left_configurations, right_configurations):
    # Importers should already have been set during __init__.
    self._exporters = {
        receiver_id: self.linker.new_exporter(right_config['parent_handle'])
        for receiver_id, right_config in right_configurations.items()
    }
    total_state = 0
    for left_config in left_configurations.values():
      if left_config['state'] is not None:
        total_state += left_config['state']
    self._current_state = total_state
    self._send_configure_left_to_right()

  def new_left_configurations(self, left_configurations):
    # These left_configurations should already have been added.
    for left_config in left_configurations:
      node = left_config['node']
      node_id = node['id']
      if node_id in self._added_sender_respond_tos:
        respond_to = self._added_sender_respond_tos.pop(node_id)
        self.send(respond_to, messages.migration.finished_adding_sender(sender_id=node_id))

  def new_right_configurations(self, right_configurations):
    for right_config in right_configurations:
      node = right_config['parent_handle']
      exporter = self.linker.new_exporter(node)
      self._exporters[node['id']] = exporter
      self.send(exporter.receiver,
                messages.migration.configure_new_flow_left(None, [
                    messages.migration.left_configuration(
                        node=self.new_handle(exporter.receiver['id']),
                        height=-1,
                        is_data=False,
                        state=self._current_state,
                        kids=[])
                ]))

  def _send_configure_left_to_right(self):
    self.logger.info("Sending configure_new_flow_left")
    for exporter in self._exporters.values():
      receiver = exporter.receiver
      message = messages.migration.configure_new_flow_left(self.migration_id, [
          messages.migration.left_configuration(
              node=self.new_handle(receiver['id']),
              height=-1,
              is_data=False,
              state=self._current_state,
              kids=[],
          )
      ])

      self.send(receiver, message)

  def is_data(self):
    return False

  @property
  def _input_importer(self):
    if self.left_is_data:
      if len(self._importers) == 0:
        return None
      elif len(self._importers) == 1:
        return next(iter(self._importers.values()))
      else:
        raise errors.InternalError("Sum node with a data node to the left may not have more than 1 importer")
    else:
      return None

  @property
  def _output_exporter(self):
    if self.right_is_data:
      if len(self._exporters) == 0:
        return None
      elif len(self._exporters) == 1:
        return next(iter(self._exporters.values()))
      else:
        raise errors.InternalError("Sum node with a data node to the right may not have more than 1 exporter")
    else:
      return None

  def initialize(self):
    self.logger.info(
        'Starting sum node {sum_node_id}. input={input_node_id}, output={output_node_id}',
        extra={
            'sum_node_id': self.id,
            'input_node_id': self._input_importer.sender_id if self._input_importer is not None else None,
            'output_node_id': self._output_exporter.receiver_id if self._output_exporter is not None else None,
        })
    if self._initial_migrator_config:
      self._initial_migrator = self.attach_migrator(self._initial_migrator_config)

    self.send(self.parent, messages.io.hello_parent(self.new_handle(self.parent['id'])))
    self._send_configure_right_to_left()

    if self._is_mid_node:
      self.send(self.parent, messages.hourglass.mid_node_up(self.new_handle(self.parent['id'])))

    self.linker.initialize()

  def _send_configure_right_to_left(self):
    self.logger.info("Sending configure_new_flow_right", extra={'receiver_ids': list(self._importers.keys())})
    for importer in self._importers.values():
      self.send(importer.sender,
                messages.migration.configure_new_flow_right(self.migration_id, [
                    messages.migration.right_configuration(
                        n_kids=None,
                        parent_handle=self.new_handle(importer.sender_id),
                        height=-1,
                        is_data=False,
                        connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'],
                    )
                ]))
    self._right_configurations_are_sent = True

  @property
  def migration_id(self):
    if self._initial_migrator is not None:
      return self._initial_migrator.migration_id
    else:
      return None

  def send_forward_messages(self, before=None):
    '''
    Generate a new sequence number, combine deltas into an update message, and send it on all exporters.

    :param dict[str, int] before: An optional dictionary mapping sender ids to sequence_numbers.
      If provided, process only up to the provided sequence number for each sender id.
    :return: the next unused sequence number
    :rtype: int
    '''
    new_state, increment, updated = self._deltas.pop_deltas(state=self._current_state, before=before)

    if not updated:
      return self.least_unused_sequence_number
    else:
      self.logger.debug("Sending new increment of {increment}.", extra={'increment': increment})
      self._current_state = new_state
      sequence_number = self.linker.advance_sequence_number()
      self._send_increment(increment=increment, sequence_number=sequence_number)
      return sequence_number + 1

  @staticmethod
  def from_config(node_config, controller):
    return SumNode(
        node_id=node_config['id'],
        senders=node_config['senders'],
        receivers=node_config['receivers'],
        left_is_data=node_config['left_is_data'],
        right_is_data=node_config['right_is_data'],
        is_mid_node=node_config['is_mid_node'],
        configure_right_parent_ids=node_config['configure_right_parent_ids'],
        parent=node_config['parent'],
        migrator_config=node_config['migrator'],
        controller=controller)

  def deliver(self, message, sequence_number, sender_id):
    '''
    Called by `Importer` instances in self._importers to deliver messages to self.
    Also called for an edge sum node adjacent to an input_node when the input node triggers incrementing the sum.
    '''
    # Don't update any internal state just yet, but wait until the next sequence number is generated.
    self._deltas.add_message(sender_id=sender_id, sequence_number=sequence_number, message=message)

  def generate_new_left_configuration(self, receiver):
    return messages.migration.left_configuration(
        height=-1, is_data=False, node=self.new_handle(receiver['id']), kids=[])

  def add_left_configuration(self, left_configuration):
    node = left_configuration['node']
    self.import_from_node(node)
    import ipdb
    ipdb.set_trace()
    self.send(node,
              messages.migration.configure_new_flow_right(
                  migration_id=None,
                  right_configurations=[
                      messages.migration.right_configuration(
                          parent_handle=self.new_handle(node['id']),
                          height=-1,
                          is_data=False,
                          n_kids=None,
                          connection_limit=0)
                  ]))

  def _maybe_swap_mid_node(self, mid_node_id):
    data = self._hourglass_data[mid_node_id]
    mid_node = data['mid_node']
    n_hourglass_senders = data['n_hourglass_senders']
    tsn = data['terminal_sequence_number']
    # FIXME(KK): Make sure these importers are ultimately removed from the linker.
    if len(tsn) == n_hourglass_senders:
      for sender_id in tsn.keys():
        self._importers.pop(sender_id)

      self.import_from_node(mid_node)
      self.send(mid_node,
                messages.migration.configure_new_flow_right(None, [
                    messages.migration.right_configuration(
                        n_kids=None,
                        parent_handle=self.new_handle(mid_node_id),
                        height=-1,
                        is_data=False,
                        connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'],
                    )
                ]))
      self._hourglass_data.pop(mid_node_id)

  def receive(self, sender_id, message):
    if self._configuration_receiver.receive(message=message, sender_id=sender_id):
      if self._is_mid_node and message['type'] == 'configure_new_flow_left':
        if all(val is not None for val in self._configuration_receiver._left_configurations.values()):
          self.send(self.parent, messages.hourglass.mid_node_ready(node_id=self.id))
      return
    elif message['type'] == 'start_hourglass':
      self.send_forward_messages()
      mid_node = message['mid_node']
      self.send(
          mid_node,
          messages.migration.configure_new_flow_left(None, [
              messages.migration.left_configuration(
                  node=self.new_handle(mid_node['id']), height=-1, is_data=False, state=self._current_state, kids=[])
          ]))
      for receiver_id in message['receiver_ids']:
        exporter = self._exporters.pop(receiver_id)
        self.send(exporter.receiver,
                  messages.hourglass.hourglass_swap(
                      mid_node_id=mid_node['id'],
                      sequence_number=exporter.internal_sequence_number,
                  ))
        self.export_to_node(mid_node)
    elif message['type'] == 'hourglass_swap':
      self._hourglass_data[message['mid_node_id']]['terminal_sequence_number'][sender_id] = message['sequence_number']
      self._maybe_swap_mid_node(message['mid_node_id'])
    elif message['type'] == 'hourglass_receive_from_mid_node':
      node = message['mid_node']
      self._hourglass_data[node['id']]['mid_node'] = node
      self._hourglass_data[node['id']]['n_hourglass_senders'] = message['n_hourglass_senders']
      self._maybe_swap_mid_node(node['id'])
    elif message['type'] == 'adopt':
      # FIXME(KK): Test and implement this
      import ipdb
      ipdb.set_trace()
    elif message['type'] == 'set_input':
      if not self.left_is_data:
        raise errors.InternalError("Can't set input when the left node is not a data node")
      if self._input_importer is None:
        self.import_from_node(message['input_node'])
      else:
        if self._input_importer.sender_id == message['input_node']['id']:
          # It was already set
          pass
        else:
          raise errors.InternalError("SumNode already has a distinct input node")
    elif message['type'] == 'set_output':
      if not self.right_is_data:
        raise errors.InternalError("Can't set output when the right node is not a data node")
      if self._output_exporter is None:
        self._exporters[message['output_node']['id']] = self.linker.new_exporter(message['output_node'])
      else:
        if self._output_exporter.receiver_id == message['output_node']['id']:
          # It was already set
          pass
        else:
          raise errors.InternalError("SumNode already has a distinct output node")
    elif message['type'] == 'added_sender':
      node = message['node']
      self.import_from_node(node)
      self.send(node,
                messages.migration.configure_new_flow_right(
                    migration_id=None,
                    right_configurations=[
                        messages.migration.right_configuration(
                            parent_handle=self.new_handle(node['id']),
                            height=-1,
                            is_data=False,
                            n_kids=None,
                            connection_limit=0)
                    ]))
      respond_to = message['respond_to']
      if respond_to is not None:
        self._added_sender_respond_tos[node['id']] = respond_to
    elif message['type'] == 'adjacent_has_split':
      # Spawn a new adjacent for the newly spawned io node and remove any kids stolen from self.
      node_id = ids.new_id('SumNode_adjacent_for_split')
      new_node = message['new_node']
      self._controller.spawn_node(
          messages.sum.sum_node_config(
              node_id=node_id,
              senders=[new_node],
              receivers=[self.transfer_handle(exporter.receiver, node_id) for exporter in self._exporters.values()],
          ))
    else:
      super(SumNode, self).receive(message=message, sender_id=sender_id)

  def import_from_node(self, node, first_sequence_number=0):
    '''
    Start importing from node.

    :param node: The :ref:`handle` of a `Node` that should now be sending to self.
    :type node: :ref:`handle`
    '''
    if node['id'] in self._importers:
      raise errors.InternalError("Already importing from this node.", extra={'existing_node_id': node['id']})
    self._importers[node['id']] = self.linker.new_importer(sender=node, first_sequence_number=first_sequence_number)
    self._deltas.add_sender(node['id'])

  def export_to_node(self, receiver):
    if receiver['id'] not in self._exporters:
      self._exporters[receiver['id']] = self.linker.new_exporter(receiver=receiver)

  def elapse(self, ms):
    self._unsent_time_ms += ms
    self._time_since_had_enough_receivers_ms += ms

    if not self.deltas_only and \
        self._deltas.has_data() and \
        self._unsent_time_ms > SumNode.SEND_INTERVAL_MS:

      self._check_limits()
      self.send_forward_messages()

    self._now_ms += ms

    for migrator in self.migrators.values():
      migrator.elapse(ms)

    self.linker.elapse(ms)

  def _check_limits(self):
    '''Test for various kinds of load problems and take appropriate actions to remedy them.'''
    SENDER_LIMIT = self.system_config['SUM_NODE_SENDER_LIMIT']
    TOO_FEW_RECEIVERS_TIME_MS = self.system_config['SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS']
    SUM_NODE_RECEIVER_LOWER_LIMIT = self.system_config['SUM_NODE_RECEIVER_LOWER_LIMIT']
    SUM_NODE_SENDER_LOWER_LIMIT = self.system_config['SUM_NODE_SENDER_LOWER_LIMIT']

    if len(self._exporters) >= SUM_NODE_RECEIVER_LOWER_LIMIT or len(self._importers) >= SUM_NODE_SENDER_LOWER_LIMIT:
      self._time_since_had_enough_receivers_ms = 0

    elif self._time_since_had_enough_receivers_ms > TOO_FEW_RECEIVERS_TIME_MS and \
        self._input_importer is None \
        and self._output_exporter is None:
      self._time_since_had_enough_receivers_ms = 0
    self.logger.info("current n_senders = {n_senders}", extra={'n_senders': len(self._importers)})

  def _send_increment(self, increment, sequence_number):
    if settings.IS_TESTING_ENV:
      if self._TESTING_swapped_once:
        self._TESTING_total_after_first_swap += increment
      else:
        self._TESTING_total_before_first_swap += increment

    if self._output_exporter:
      self._output_exporter.export_message(
          message=messages.io.output_action(increment),
          sequence_number=sequence_number,
      )
    else:
      for exporter in self._exporters.values():
        exporter.export_message(
            message=messages.sum.increment(amount=increment),
            sequence_number=sequence_number,
        )

    self._unsent_time_ms = 0

  def switch_flows(self, migration_id, old_exporters, new_exporters, new_receivers):
    for receiver_id in old_exporters:
      exporter = self._exporters.pop(receiver_id)
      self.send(exporter.receiver,
                messages.migration.swapped_from_duplicate(
                    migration_id, first_live_sequence_number=exporter.internal_sequence_number))

    for exporter in new_exporters.values():
      self.send(exporter.receiver,
                messages.migration.swapped_to_duplicate(
                    migration_id, first_live_sequence_number=exporter.internal_sequence_number))

    self._exporters.update(new_exporters)

  def activate_swap(self, migration_id, kids, use_output=False, use_input=False):
    if len(kids) != 0:
      raise errors.InternalError("Sum nodes should never be passed kids by a migrator.")

  def checkpoint(self, before=None):
    self.send_forward_messages(before=before)

  def remove_migrator(self, migration_id):
    for exporter in self._exporters.values():
      exporter._migration_id = None

    super(SumNode, self).remove_migrator(migration_id)

  def sink_swap(self, deltas, old_sender_ids, new_senders, new_importers, linker):
    self._deltas = deltas
    self.linker.remove_importers(old_sender_ids)
    self.linker.absorb_linker(linker)
    self._importers = new_importers

  def stats(self):
    return {
        'height': -1,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def handle_api_message(self, message):
    if message['type'] == 'get_kids':
      return {}
    elif message['type'] == 'get_senders':
      return {sender_id: importer.sender for sender_id, importer in self._importers.items()}
    elif message['type'] == 'get_receivers':
      return {receiver_id: exporter.receiver for receiver_id, exporter in self._exporters.items()}
    else:
      return super(SumNode, self).handle_api_message(message)
