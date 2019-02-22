import dist_zero.ids

from dist_zero import messages, spawners


class Utils(object):
  def base_config(self):
    system_config = messages.machine.std_system_config()
    system_config['DATA_NODE_KIDS_LIMIT'] = 3
    system_config['TOTAL_KID_CAPACITY_TRIGGER'] = 0
    system_config['SUM_NODE_SENDER_LIMIT'] = 6
    system_config['SUM_NODE_RECEIVER_LIMIT'] = 6
    return {
        'system_config': system_config,
        'network_errors_config': messages.machine.std_simulated_network_errors_config(),
    }

  async def root_io_tree(self, machine, leaf_config):
    '''spawn a new io tree and return the id of the root.'''
    node_id = dist_zero.ids.new_id('DataNode_root')
    self.demo.system.spawn_node(
        on_machine=machine,
        node_config=messages.io.data_node_config(node_id, parent=None, height=2, leaf_config=leaf_config))
    await self.demo.run_for(ms=200)
    return node_id

  async def spawn_users(self,
                        root_node,
                        n_users,
                        ave_inter_message_time_ms=0,
                        send_messages_for_ms=0,
                        send_after=0,
                        add_user=False):
    wait_per_loop = 870
    total_wait = n_users * wait_per_loop
    waited_so_far = 0
    for i in range(n_users):
      kid_id = self.demo.system.create_descendant(
          data_node_id=root_node,
          new_node_name='user_{}'.format(i),
          machine_id=self.machine_ids[i % len(self.machine_ids)],
          recorded_user=None if not add_user else self.demo.new_recorded_user(
              name='user_{}'.format(i),
              send_after=send_after + total_wait - waited_so_far,
              ave_inter_message_time_ms=ave_inter_message_time_ms,
              send_messages_for_ms=send_messages_for_ms,
          ))
      await self.demo.run_for(ms=wait_per_loop)
      waited_so_far += wait_per_loop
