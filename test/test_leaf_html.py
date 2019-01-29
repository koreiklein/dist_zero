import pytest
from cryptography.fernet import Fernet

from dist_zero import messages, ids
from dist_zero.node.io import leaf_html


@pytest.fixture
def leaf_config(request):
  node_handle = {
      'id': ids.new_id('DataNode_test'),
      'controller_id': ids.new_id('MachineController_test'),
      'transport': messages.machine.ip_transport('127.0.0.1'),
      'fernet_key': Fernet.generate_key().decode(messages.ENCODING),
  }
  return messages.io.data_node_config(
      node_id=ids.new_id('LeafNode_test'),
      parent=node_handle,
      variant='input',
      height=0,
      leaf_config=messages.io.sum_leaf_config(0),
  )


def test_make_leaf_html(leaf_config):
  leaf_html.from_kid_config(leaf_config)
