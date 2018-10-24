from .all_to_all import *
from .all_to_one_available import *


def from_json(j, *args, **kwargs):
  if j['type'] == 'all_to_all_connector':
    return AllToAllConnector.from_json(j, *args, **kwargs)
  elif j['type'] == 'all_to_one_available_connector':
    return AllToOneAvailableConnector.from_json(j, *args, **kwargs)
  else:
    raise errors.InternalError(f"Unrecognized connector type {j['type']}.")


def new_connector(connector_type, left_configurations, right_configurations, computation_node):
  if connector_type['type'] == 'all_to_all_connector':
    return AllToAllConnector(
        height=computation_node.height,
        left_configurations=left_configurations,
        right_configurations=right_configurations,
        left_is_data=computation_node.left_is_data,
        right_is_data=computation_node.right_is_data,
        max_outputs=computation_node.system_config['SUM_NODE_RECEIVER_LIMIT'],
        max_inputs=computation_node.system_config['SUM_NODE_SENDER_LIMIT'],
    )
  elif connector_type['type'] == 'all_to_one_available_connector':
    # FIXME(KK): Add the proper parameters here
    return AllToOneAvailableConnector(
        height=computation_node.height,
        left_configurations=left_configurations,
        right_configurations=right_configurations,
        left_is_data=computation_node.left_is_data,
        right_is_data=computation_node.right_is_data,
        max_outputs=computation_node.system_config['SUM_NODE_RECEIVER_LIMIT'],
        max_inputs=computation_node.system_config['SUM_NODE_SENDER_LIMIT'],
    )
  else:
    raise errors.InternalError(f'Unrecognized connector type "{connector_type["type"]}"')
