import logging
import os
import time

from collections import defaultdict

import boto3
import paramiko
import paramiko.ssh_exception

from dist_zero import settings, messages, spawners, transport
from .. import spawner

logger = logging.getLogger(__name__)


class Ec2Spawner(spawner.Spawner):
  def __init__(self, aws_region=settings.DEFAULT_AWS_REGION):
    self._handle_by_id = {} # id to machine controller handle
    self._aws_instance_by_id = {} # id to the boto instance object

    self._aws_region = aws_region

    if not aws_region:
      raise RuntimeError("Missing aws region parameter")

    self._ec2 = boto3.client(
        'ec2',
        region_name=self._aws_region,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY_ID)
    self._ec2_resource = boto3.resource(
        'ec2',
        region_name=self._aws_region,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY_ID)

  def mode(self):
    return spawners.MODE_CLOUD

  def clean_all(self):
    if self._aws_instance_by_id:
      self._ec2.terminate_instances(InstanceIds=[instance.id for instance in self._aws_instance_by_id.values()], )

  def create_machine(self, machine_config):
    return self.create_machines([machine_config])[0]

  def _instance_placement(self):
    return {
        'AvailabilityZone': self._aws_region,
        #'Affinity': 'string',
        #'GroupName': 'string',
        'HostId': 'string',
        'Tenancy': 'default',
        #'SpreadDomain': 'string'
    },

  def _instance_block_device_mappings(self):
    return [
        {
            'DeviceName': '',
            'VirtualName': '',
            'Ebs': {
                'Encrypted': False,
                'DeleteOnTermination': True,
                'Iops': 123,
                'KmsKeyId': 'string',
                'SnapshotId': 'string',
                'VolumeSize': 123,
                'VolumeType': 'gp2',
            },
        },
    ]

  def _instance_tag_specifications(self):
    return [{
        'ResourceType':
        'instance',
        'Tags': [
            {
                'Key': 'Application',
                'Value': 'dist_zero'
            },
            {
                'Key': 'dist_zero_type',
                'Value': 'std_instance'
            },
        ],
    }]

  def create_machines(self, machine_configs):
    # see http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
    # For available parameters.
    logger.info("Creating an instance on on aws ec2")
    n_new_machines = len(machine_configs)
    instances = self._ec2_resource.create_instances(
        #BlockDeviceMappings=self._instance_block_device_mappings(),
        #Placement=self._instance_placement(),
        ImageId='ami-20335f58',
        KeyName='dist_zero',
        MaxCount=n_new_machines,
        MinCount=n_new_machines,
        InstanceType='t1.micro',
        Monitoring={'Enabled': False},
        SecurityGroupIds=['sg-ebd74d95'],
        InstanceInitiatedShutdownBehavior='stop',
        TagSpecifications=self._instance_tag_specifications(),
    )
    self._wait_for_running_instances(instances)
    # NOTE(KK): reachability can take longer than it takes before ssh works.  We should skip this check to run faster.
    #self._wait_for_reachable_instances(instances)

    return [
        self._configure_instance(instance, machine_config)
        for instance, machine_config in zip(instances, machine_configs)
    ]

  def _configure_instance(self, instance, machine_config):
    machine_name = machine_config['machine_name']
    machine_controller_id = machine_config['id']
    extra = {
        'machine_name': machine_name,
        'machine_controller_id': machine_controller_id,
    }
    logger.info("Configuring instance '{machine_name}' id='{machine_controller_id}'", extra=extra)

    instance.create_tags(Tags=[
        {
            'Key': 'Name',
            'Value': machine_name
        },
        {
            'Key': 'machine_controller_id',
            'Value': machine_controller_id
        },
    ])

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
    connected = False
    for i in range(5):
      try:
        ssh.connect(hostname=instance.public_dns_name, key_filename='.keys/dist_zero.pem', username='ec2-user')
        connected = True
        break
      except paramiko.ssh_exception.NoValidConnectionsError:
        time.sleep(2)

    if not connected:
      raise RuntimeError("Could not connect via ssh")

    for command in [
        "sudo yum update -y",
        "sudo yum install -y python36",
        "sudo easy_install-3.6 pip",
        "sudo /usr/local/bin/pip3 install --upgrade pip",
        "sudo mkdir -p /dist_zero",
        "sudo mkdir -p /logs",
        "sudo chown ec2-user /dist_zero",
        "sudo chown ec2-user /logs",
    ]:
      ssh.exec_command(command)

    logger.info("Rsyncing dist_zero to aws instance", extra=extra)
    # Do the rsync
    for precommand in [
        'rsync -avz -e "ssh -oStrictHostKeyChecking=no -i {keyfile}" dist_zero {user}@{instance}:/dist_zero/',
        'rsync -avz -e "ssh -oStrictHostKeyChecking=no -i {keyfile}" requirements.txt {user}@{instance}:/dist_zero/requirements.txt',
    ]:

      command = precommand.format(keyfile='.keys/dist_zero.pem', user='ec2-user', instance=instance.public_dns_name)
      os.system(command)

    ssh.exec_command('cd /dist_zero; pip3 install --user -r requirements.txt')

    ssh.exec_command('''cat << EOF > /dist_zero/.env\n\n{}\nEOF\n'''.format('\n'.join(
        "{}='{}'".format(variable, getattr(settings, variable)) for variable in settings.CLOUD_ENV_VARS)))

    command = "cd /dist_zero; nohup python3 -m dist_zero.machine_init '{machine_controller_id}' '{machine_name}' '{mode}' &".format(
        machine_controller_id=machine_controller_id, machine_name=machine_name, mode=spawners.MODE_CLOUD)
    logger.info("Starting machine_init on aws instance", extra=extra)
    ssh.exec_command(command)

    handle = messages.os_machine_controller_handle(machine_controller_id)
    self._handle_by_id[machine_controller_id] = handle
    self._aws_instance_by_id[machine_controller_id] = instance
    return handle

  def _instance_status_is_reachable(self, status):
    details = status['InstanceStatus']['Details']
    reachabilities = [detail for detail in details if detail['Name'] == 'reachability']
    if len(reachabilities) == 1:
      reachability = reachabilities[0]
      if reachability['Status'] == 'passed':
        return True

    return False

  def _wait_for_reachable_instances(self, instances):
    logger.info(
        "Waiting for aws instances to enter a 'reachable' status",
        extra={'instance_ids': [instance.id for instance in instances]})

    while True:
      resp = self._ec2.describe_instance_status(InstanceIds=[instance.id for instance in instances])
      if all(self._instance_status_is_reachable(status) for status in resp['InstanceStatuses']):
        logger.info("The requested aws instances are now reachable")
        break
      time.sleep(4)

  def _wait_for_running_instances(self, instances):
    logger.info(
        "Waiting for aws instances to enter a 'running' state",
        extra={'instance_ids': [instance.id for instance in instances]})

    while True:
      for instance in instances:
        instance.load()
      states = [instance.state['Name'] for instance in instances]
      if all(state == 'running' for state in states):
        logger.info("The requested aws instances are now running")
        break

      non_running = defaultdict(int)
      for state in states:
        if state != 'running':
          non_running[state] += 1

      logger.info(
          "Not all instances were running", extra={
              'non_running_instance_counts': non_running,
          })
      time.sleep(4)

  def send_to_machine(self, machine, message, sock_type='udp'):
    instance = self._aws_instance_by_id[machine['id']]
    instance.load()

    if sock_type == 'udp':
      dst = (instance.public_ip_address, settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT)
      return transport.send_udp(message, dst)
    elif sock_type == 'tcp':
      dst = (instance.public_ip_address, settings.MACHINE_CONTROLLER_DEFAULT_TCP_PORT)
      return transport.send_tcp(message, dst)
    else:
      raise RuntimeError("Unrecognized sock_type {}".format(sock_type))
