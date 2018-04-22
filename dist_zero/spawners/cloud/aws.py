import logging
import os
import time
import uuid

from collections import defaultdict

import boto3
import paramiko
import paramiko.ssh_exception

from dist_zero import settings, messages, spawners, transport
from .. import spawner

logger = logging.getLogger(__name__)


class Ec2Spawner(spawner.Spawner):
  '''
  A 'Spawner` subclass that creates each new `MachineController` instance by spinning up an aws ec2 instance
  and starting a long-running `dist_zero.machine_init` process on it.
  '''

  def __init__(self,
               system_id,
               aws_region=settings.DEFAULT_AWS_REGION,
               base_ami=settings.AWS_BASE_AMI,
               security_group=settings.DEFAULT_AWS_SECURITY_GROUP,
               instance_type=settings.DEFAULT_AWS_INSTANCE_TYPE):
    '''
    :param str system_id: The id of  the overall distributed system
    :param str aws_region: The aws region in which to spawn the new ec2 instances.
    :param str base_ami: A base aws ami image to use when spawning new ec2 instances.
    :param str security_group: A security group to add to all new ec2 instances.  It must open the appropriate ports.
      Ports for communication with running instances are defined in `dist_zero.settings`
    :param str instance_type: The aws instance type (e.g. 't2.micro')
    '''
    self._system_id = system_id
    self._handle_by_id = {} # id to machine controller handle
    self._aws_instance_by_id = {} # id to the boto instance object

    self._aws_region = aws_region
    self._base_ami = base_ami
    self._security_group = security_group
    self._instance_type = instance_type

    if not aws_region:
      raise RuntimeError("Missing aws region parameter")
    if not base_ami:
      raise RuntimeError("Missing base_ami parameter")
    if not security_group:
      raise RuntimeError("Missing security_group parameter")
    if not instance_type:
      raise RuntimeError("Missing instance_type parameter")

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
        'Tags': [{
            'Key': 'Application',
            'Value': 'dist_zero'
        }, {
            'Key': 'dist_zero_type',
            'Value': 'std_instance'
        }, {
            'Key': 'System ID',
            'Value': self._system_id,
        }],
    }]

  def create_machines(self, machine_configs):
    # see http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
    # For available parameters.
    n_new_machines = len(machine_configs)
    logger.info("Creating new instance(s) on on aws ec2. n={n_instances}", extra={'n_instances': n_new_machines})
    instances = self._ec2_resource.create_instances(
        #BlockDeviceMappings=self._instance_block_device_mappings(),
        #Placement=self._instance_placement(),
        ImageId=self._base_ami,
        KeyName='dist_zero',
        MaxCount=n_new_machines,
        MinCount=n_new_machines,
        InstanceType=self._instance_type,
        Monitoring={'Enabled': False},
        SecurityGroupIds=[self._security_group],
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
        'aws_instance_id': instance.id,
        'provisioning_aws_instance': True,
    }
    logger.info("Configuring instance '{machine_name}' id='{machine_controller_id}'", extra=extra)

    logger.debug("Configuring machine specific tags", extra=extra)
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
    logger.debug("Connecting to aws instance {aws_instance_id} over ssh", extra=extra)
    for i in range(5):
      try:
        ssh.connect(hostname=instance.public_dns_name, key_filename='.keys/dist_zero.pem', username='ec2-user')
        connected = True
        break
      except paramiko.ssh_exception.NoValidConnectionsError:
        logger.debug("Ssh failed to connect to instance {aws_instance_id}", extra=extra)
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

    logger.info("Rsyncing dist_zero files to aws instance {aws_instance_id}", extra=extra)

    # Do the rsync
    rsync_ssh_params = 'ssh -oStrictHostKeyChecking=no -i {keyfile}'
    rsync_excludes = '--exclude "*.pyc" --exclude "*__pycache__*"'
    for precommand in [
        'rsync -avz -e "{rsync_ssh_params}" {rsync_excludes} dist_zero {user}@{instance}:/dist_zero/ >> {outfile}',
        'rsync -avz -e "{rsync_ssh_params}" {rsync_excludes} requirements.txt {user}@{instance}:/dist_zero/requirements.txt >> {outfile}',
    ]:
      command = precommand.format(
          rsync_ssh_params=rsync_ssh_params.format(keyfile='.keys/dist_zero.pem'),
          rsync_excludes=rsync_excludes,
          user='ec2-user',
          instance=instance.public_dns_name,
          outfile='.rsync.output.log',
      )
      logger.debug(
          "Running rsync command",
          extra={
              'aws_instance_id': instance.id,
              'provisioning_aws_instance': True,
              'command': command,
          })
      os.system(command)

    logger.debug("Running pip install", extra=extra)
    ssh.exec_command('cd /dist_zero; pip3 install --user -r requirements.txt')

    logger.debug("Copying relevant environment variables", extra=extra)
    ssh.exec_command('''cat << EOF > /dist_zero/.env\n\n{}\nEOF\n'''.format('\n'.join(
        "{}='{}'".format(variable, getattr(settings, variable)) for variable in settings.CLOUD_ENV_VARS)))

    command = "cd /dist_zero; nohup python3 -m dist_zero.machine_init '{machine_controller_id}' '{machine_name}' '{mode}' &".format(
        machine_controller_id=machine_controller_id, machine_name=machine_name, mode=spawners.MODE_CLOUD)
    logger.info("Starting a MachineController process on an aws instance", extra=extra)
    ssh.exec_command(command)

    handle = messages.os_machine_controller_handle(machine_controller_id)
    self._handle_by_id[machine_controller_id] = handle
    self._aws_instance_by_id[machine_controller_id] = instance
    return handle

  def _instance_status_is_reachable(self, status):
    '''
    Check if a machine is reachable.

    :param object status: An InstanceStatus object as returned by the boto3 API.
    :return: True iff the status object indicates that the machine is reachable.
    :rtype: bool
    '''
    details = status['InstanceStatus']['Details']
    reachabilities = [detail for detail in details if detail['Name'] == 'reachability']
    if len(reachabilities) == 1:
      reachability = reachabilities[0]
      if reachability['Status'] == 'passed':
        return True

    return False

  def _retry_loop(self, on_wait, on_fail, wait_time_sec=4, retries=10):
    def _result(loop_iteration):
      remaining = retries
      while True:
        if loop_iteration():
          return
        remaining -= 1
        if remaining >= 0:
          on_wait()
          time.sleep(wait_time_sec)
        else:
          on_fail()

    return _result

  def _wait_for_reachable_instances(self, instances):
    '''
    Wait until the boto3 API indicates that the desired are reachable.

    :param list instances: A list of aws instances.
    '''
    instance_ids = [instance.id for instance in instances]
    logger.info("Waiting for aws instances to enter a 'reachable' status", extra={'instance_ids': instance_ids})

    def on_wait():
      logger.debug(
          "aws instances were not all reachable.  Waiting and trying again.", extra={'instance_ids': instance_ids})

    def on_fail():
      logger.error("instances did not become reachable.", extra={'instance_ids': instance_ids})
      raise RuntimeError("Instances did not become reachable")

    @self._retry_loop(on_wait=on_wait, on_fail=on_fail)
    def _loop():
      resp = self._ec2.describe_instance_status(InstanceIds=instance_ids)
      if all(self._instance_status_is_reachable(status) for status in resp['InstanceStatuses']):
        logger.info("The requested aws instances are now reachable", extra={'instance_ids': instance_ids})
        return True
      else:
        return False

  def _wait_for_running_instances(self, instances):
    instance_ids = [instance.id for instance in instances]
    logger.info("Waiting for aws instances to enter a 'running' state", extra={'instance_ids': instance_ids})

    def on_wait():
      logger.debug("instances were not all running.  Waiting and trying again.", extra={'instance_ids': instance_ids})

    def on_fail():
      logger.error("Instances did not become running.", extra={'instance_ids': instance_ids})
      raise RuntimeError("Instances did not become running.")

    @self._retry_loop(on_wait=on_wait, on_fail=on_fail)
    def _loop():
      for instance in instances:
        instance.load()

      states = [instance.state['Name'] for instance in instances]

      if all(state == 'running' for state in states):
        logger.info("The requested aws instances are now running")
        return True
      else:
        return False

  def send_to_machine(self, machine, message, sock_type='udp'):
    instance = self._aws_instance_by_id[machine['id']]

    if sock_type == 'udp':
      dst = (instance.public_ip_address, settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT)
      return transport.send_udp(message, dst)
    elif sock_type == 'tcp':
      dst = (instance.public_ip_address, settings.MACHINE_CONTROLLER_DEFAULT_TCP_PORT)
      return transport.send_tcp(message, dst)
    else:
      raise RuntimeError("Unrecognized sock_type {}".format(sock_type))
