import json
import logging
import os
import tempfile
import time
import uuid

from collections import defaultdict

import boto3
import paramiko
import paramiko.ssh_exception

from dist_zero import settings, messages, spawners, transport
from .. import spawner

logger = logging.getLogger(__name__)

DNS_TTL = 1500


class Ec2Spawner(spawner.Spawner):
  '''
  A `Spawner` subclass that creates each new `MachineController` instance by spinning up an aws ec2 instance
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
    self.aws_instance_by_id = {} # id to the boto instance object

    self._aws_region = aws_region
    self._base_ami = base_ami
    self._security_group = security_group
    self._instance_type = instance_type

    self._owned_instances = []

    if not aws_region:
      raise RuntimeError("Missing aws region parameter")
    if not base_ami:
      raise RuntimeError("Missing base_ami parameter")
    if not security_group:
      raise RuntimeError("Missing security_group parameter")
    if not instance_type:
      raise RuntimeError("Missing instance_type parameter")

    self._route53 = self._create_client_by_name('route53')
    self._ec2 = self._create_client_by_name('ec2')
    self._ec2_resource = boto3.resource(
        'ec2',
        region_name=self._aws_region,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY_ID)

  def sleep_ms(self, ms):
    return asyncio.sleep(ms / 1000)

  def _create_client_by_name(self, aws_service_name):
    return boto3.client(
        aws_service_name,
        region_name=self._aws_region,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY_ID)

  def remote_spawner_json(self):
    '''Generate an `Ec2Spawner` config for a new machine.'''
    return {
        'system_id': self._system_id,
        'aws_region': self._aws_region,
        'base_ami': self._base_ami,
        'security_group': self._security_group,
        'instance_type': self._instance_type,
    }

  @staticmethod
  def from_spawner_json(spawner_config):
    logger.info("Creating {parsed_spawner_type} from spawner_config", extra={'parsed_spawner_type': 'Ec2Spawner'})
    return Ec2Spawner(
        system_id=spawner_config['system_id'],
        aws_region=spawner_config['aws_region'],
        base_ami=spawner_config['base_ami'],
        security_group=spawner_config['security_group'],
        instance_type=spawner_config['instance_type'],
    )

  def mode(self):
    return spawners.MODE_CLOUD

  def clean_all(self):
    if self._owned_instances:
      instance_ids = [instance.id for instance in self._owned_instances]
      self._ec2.create_tags(Resources=instance_ids, Tags=[{'Key': 'DistZeroFree', 'Value': 'True'}])
      #self._ec2.terminate_instances(InstanceIds=instance_ids, )

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

  def _instance_tags(self):
    return [{
        'Key': 'Application',
        'Value': 'dist_zero'
    }, {
        'Key': 'dist_zero_type',
        'Value': 'std_instance'
    }, {
        'Key': 'DistZeroFree',
        'Value': 'False'
    }, {
        'Key': 'System ID',
        'Value': self._system_id,
    }]

  def _instance_tag_specifications(self):
    return [{
        'ResourceType': 'instance',
        'Tags': self._instance_tags(),
    }]

  def _free_instance_ids(self):
    '''
    Search AWS for instances that are for dist_zero but are not part of any system
    and return their ids.
    '''
    return [
        instance['InstanceId'] for reservation in self._ec2.describe_instances(Filters=[
            {
                'Name': 'instance-state-name',
                'Values': ['running']
            },
            {
                'Name': 'tag:Application',
                'Values': ['dist_zero']
            },
            {
                'Name': 'tag:DistZeroFree',
                'Values': ['True']
            },
        ])['Reservations'] for instance in reservation['Instances']
    ]

  def create_machines(self, machine_configs):
    # see http://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
    # For available parameters.
    n_new_machines = len(machine_configs)

    free_instance_ids = self._free_instance_ids()
    if free_instance_ids:
      instances = list(self._ec2_resource.instances.filter(InstanceIds=free_instance_ids))[:n_new_machines]
      self._ec2.create_tags(Resources=free_instance_ids, Tags=self._instance_tags())
    else:
      instances = []
    n_reused_machines = len(instances)

    n_missing_machines = n_new_machines - n_reused_machines
    logger.info("Creating new instance(s) on on aws ec2. n={n_instances}", extra={'n_instances': n_new_machines})
    if n_missing_machines > 0:
      instances += self._ec2_resource.create_instances(
          #BlockDeviceMappings=self._instance_block_device_mappings(),
          #Placement=self._instance_placement(),
          ImageId=self._base_ami,
          KeyName='dist_zero',
          MaxCount=n_missing_machines,
          MinCount=n_missing_machines,
          InstanceType=self._instance_type,
          Monitoring={'Enabled': False},
          SecurityGroupIds=[self._security_group],
          InstanceInitiatedShutdownBehavior='stop',
          TagSpecifications=self._instance_tag_specifications(),
      )
    self._owned_instances = instances
    self._wait_for_running_instances(instances)
    # NOTE(KK): reachability can take longer than it takes before ssh works.  We should skip this check to run faster.
    #self._wait_for_reachable_instances(instances)

    result = []
    for instance, machine_config in zip(instances, machine_configs):
      machine_controller_id = _AwsInstanceProvisioner(
          instance=instance, machine_config=machine_config, ec2_spawner=self).configure_instance()
      self.aws_instance_by_id[machine_controller_id] = instance
      result.append(machine_controller_id)

    return result

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

  def _canonicalize_domain_name(self, domain_name):
    if domain_name[-1] != '.':
      return domain_name + '.'
    else:
      return domain_name

  def _get_zone_for_domain_name(self, canonical_domain_name):
    parts = canonical_domain_name.split('.')
    hosted_zones = self._route53.list_hosted_zones()['HostedZones']
    zone_by_name = {zone['Name']: zone for zone in hosted_zones}

    for i in range(len(parts)):
      suffix = '.'.join(parts[i:])
      if suffix in zone_by_name:
        return zone_by_name[suffix]
    return None

  def _split_by_desired_zone_name(self, canonical_domain_name):
    parts = canonical_domain_name.split('.')
    return '.'.join(parts[:-3]), '.'.join(parts[-3:])

  def _create_new_zone(self, canonical_domain_name):
    before_zone_name, zone_name = self._split_by_desired_zone_name(canonical_domain_name)
    response = self._route53.create_hosted_zone(
        Name=zone_name,
        CallerReference=uuid.uuid4(),
        #VPC
        #HostedZoneConfig
        #DelegationSetId
    )
    return response['HostedZone']

  def _make_domain_upsert_change(self, domain_name, ip_address):
    return {
        'Action': 'UPSERT',
        'ResourceRecordSet': {
            'Name': domain_name,
            'Type': 'A',
            'TTL': DNS_TTL,
            'ResourceRecords': [{
                'Value': ip_address
            }],
        },
    }

  def map_domain_to_ip(self, domain_name, ip_address):
    canonical_domain_name = self._canonicalize_domain_name(domain_name)
    zone = self._get_zone_for_domain_name(canonical_domain_name)
    if zone is None:
      zone = self._create_new_zone(canonical_domain_name)

    self._route53.change_resource_record_sets(
        HostedZoneId=zone['Id'],
        ChangeBatch={
            'Comment': 'Done as part of some scripted testing',
            'Changes': [self._make_domain_upsert_change(domain_name=canonical_domain_name, ip_address=ip_address)],
        })


class _AwsInstanceProvisioner(object):
  RSYNC_SSH_PARAMS = 'ssh -oStrictHostKeyChecking=no -i {keyfile}'
  RSYNC_EXCLUDES = ''.join(' --exclude "{}"'.format(exp) for exp in [
      "*.pyc",
      "*__pycache__*",
      ".pytest_cache*",
      ".tmp*",
      ".git*",
      ".keys*",
  ])

  def __init__(self, instance, machine_config, ec2_spawner):
    self._instance = instance
    self._machine_config = machine_config
    self._ec2_spawner = ec2_spawner

    self._machine_name = self._machine_config['machine_name']
    self._machine_controller_id = self._machine_config['id']
    self._extra = {
        'machine_name': self._machine_name,
        'machine_controller_id': self._machine_controller_id,
        'aws_instance_id': self._instance.id,
        'provisioning_aws_instance': True,
    }

  def _exec_commands(self, ssh, commands):
    # NOTE(KK): ``commands`` may contain sensitive data like cloud access credentials.
    #  They need to be send SAFELY to the remote host via ssh.
    #  Please be a mensch and do not let them get into the logs, or anywhere else they shouldn't be.
    command = " && ".join(commands)
    stdin, stdout, stderr = ssh.exec_command(command)
    # Wait for the command to finish
    status = stdout.channel.recv_exit_status()
    if 0 != status:
      raise RuntimeError("Command did not execute with 0 exit status."
                         " Got {}: {}.".format(status, ''.join(stderr.readlines())))

  def _connect_as(self, key_filename, username):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connected = False
    logger.debug("Connecting to aws instance {aws_instance_id} over ssh", extra=self._extra)
    for i in range(8):
      try:
        ssh.connect(hostname=self._instance.public_dns_name, key_filename=key_filename, username=username)
        connected = True
        break
      except paramiko.ssh_exception.NoValidConnectionsError:
        logger.debug("Ssh failed to connect to instance {aws_instance_id}", extra=self._extra)
        time.sleep(2)

    if not connected:
      raise RuntimeError("Could not connect via ssh")

    return ssh

  def _write_machine_config_locally(self):
    '''Write a local file for this instance's machine_config.json and return the filename'''
    machine_config_with_extras = dict(self._machine_config)
    machine_config_with_extras.update({
        'spawner': {
            'type': 'aws',
            'value': self._ec2_spawner.remote_spawner_json()
        },
        'ip_address': self._instance.public_ip_address,
    })
    # Create local machine config file
    tempdir = tempfile.mkdtemp()
    filename = os.path.join(tempdir, 'machine_config.json')
    with open(filename, 'w') as f:
      json.dump(machine_config_with_extras, f)
    return filename

  def _rsync(self, source, target):
    '''Rsync as the dist_zero user on the remote instance.'''
    command = 'rsync -avz -e "{rsync_ssh_params}" {rsync_excludes} {source} {user}@{instance}:{target} >> {outfile}'.format(
        source=source,
        target=target,
        rsync_ssh_params=_AwsInstanceProvisioner.RSYNC_SSH_PARAMS.format(keyfile='.keys/dist_zero.pem'),
        rsync_excludes=_AwsInstanceProvisioner.RSYNC_EXCLUDES,
        user='dist_zero',
        instance=self._instance.public_dns_name,
        outfile='.rsync.output.log',
    )
    logger.debug(
        "Running rsync command",
        extra={
            'aws_instance_id': self._instance.id,
            'provisioning_aws_instance': True,
            'command': command,
            'machine_name': self._machine_name,
            'machine_controller_id': self._machine_controller_id,
        })
    os.system(command)

  def configure_instance(self):
    logger.info("Configuring instance '{machine_name}' id='{machine_controller_id}'", extra=self._extra)

    logger.debug("Configuring machine specific tags", extra=self._extra)
    self._instance.create_tags(Tags=[
        {
            'Key': 'Name',
            'Value': self._machine_name
        },
        {
            'Key': 'machine_controller_id',
            'Value': self._machine_controller_id
        },
    ])

    logger.info("Rsyncing dist_zero files to aws instance {aws_instance_id}", extra=self._extra)

    # Wait for ssh to be up and running *before* running any rsync.
    ssh = self._connect_as(key_filename='.keys/dist_zero.pem', username='dist_zero')

    recycle_in_secs = 3
    self._exec_commands(
        ssh,
        [
            # Allow faster recycling of tcp connections
            "sudo /sbin/sysctl -w net.ipv4.tcp_tw_recycle={}".format(recycle_in_secs),
            # In case it's already running, stop the dist-zero process
            "sudo /usr/bin/systemctl stop dist-zero",
        ])

    # Do the rsync
    local_config_filename = self._write_machine_config_locally()
    self._rsync('dist_zero Pipfile Pipfile.lock {}'.format(local_config_filename), '/dist_zero/')

    logger.debug("Copying relevant environment variables", extra=self._extra)

    copy_environment_command = '''cat << EOF > /dist_zero/.env\n\n{}\nEOF'''.format('\n'.join(
        "{}='{}'".format(variable, getattr(settings, variable)) for variable in settings.CLOUD_ENV_VARS))

    self._exec_commands(ssh, [copy_environment_command])
    self._exec_commands(
        ssh,
        [
            # FIXME(KK): It would be better to always do a pipenv install, but it's just so darn slow!
            #   See https://github.com/pypa/pipenv/issues/2207
            #"cd /dist_zero && pipenv install",
            "sudo /usr/bin/systemctl enable dist-zero",
            "sudo /usr/bin/systemctl start dist-zero",
        ])

    return self._machine_controller_id
