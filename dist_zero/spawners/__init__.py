'''
DistZero can operate in 3 distinct modes, each with its own `Spawner` class that implements the interface
to distirbuted hardware in its own way.
'''

#: `SimulatedSpawner`: For running machines in a simulation in the parent process.
MODE_SIMULATED = 'simulated'
#: `DockerSpawner`: For running machines as virtual machines on the parent's host.
MODE_VIRTUAL = 'virtual'
#: `Ec2Spawner`: For running machines by provisioning instances in the cloud.
MODE_CLOUD = 'cloud'

#: The list of all modes
ALL_MODES = [MODE_SIMULATED, MODE_VIRTUAL, MODE_CLOUD]
