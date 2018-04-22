'''

There are 3 modes that determine how `MachineController` instances are created and how they operate:
'''

#: For running machines in a simulation in the parent process.
MODE_SIMULATED = 'simulated'
#: For running machines as virtual machines on the parent's host.
MODE_VIRTUAL = 'virtual'
#: For running machines by provisioning instances in the cloud.
MODE_CLOUD = 'cloud'
