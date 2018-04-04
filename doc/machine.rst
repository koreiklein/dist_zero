Machine
====================================

MachineControllers are described in the abstract by `MachineController`.
In simulated environments, they will run as a special implemnetation, the `SimulatedMachineController`,
and in virtual/cloud environments, they run as the `OsMachineController`.

.. automodule:: dist_zero.machine
   :members: MachineController, node_from_config, node_output_file

.. automodule:: dist_zero.os_machine_controller
   :members:
