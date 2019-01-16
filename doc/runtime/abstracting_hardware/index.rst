Abstracting the DistZero Runtime over Physical Hardware
========================================================

Most of the DistZero runtime is written on top of an abstract interface to distributed
hardware.  This interface has many distinct implementations, for running the runtime in e.g.

  - **simulated:** A single process
  - **virtualized:** Multiple containers on a single host
  - **cloud:** Multiple hosts provisioned with a cloud provider

Diagrammatically:

.. graphviz::

   digraph {
     datasets -> interface
     links -> interface
     other -> interface

     interface -> simulated
     interface -> virtualized
     interface -> cloud

     interface [label="Distributed Hardware Interface",shape=ellipse]
     datasets [label="Datasets",shape=polygon,sides=4]
     links [label="Links",shape=polygon,sides=4]
     other [label="Other processes in the distrubed runtime",shape=polygon,sides=4]
     simulated [label="Simulated Hardware",shape=polygon,sides=4]
     virtualized [label="Virtualized Hardware",shape=polygon,sides=4]
     cloud [label="Cloud Provider Hardware",shape=polygon,sides=4]
   }

Abstracting past the hardware this way has a number of advantages:

  - By using a simulated implementation of the hardware, distributed system tests can run very fast but still provide
    good coverage of functionality that will eventually be split between a large number of machines.
    The tests can achieve a 100X to 10000X speedup by simulating the passage of time at a faster rate
    (see `SimulatedSpawner.run_for` and the implementation of `SimulatedSpawner.sleep_ms`).  They also benefit
    from having less overall overhead (e.g. no need to spin up docker containers or provision hosts in the cloud).
  - By factoring away the OS and network related code, DistZero has a dramatically simplified implementation
    of its fundamental distributed datastructures like :ref:`dataset` and :ref:`link`
  - Certain kinds of performance improvements can be implemented entierly within the **cloud** implementation
    with wide-reaching benefits to all the runtime code.  Examples include:

    - Optimizing network communications.
    - Optimizing the placement of units of computation.

.. toctree::
   :maxdepth: 2

   interface
   implementation
   load_balancer

   spawners/index




