Abstracting the DistZero Runtime over Physical Hardware
========================================================

The DistZero runtime contains facilities for abstracting away the details of
physical distributed hardware, networks, clusters and operating systems.

Abstracting past the hardware has a number of advantages:

  - By using a simulated implementation of the hardware, distributed system tests can run very fast.
    The tests can be configured to run entirely on a single host or even a single process (drastically reducing
    start up overhead and network latency) but still provide
    good coverage of functionality that will eventually be split between a large number of machines.
  - By factoring away the OS and network related code, DistZero has a dramatically simplified implementation
    of its fundamental distributed datastructures like :ref:`dataset` and :ref:`link`

.. toctree::
   :maxdepth: 2

   interface
   implementation
   load_balancer

   spawners/index




