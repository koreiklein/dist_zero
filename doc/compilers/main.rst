Compilation of Distributed Programs
=======================================

Compilation of a user's program in DistZero is done in several phases.  All the phases
are encapsulated and hidden behind a simple interface provided by the DistZero `DistributedCompiler`.

The Externally-Facing Compiler
------------------------------------------

.. automodule:: dist_zero.compiler.distributed
   :members:

Internal Compiler Phases
-----------------------------

Normalization Phase
~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: dist_zero.compiler.normalize
   :members:

Cardinalization Phase
~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: dist_zero.compiler.cardinality
   :members:

Partitioning Phase
~~~~~~~~~~~~~~~~~~~~

.. automodule:: dist_zero.compiler.partition
   :members:

Localization Phase
~~~~~~~~~~~~~~~~~~~

.. automodule:: dist_zero.compiler.localizer
   :members:
