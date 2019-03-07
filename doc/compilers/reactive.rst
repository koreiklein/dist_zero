Compiling a Reactive Program for a Single Machine
==================================================

Reactive programming in DistZero proceeds as follows:

  - A user defines a set of reactive expressions ``E`` in a high-level
    language embedded in python.  See `ConcreteExpression` for the representation of these
    high-level language expressions.
  - The expressions are compiled into a running reactive network ``net``.
  - ``net`` is gradually initialized as the user provides it values for the input expressions in ``E``.
  - ``net`` computes the output expressions in ``E``
  - Over time, the user submits various transitions on the input expressions of ``E`` to ``net``,
    and ``net`` computes the appropriate transitions on the output expressions of ``E``

See `ReactiveCompiler.compile` for a detailed description of exactly how to produce and interact
with these ``net`` objects.

.. automodule:: dist_zero.reactive.compiler
   :members:

