Language for Distributed Programs
===================================


Externally Facing Language
-----------------------------

DistZero contains the following embedded "frontend" language for defining distributed programs.
Ultimately, end users will produce a program defined by `Expression` instances,
and the `DistributedCompiler` will compile it, and a `ProgramNode` will run it.

Semantic "Frontend" Expressions and Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following clases are meant to describe a semantic denotational program.
They describe exactly the end-of-day behavior the program should follow, but not the details
of how it is implemented.

.. automodule:: dist_zero.types
   :members:

.. automodule:: dist_zero.expression
   :members:

.. automodule:: dist_zero.primitive
   :members:

Internal Distirbuted Program Language
----------------------------------------

DistZero contains the following internal representation of distirbuted programs.

This representation serves as the link between the `DistributedCompiler` and the runtime.
`DistirbutedPrograms <DistributedProgram>` are produced by `DistributedCompiler.compile` and can
be run by a `ProgramNode` in the DistZero runtime.

Programs
~~~~~~~~~~

.. autoclass:: dist_zero.program.DistributedProgram
   :members:

Datasets
~~~~~~~~~~

.. autoclass:: dist_zero.program.Dataset
   :members:

Links
~~~~~~~~~~

.. autoclass:: dist_zero.program.Link
   :members:

Concrete "Backend" Expressions and Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following classes are meant to describe the exact operation of one `dataset's <dist_zero.program.Dataset>` worth
of expressions.  They define the exact wire format and in-memory representation of each type, and can be
translated into compiled code to implement the behavior of each expression.

They also serve as inputs to each of the `ReactiveCompilers <ReactiveCompiler>` that live on the various
machines participating in implementing any one distributed program.

.. automodule:: dist_zero.reactive.expression
   :members:

.. automodule:: dist_zero.concrete_types
   :members:

