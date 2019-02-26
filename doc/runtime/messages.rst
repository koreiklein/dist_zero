Messages
====================================

.. _message:

Message
--------------------------

DistZero messages are `json` serializable objects that are used to represent various the messages that nodes send
to each other.  Each message has a 'type' key, and the other values depend on the type.

.. automodule:: dist_zero.messages
   :members:

Machine messages
-----------------
.. automodule:: dist_zero.messages.machine
   :members:

Transaction messages
----------------------
.. automodule:: dist_zero.messages.transaction
   :members:

IO Tree messages
----------------
.. automodule:: dist_zero.messages.data
   :members:

Sum node messages
------------------
.. automodule:: dist_zero.messages.sum
   :members:

Linker messages
-----------------
.. automodule:: dist_zero.messages.linker
   :members:

Link node messages
---------------------------
.. automodule:: dist_zero.messages.link
   :members:

Common messages
-----------------
.. automodule:: dist_zero.messages.common
   :members:
