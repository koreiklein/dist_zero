Classes for resending and acknowledging messages
==================================================

The linker/exporter/importer classes are designed to cover all the logic regarding message acknowledgement and
retransmission.  Nodes can ensure messages are retransmitted and acknowledged appropriately by receiving all
their messages with importers, sending all their messages with exporters, and updating their internal sequence
number from time to time.  The linker connects importers with exporters, and will ensure that acknowledgements
are sent on any importer for any message M once all the messages exporter because of M have been acknowledged.

.. automodule:: dist_zero.linker
   :members:

.. automodule:: dist_zero.importer
   :members:

.. automodule:: dist_zero.exporter
   :members:

