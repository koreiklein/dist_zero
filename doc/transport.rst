.. _transport:

Transport
===========================

A "transport" is a `json` serializable object that represents the information one `Node` needs in
order to communicate with another `Node`.  Each transport allows a single "sending" `Node` to send messages
to a unique "receiving" `Node`.

Transports should include all the network information about the receiving `Node`, along with any cryptographic
information used to secure the connection.

A transport that allows for sending messages to a node must be created either:

- by the receiving `Node`, from the node_id of the sending `Node`. (see `Node.new_transport_for`)
- by converting a separate transport to a new sender. (see `Node.convert_transport_for`)

