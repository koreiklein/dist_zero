.. _transaction:

Transactions
=============

Distributed Transactions in DistZero.

Each transaction exists not as a single python object, but as a collection
of related `TransactionRole` instances, each living on its own `Node`.


**Motivation**

DistZero involves many distributed networks of nodes which need to maintain some kind of distributed invariant.
By **distributed invariant** we mean a property that holds of the combined state of many nodes at a slice in time
(e.g. "if ``p`` is a `DataNode` instance and ``k`` is a node whose handle appears in ``p._kids``,
then ``k._parent`` is the handle of ``p``").
In general, the structure of the network of nodes will spend most
of its time meeting all the distributed invariants.  When it's time to change the structure of the network,
(e.g. when adding a new leaf to a tree of `DataNodes <DataNode>` instances, or when bumping its height)
certain distributed invariants will be broken for a brief period of time and shortly thereafter restored.
The Transaction abstraction is meant to simplify reasoning about the global correctness of a system of nodes
under these kinds of circumstances. 

**Lifetime of a transaction**

- A transaction begins on some "originator" node when that node calls `start_transaction_eventually`
  with an `OriginatorRole` instance.
- Some amount of time later, `TransactionRole.run` is called on that role.
- That originator role executes arbitrary code to run the transaction.  Passed into `TransactionRole.run`
  is a `TransactionRoleController` object which is used to enlist other nodes into the transaction and to interact with them.
  While running, any `TransactionRole` may

  - `enlist` existing nodes into the transaction with a `ParticipantRole` describing their role in the transaction.
  - create new nodes with `spawn_enlist`.  These nodes begin their existence running a `ParticipantRole`.
  - `send <TransactionRoleController.send>` messages to `TransactionRoles <TransactionRole>` of the same transaction.
  - `listen <TransactionRoleController.listen>` for messages sent by other roles.
  - make arbitrary modifications to the internal private state of the underlying `Node` instance.
- Once the `TransactionRole.run` method on a role finishes, the role has ended and its underlying `Node` is no longer
  part of the transaction.

**Advantages to using transactions**

Our experience has been that code written using this Transaction framework has been substantially shorter, simpler
and dramatically easier to reason about than code written without such a framework.

Intuitively, by grouping changes into distinct transactions, it becomes possible to reason about them entirely
independently; to prove any one transaction correct, we need to reason about its state and the state of the underlying
nodes, but not the state of any other transactions.  For example, we don't have to reason about what happens when
a root `DataNode` attempts to run a `ConsumeProxy` operation on a child while that child is in the middle of a
`SplitNode` operation.

**How to reason about networks of nodes that start transactions**

To reason about the overall correctness of a network of nodes that engage in transactions:

  - We state the distributed invariants of the network
  - For each transaction

    - We state the purpose that the transaction is supposed to accomplish
    - We assume that
      
      - at the start of the transacation, all nodes meet their distributed invariants.
      - all calls to `enlist` and `spawn_enlist` eventually start the requested role.
      - all `sends <TransactionRoleController.send>` are eventually received exactly once in the order sent.

    - We then show that

      - the transaction eventually accomplishes its stated purpose
      - as the transaction terminates, it restores all the distributed invariants of the nodes on which it ran

  - Since no node will even run more than one transaction role at a time, we may reason about each transaction
    as though it is the only transaction running.

Please note that the above assumptions are rather strong.  They effectively guarantee message delivery,
the absence of deadlocks when calling `enlist` or `spawn_enlist`, and that the system
starts off meeting its distributed invariants.  **None** of these assumptions come for free, but with them, it is
much easier to write and reason about a transaction.

If we can successfully complete the above steps of reasoning for each transaction independently, then it should
follow under much looser assumptions that every transaction eventually finishes and accomplishes its stated purpose.

.. automodule:: dist_zero.transaction
   :members:

