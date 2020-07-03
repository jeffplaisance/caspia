# CASPIA

Caspia is a collection of consensus algorithms built on top of unmodified existing datastores. The underlying datastore must be durable and provide single row compare-and-set and put-if-absent operations which return success or failure. notably, there is no requirement for idempotence or repeatability of these operations. Caspia is implemented completely as a client library, which has the advantage of greatly reducing implementation complexity in addition to simplifying deployment.

Caspia provides two basic distributed primitives, a register and a log.

## Caspia Register

A distributed modifiable register which stores a single value. The available operations are reading the value and modifying the value by application of a function. The register abstraction also provides the ability to reconfigure the quorum by adding or removing one replica at a time. This primitive arose from an original idea that was then heavily influenced by [CASPaxos](https://arxiv.org/abs/1802.07000). The combination of the client and proposer roles required a unique approach to quorum reconfiguration.

## Caspia Log

A distributed log which stores a sequence of values, each of which is immutable once decided. Each entry in the log has a sequentially assigned integer index. A client can attempt to read a value at any index. A correct client will only be able to write a value at an index at which no quorum has already accepted another value. A correct client will only attempt to write a value at either index 1 or an index which immediately follows an index for which that client is certain a value has been accepted by a quorum of replicas. The log abstraction is not inherently reconfigurable but may be reconfigured by placing a metadata record in the log which indicates the new quorum. Viewed from the state machine replication approach, the log abstraction in Caspia deliberately separates the responsibility for determining a total ordering of actions from materializing the state resulting from the application of those actions. This means that there is no requirement for an individual replica to store every action, resulting in greatly reduced implementation complexity as well as far simpler recovery than protocols such as Raft. At its core, the log implementation is very similar to multi paxos.

Both the register and the log contain optimizations allowing for a stable client to do consecutive writes in only 1 round trip. When the active client changes, 3 round trips are required for the register protocol and 2 round trips are required by the log protocol (plus any additional round trips which may be needed by the new client to replay actions added to the log by the previous client). These protocols are intended to be used as the persistence layer for systems with a stable leader, and as such the vast majority of writes should complete in 1 round trip.

Currently an implementation is provided in java using MariaDB or MySQL as the backing storage.

Future extensions include:
1. Support for more languages
2. Support for more backing stores
3. Erasure coded (instead of replicated) log and write-once register implementations
4. Better documentation, explanations, and formal proofs
5. A paper

References:
1. [CASPaxos](https://arxiv.org/abs/1802.07000)
2. [Paxos Made Simple](https://www.microsoft.com/en-us/research/publication/paxos-made-simple/)
