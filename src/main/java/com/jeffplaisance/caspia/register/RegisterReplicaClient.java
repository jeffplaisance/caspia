package com.jeffplaisance.caspia.register;

import java.io.Closeable;

public interface RegisterReplicaClient extends Closeable {

    RegisterReplicaResponse read(Object index) throws Exception;

    default boolean writeAtomic(
            Object id,
            long proposal,
            long accepted,
            byte[] value,
            long[] replicas,
            byte quorumModified,
            long changedReplica,
            boolean expect_absent,
            long expect_proposal,
            long expect_accepted
    ) throws Exception {
        if (expect_absent) {
            return putIfAbsent(id, proposal, accepted, value, replicas, quorumModified, changedReplica);
        } else {
            return compareAndSet(id, proposal, accepted, value, replicas, quorumModified, changedReplica, expect_proposal, expect_accepted);
        }
    }

    default boolean compareAndSet(
            Object id,
            long proposal,
            long accepted,
            byte[] value,
            long[] replicas,
            byte quorumModified,
            long changedReplica,
            long expect_proposal,
            long expect_accepted
    ) throws Exception {
        return writeAtomic(id, proposal, accepted, value, replicas, quorumModified, changedReplica, false, expect_proposal, expect_accepted);
    }

    default boolean putIfAbsent(
            Object id,
            long proposal,
            long accepted,
            byte[] value,
            long[] replicas,
            byte quorumModified,
            long changedReplica
    ) throws Exception {
        return writeAtomic(id, proposal, accepted, value, replicas, quorumModified, changedReplica, true, 0, 0);
    }

    long getReplicaId();
}
