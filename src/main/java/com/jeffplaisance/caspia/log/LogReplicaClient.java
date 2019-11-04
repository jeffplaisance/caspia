package com.jeffplaisance.caspia.log;

public interface LogReplicaClient {

    LogReplicaResponse read(long index) throws Exception;

    default boolean writeAtomic(
            long id,
            int proposal,
            int accepted,
            byte[] value,
            boolean expect_absent,
            int expect_proposal,
            int expect_accepted
    ) throws Exception {
        if (expect_absent) {
            return putIfAbsent(id, proposal, accepted, value);
        } else {
            return compareAndSet(id, proposal, accepted, value, expect_proposal, expect_accepted);
        }
    }

    default boolean compareAndSet(
            long id,
            int proposal,
            int accepted,
            byte[] value,
            int expect_proposal,
            int expect_accepted
    ) throws Exception {
        return writeAtomic(id, proposal, accepted, value, false, expect_proposal, expect_accepted);
    }

    default boolean putIfAbsent(
            long id,
            int proposal,
            int accepted,
            byte[] value
    ) throws Exception {
        return writeAtomic(id, proposal, accepted, value, true, 0, 0);
    }

    long readLastIndex() throws Exception;
}
