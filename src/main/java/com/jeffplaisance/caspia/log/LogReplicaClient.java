package com.jeffplaisance.caspia.log;

public interface LogReplicaClient {

    LogReplicaState read(long index) throws Exception;

    default boolean writeAtomic(
            long id,
            int proposal,
            int accepted,
            byte[] value,
            boolean expect_absent,
            LogReplicaState expect
    ) throws Exception {
        if (expect_absent) {
            return putIfAbsent(id, proposal, accepted, value);
        } else {
            return compareAndSet(id, proposal, accepted, value, expect);
        }
    }

    default boolean compareAndSet(
            long id,
            int proposal,
            int accepted,
            byte[] value,
            LogReplicaState expect
    ) throws Exception {
        return writeAtomic(id, proposal, accepted, value, false, expect);
    }

    default boolean putIfAbsent(
            long id,
            int proposal,
            int accepted,
            byte[] value
    ) throws Exception {
        return writeAtomic(id, proposal, accepted, value, true, LogReplicaState.EMPTY);
    }

    long readLastIndex() throws Exception;
}
