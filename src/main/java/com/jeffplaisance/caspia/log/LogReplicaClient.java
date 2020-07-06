package com.jeffplaisance.caspia.log;

public interface LogReplicaClient {

    LogReplicaState read(long index) throws Exception;

    default boolean writeAtomic(
            long id,
            LogReplicaState update,
            boolean expect_absent,
            LogReplicaState expect
    ) throws Exception {
        if (expect_absent) {
            return putIfAbsent(id, update);
        } else {
            return compareAndSet(id, update, expect);
        }
    }

    default boolean compareAndSet(
            long id,
            LogReplicaState update,
            LogReplicaState expect
    ) throws Exception {
        return writeAtomic(id, update, false, expect);
    }

    default boolean putIfAbsent(
            long id,
            LogReplicaState update
    ) throws Exception {
        return writeAtomic(id, update, true, LogReplicaState.EMPTY);
    }

    long readLastIndex() throws Exception;
}
