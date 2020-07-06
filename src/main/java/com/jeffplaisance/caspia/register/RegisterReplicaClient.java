package com.jeffplaisance.caspia.register;

import java.io.Closeable;

public interface RegisterReplicaClient extends Closeable {

    RegisterReplicaState read(Object index) throws Exception;

    default boolean writeAtomic(
            Object id,
            RegisterReplicaState update,
            boolean expect_absent,
            RegisterReplicaState expect
    ) throws Exception {
        if (expect_absent) {
            return putIfAbsent(id, update);
        } else {
            return compareAndSet(id, update, expect);
        }
    }

    default boolean compareAndSet(
            Object id,
            RegisterReplicaState update,
            RegisterReplicaState expect
    ) throws Exception {
        return writeAtomic(id, update, false, expect);
    }

    default boolean putIfAbsent(
            Object id,
            RegisterReplicaState update
    ) throws Exception {
        return writeAtomic(id, update, true, RegisterReplicaState.EMPTY);
    }

    long getReplicaId();
}
