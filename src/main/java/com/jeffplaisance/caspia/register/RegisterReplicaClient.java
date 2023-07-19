/*
Copyright 2023 Jeff Plaisance

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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
