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

public final class ReplicaUpdate {
    public static final byte UNMODIFIED = 0;
    public static final byte REPLICA_REMOVED = 1;
    public static final byte REPLICA_ADDED = 2;

    private static final ReplicaUpdate UNMODIFIED_UPDATE = new ReplicaUpdate(UNMODIFIED, 0);

    public static ReplicaUpdate add(long changedReplica) {
        return new ReplicaUpdate(REPLICA_ADDED, changedReplica);
    }

    public static ReplicaUpdate remove(long changedReplica) {
        return new ReplicaUpdate(REPLICA_REMOVED, changedReplica);
    }

    public static ReplicaUpdate unmodified() {
        return UNMODIFIED_UPDATE;
    }

    private final byte type;
    private final long changedReplica;

    private ReplicaUpdate(byte type, long changedReplica) {
        this.type = type;
        this.changedReplica = changedReplica;
    }

    public byte getType() {
        return type;
    }

    public long getChangedReplica() {
        return changedReplica;
    }
}
