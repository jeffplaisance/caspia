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

public final class RegisterReplicaState {
    public static final RegisterReplicaState EMPTY = new RegisterReplicaState(0, 0, null, null, ReplicaUpdate.UNMODIFIED, 0);
    private final long proposal;
    private final long accepted;
    private final byte[] value;
    private final long[] replicas;
    private final byte quorumModified;
    private final long changedReplica;

    public RegisterReplicaState(long proposal, long accepted, byte[] value, long[] replicas, byte quorumModified, long changedReplica) {
        this.proposal = proposal;
        this.accepted = accepted;
        this.value = value;
        this.replicas = replicas;
        this.quorumModified = quorumModified;
        this.changedReplica = changedReplica;
    }

    public long getProposal() {
        return proposal;
    }

    public long getAccepted() {
        return accepted;
    }

    public byte[] getValue() {
        return value;
    }

    public long[] getReplicas() {
        return replicas;
    }

    public byte getQuorumModified() {
        return quorumModified;
    }

    public long getChangedReplica() {
        return changedReplica;
    }
}
