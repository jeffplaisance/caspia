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
