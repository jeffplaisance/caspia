package com.jeffplaisance.caspia.log;

public final class LogReplicaState {

    public static final LogReplicaState EMPTY = new LogReplicaState(0, 0, null);

    private final int proposal;
    private final int accepted;
    private final byte[] value;

    public LogReplicaState(int proposal, int accepted, byte[] value) {
        this.proposal = proposal;
        this.accepted = accepted;
        this.value = value;
    }

    public int getProposal() {
        return proposal;
    }

    public int getAccepted() {
        return accepted;
    }

    public byte[] getValue() {
        return value;
    }
}
