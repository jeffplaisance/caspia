package com.jeffplaisance.caspia.log;

public final class LogReplicaResponse {

    public static final LogReplicaResponse EMPTY = new LogReplicaResponse(0, 0, null);

    private final int proposal;
    private final int accepted;
    private final byte[] value;

    public LogReplicaResponse(int proposal, int accepted, byte[] value) {
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
