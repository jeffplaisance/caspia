package com.jeffplaisance.caspia.log;

public final class LogReplicaResponse {

    public static final LogReplicaResponse FAILURE = new LogReplicaResponse(false, 0, 0, null);
    public static final LogReplicaResponse EMPTY = new LogReplicaResponse(true, 0, 0, null);

    private final boolean success;
    private final int proposal;
    private final int accepted;
    private final byte[] value;

    public LogReplicaResponse(boolean success, int proposal, int accepted, byte[] value) {
        this.success = success;
        this.proposal = proposal;
        this.accepted = accepted;
        this.value = value;
    }

    public boolean isSuccess() {
        return success;
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
