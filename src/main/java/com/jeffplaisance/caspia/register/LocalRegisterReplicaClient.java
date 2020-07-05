package com.jeffplaisance.caspia.register;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LocalRegisterReplicaClient implements RegisterReplicaClient {

    private final ConcurrentMap<Object, RegisterReplicaState> state = new ConcurrentHashMap<>();
    private final long replicaId;
    private final double failureProbability;
    private final double delayProbability;
    private final int delayNs;
    private final Random random = new Random();

    public LocalRegisterReplicaClient(long replicaId) {
        this(replicaId, 0, 0, 0);
    }

    public LocalRegisterReplicaClient(long replicaId, double failureProbability, double delayProbability, int delayNs) {
        this.replicaId = replicaId;
        this.failureProbability = failureProbability;
        this.delayProbability = delayProbability;
        this.delayNs = delayNs;
    }

    private void doNemesis() throws Exception {
        if (random.nextDouble() < delayProbability) {
            int delay = random.nextInt(delayNs) + 1;
            if (delay > 0) Thread.sleep(delay / 1000000, delay % 1000000);
        }
        if (random.nextDouble() < failureProbability) throw new Exception();
    }

    @Override
    public RegisterReplicaState read(Object index) throws Exception {
        doNemesis();
        return state.getOrDefault(index, RegisterReplicaState.EMPTY);
    }

    @Override
    public boolean compareAndSet(Object id, long proposal, long accepted, byte[] value, long[] replicas, byte quorumModified, long changedReplica, long expect_proposal, long expect_accepted) throws Exception {
        doNemesis();
        final RegisterReplicaState update = new RegisterReplicaState(proposal, accepted, value, replicas, quorumModified, changedReplica);
        final RegisterReplicaState current = state.computeIfPresent(id, (k, v) -> v.getAccepted() == expect_accepted && v.getProposal() == expect_proposal ? update : v);
        return update == current;
    }

    @Override
    public boolean putIfAbsent(Object id, long proposal, long accepted, byte[] value, long[] replicas, byte quorumModified, long changedReplica) throws Exception {
        doNemesis();
        final RegisterReplicaState update = new RegisterReplicaState(proposal, accepted, value, replicas, quorumModified, changedReplica);
        return null == state.putIfAbsent(id, update);
    }

    @Override
    public long getReplicaId() {
        return replicaId;
    }

    @Override
    public void close() {}
}
