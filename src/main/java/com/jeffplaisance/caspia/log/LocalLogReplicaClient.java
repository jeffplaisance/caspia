package com.jeffplaisance.caspia.log;

import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class LocalLogReplicaClient implements LogReplicaClient {

    private final ConcurrentNavigableMap<Long, LogReplicaResponse> data = new ConcurrentSkipListMap<>();
    private final double failureProbability;
    private final double delayProbability;
    private final int delayNs;
    private final Random random = new Random();

    public LocalLogReplicaClient(double failureProbability, double delayProbability, int delayNs) {
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
    public LogReplicaResponse read(long index) throws Exception {
        doNemesis();
        return data.getOrDefault(index, LogReplicaResponse.EMPTY);
    }

    @Override
    public boolean compareAndSet(long id, int proposal, int accepted, byte[] value, int expect_proposal, int expect_accepted) throws Exception {
        doNemesis();
        final LogReplicaResponse update = new LogReplicaResponse(proposal, accepted, value);
        final LogReplicaResponse current = data.computeIfPresent(id, (k, v) -> v.getAccepted() == expect_accepted && v.getProposal() == expect_proposal ? update : v);
        return update == current;
    }

    @Override
    public boolean putIfAbsent(long id, int proposal, int accepted, byte[] value) throws Exception {
        doNemesis();
        final LogReplicaResponse update = new LogReplicaResponse(proposal, accepted, value);
        return null == data.putIfAbsent(id, update);
    }

    @Override
    public long readLastIndex() throws Exception {
        doNemesis();
        try {
            return data.lastKey();
        } catch (NoSuchElementException e) {
            return 1;
        }
    }
}
