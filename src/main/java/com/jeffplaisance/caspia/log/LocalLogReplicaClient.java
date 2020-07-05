package com.jeffplaisance.caspia.log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class LocalLogReplicaClient implements LogReplicaClient {

    private final List<List<LogReplicaResponse>> data = new ArrayList<>();
    private final double failureProbability;
    private final double delayProbability;
    private final int delayNs;
    private final Random random = new Random();

    public LocalLogReplicaClient(double failureProbability, double delayProbability, int delayNs) {
        this.failureProbability = failureProbability;
        this.delayProbability = delayProbability;
        this.delayNs = delayNs;
        data.add(null);
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
        final List<LogReplicaResponse> list;
        synchronized (data) {
            if (index >= data.size()) {
                return LogReplicaResponse.EMPTY;
            }
            list = data.get((int) index);
            if (list == null) {
                return LogReplicaResponse.EMPTY;
            }
        }
        synchronized (list) {
            return list.get(list.size()-1);
        }
    }

    @Override
    public boolean compareAndSet(long id, int proposal, int accepted, byte[] value, int expect_proposal, int expect_accepted) throws Exception {
        doNemesis();
        final List<LogReplicaResponse> list;
        synchronized (data) {
            if (id >= data.size()) {
                return false;
            }
            list = data.get((int) id);
            if (list == null) {
                return false;
            }
        }
        synchronized (list) {
            LogReplicaResponse current = list.get(list.size()-1);
            if (current.getAccepted() == expect_accepted && current.getProposal() == expect_proposal) {
                final LogReplicaResponse update = new LogReplicaResponse(proposal, accepted, value == null ? null : Arrays.copyOf(value, value.length));
                list.add(update);
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean putIfAbsent(long id, int proposal, int accepted, byte[] value) throws Exception {
        doNemesis();
        synchronized (data) {
            final LogReplicaResponse update = new LogReplicaResponse(proposal, accepted, value == null ? null : Arrays.copyOf(value, value.length));
            List<LogReplicaResponse> list = new ArrayList<>();
            list.add(update);
            if (id < data.size()) {
                if (data.get((int) id) == null) {
                    data.set((int) id, list);
                    return true;
                }
                return false;
            }
            while (id > data.size()) {
                data.add(null);
            }
            if (id == data.size()) {
                data.add(list);
                return true;
            } else {
                final Exception e = new Exception("ruh roh");
                e.printStackTrace(System.err);
                throw e;
            }
        }
    }

    @Override
    public long readLastIndex() throws Exception {
        doNemesis();
        synchronized (data) {
            return data.size()-1;
        }
    }
}
