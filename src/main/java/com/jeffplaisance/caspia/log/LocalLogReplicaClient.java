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

package com.jeffplaisance.caspia.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class LocalLogReplicaClient implements LogReplicaClient {

    private final List<List<LogReplicaState>> data = new ArrayList<>();
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
    public LogReplicaState read(long index) throws Exception {
        doNemesis();
        final List<LogReplicaState> list;
        synchronized (data) {
            if (index >= data.size()) {
                return LogReplicaState.EMPTY;
            }
            list = data.get((int) index);
            if (list == null) {
                return LogReplicaState.EMPTY;
            }
        }
        synchronized (list) {
            return list.get(list.size()-1);
        }
    }

    @Override
    public boolean compareAndSet(long id, LogReplicaState update, LogReplicaState expect) throws Exception {
        doNemesis();
        final List<LogReplicaState> list;
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
            LogReplicaState current = list.get(list.size()-1);
            if (current.getAccepted() == expect.getAccepted() && current.getProposal() == expect.getProposal()) {
                list.add(update);
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean putIfAbsent(long id, LogReplicaState update) throws Exception {
        doNemesis();
        synchronized (data) {
            List<LogReplicaState> list = new ArrayList<>();
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
