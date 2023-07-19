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
    public boolean compareAndSet(Object id, RegisterReplicaState update, RegisterReplicaState expect) throws Exception {
        doNemesis();
        final RegisterReplicaState current = state.computeIfPresent(id, (k, v) -> v.getAccepted() == expect.getAccepted() && v.getProposal() == expect.getProposal() ? update : v);
        return update == current;
    }

    @Override
    public boolean putIfAbsent(Object id, RegisterReplicaState update) throws Exception {
        doNemesis();
        return null == state.putIfAbsent(id, update);
    }

    @Override
    public long getReplicaId() {
        return replicaId;
    }

    @Override
    public void close() {}
}
