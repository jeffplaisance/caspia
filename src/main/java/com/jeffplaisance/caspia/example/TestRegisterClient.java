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

package com.jeffplaisance.caspia.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jeffplaisance.caspia.common.Transcoder;
import com.jeffplaisance.caspia.register.LocalRegisterReplicaClient;
import com.jeffplaisance.caspia.register.RegisterClient;
import com.jeffplaisance.caspia.register.RegisterReplicaClient;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class TestRegisterClient {

    private static final ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("client-thread-%d").setDaemon(false).build());

    public static void main(String[] args) throws Exception {
        for (int k = 0; k < 1000; k++) {
            final List<RegisterClient<int[]>> clients = new ArrayList<>();
            final List<Long> replicas = Arrays.asList(1L, 2L, 3L);
            final ConcurrentMap<Long, RegisterReplicaClient> replicaClients = new ConcurrentHashMap<>();
            final String id = "jeff";
            final int numClients = 2;
            final double failureProbability = 0.2;
            final double delayProbability = 0.01;
            final int delayNs = 10000;
            final int iterations = 10000;
            final ExecutorCompletionService ecs = new ExecutorCompletionService(threadPool);
            for (int i = 0; i < numClients; i++) {
                final RegisterClient<int[]> client = new RegisterClient<>(replicas, replica -> replicaClients.computeIfAbsent(replica, replicaId -> new LocalRegisterReplicaClient(replicaId, failureProbability, delayProbability, delayNs)), new IntArrayTranscoder(), id);
                clients.add(client);
                final int clientIndex = i;
                ecs.submit(() -> {
                    int[] previous = null;
                    int success = 0;
                    for (int i1 = 0; i1 < iterations; i1++) {
                        final int[] update;
                        try {
                            final int finalI = i1;
                            final int[] finalPrevious = previous;
                            update = client.write(ints -> {
                                if (ints == null) {
                                    if (finalPrevious != null) {
                                        System.err.println("ruh roh 1!");
                                    }
                                    final int[] ret = new int[1];
                                    ret[0] = iterations * clientIndex + finalI;
                                    return ret;
                                }
                                if (finalPrevious != null) {
                                    for (int j = 0; j < finalPrevious.length; j++) {
                                        if (finalPrevious[j] != ints[j]) {
                                            System.err.println("ruh roh 2!");
                                            System.out.println("finalPrevious = " + Arrays.toString(finalPrevious));
                                            System.out.println("ints = " + Arrays.toString(ints));
                                        }
                                    }
                                }
                                final int[] ret = Arrays.copyOf(ints, ints.length + 1);
                                ret[ret.length - 1] = iterations * clientIndex + finalI;
                                return ret;
                            });
                        } catch (Exception e) {
                            continue;
                        }
                        if (previous != null) {
                            for (int j = 0; j < previous.length; j++) {
                                if (update[j] != previous[j]) {
                                    System.err.println("ruh roh 3!");
                                    System.out.println("previous = " + Arrays.toString(previous));
                                    System.out.println("update = " + Arrays.toString(update));
                                }
                            }
                        }
                        if (update[update.length - 1] != iterations * clientIndex + i1) {
                            System.err.println("ruh roh 4!");
                        }
                        success++;
                        previous = update;
                    }
                    System.out.printf("clientIndex = %d, success = %d\r\n", clientIndex, success);
                }, null);
            }
            for (int i = 0; i < numClients; i++) {
                ecs.take();
            }
            while (true) {
                try {
                    final int[] vals = clients.get(0).read();
//                    System.out.println(Arrays.toString(vals));
                    System.out.println(vals.length);
                    final int[] threadMax = new int[numClients];
                    Arrays.fill(threadMax, -1);
                    for (int i : vals) {
                        final int client = i / iterations;
                        final int iteration = i % iterations;
                        if (iteration <= threadMax[client]) {
                            System.err.println("ruh roh 5!");
                        }
                        threadMax[client] = iteration;
                    }
                    break;
                } catch (Exception e) {
                    //ignore
                }
            }
        }
        System.exit(0);
    }

    private static final class IntArrayTranscoder implements Transcoder<int[]> {

        @Override
        public byte[] toBytes(int[] ints) {
            final byte[] out = new byte[ints.length*4];
            final ByteBuffer buffer = ByteBuffer.wrap(out);
            buffer.order(ByteOrder.nativeOrder());
            for (int i : ints) {
                buffer.putInt(i);
            }
            return out;
        }

        @Override
        public int[] fromBytes(byte[] bytes) {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            buffer.order(ByteOrder.nativeOrder());
            final int[] ret = new int[bytes.length/4];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = buffer.getInt();
            }
            return ret;
        }
    }
}
