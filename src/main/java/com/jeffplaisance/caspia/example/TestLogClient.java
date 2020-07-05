package com.jeffplaisance.caspia.example;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jeffplaisance.caspia.log.LocalLogReplicaClient;
import com.jeffplaisance.caspia.log.LogClient;
import com.jeffplaisance.caspia.log.LogReplicaClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class TestLogClient {
    private static final ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("client-thread-%d").setDaemon(false).build());

    public static void main(String[] args) throws Exception {
        for (int k = 0; k < 1000; k++) {
            final double failureProbability = 0.2;
            final double delayProbability = 0;
            final int delayNs = 10000;
            final List<LogClient> clients = new ArrayList<>();
            final List<LogReplicaClient> replicas = Arrays.asList(
                    new LocalLogReplicaClient(failureProbability, delayProbability, delayNs),
                    new LocalLogReplicaClient(failureProbability, delayProbability, delayNs),
                    new LocalLogReplicaClient(failureProbability, delayProbability, delayNs)
            );
            final int numClients = 2;
            final int iterations = 10000;
            final CyclicBarrier barrier = new CyclicBarrier(numClients+1);
            for (int i = 0; i < numClients; i++) {
                final LogClient client = new LogClient(replicas);
                clients.add(client);
                final int clientIndex = i;
                threadPool.submit(() -> {
                    final ArrayList<Integer> claimedIndexes = new ArrayList<>();
                    for (int i1 = 1; i1 <= iterations; i1++) {
                        while (true) {
                            try {
                                if (client.write(i1, Ints.toByteArray(clientIndex))) {
                                    claimedIndexes.add(i1);
                                } else {
                                    final int newIndex = (int) client.readLastIndex();
                                    //System.out.printf("client %d skipping from %d to %d\r\n", clientIndex, i1, newIndex);
                                    i1 = Math.max(i1, newIndex-1);
                                }
                                break;
                            } catch (Exception e) {
                                //ignore
                            }
                        }
                    }
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw Throwables.propagate(e);
                    }
                    for (int index : claimedIndexes) {
                        while (true) {
                            try {
                                if (Ints.fromByteArray(client.read(index)) != clientIndex) {
                                    System.err.println("ruh roh 1");
                                }
                                break;
                            } catch (Exception e) {
                                //e.printStackTrace(System.err);
                                //System.err.println("ruh roh 2");
                            }
                        }
                    }
                    System.out.printf("clientIndex = %d, success = %d\r\n", clientIndex, claimedIndexes.size());
                }, null);
            }
            final List<Integer> readerValues = new ArrayList<>();
            readerValues.add(null);
            final LogClient reader = new LogClient(replicas);
            for (int i = 1; i <= iterations; i++) {
                int loopCount = 0;
                while (true) {
                    try {
                        final byte[] bytes = reader.read(i);
                        if (bytes != null) {
                            final int j = Ints.fromByteArray(bytes);
                            if (j < 0 || j >= numClients) {
                                System.err.println("ruh roh 4");
                            }
                            readerValues.add(j);
                            break;
                        }
                    } catch (Exception e) {
                        if (loopCount > 10000) {
                            System.out.println(i);
                        }
                    }
                    loopCount++;
                }
            }
            barrier.await();
            for (int i = 1; i <= iterations; i++) {
                while (true) {
                    try {
                        final byte[] bytes = reader.read(i);
                        if (bytes == null) {
                            System.err.println("ruh roh 2");
                        } else if (Ints.fromByteArray(bytes) != readerValues.get(i)) {
                            System.err.println("ruh roh 3");
                        }
                        break;
                    } catch (Exception e) {
                        //ignore
                    }
                }
            }
        }
        System.exit(0);
    }
}
