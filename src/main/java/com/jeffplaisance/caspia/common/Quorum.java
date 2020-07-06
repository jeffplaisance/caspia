package com.jeffplaisance.caspia.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceArray;

public final class Quorum {

    private static final ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("broadcast-thread-%d").setDaemon(false).build());

    public static <A, R, E extends Exception> List<R> broadcast(List<A> replicas, int minSuccessful, final ThrowingFunction<A, R, E> function, R failureResponse) throws Exception {
        return broadcast(replicas, minSuccessful, Collections.nCopies(replicas.size(), function), failureResponse);
    }

    public static <A, R, E extends Exception> List<R> broadcast(List<A> replicas, int minSuccessful, final List<ThrowingFunction<A, R, E>> functions, R failureResponse) throws Exception {
        return broadcast(replicas, minSuccessful, Collections.nCopies(replicas.size(), Boolean.TRUE), functions, failureResponse);
    }

    public static <A, R, E extends Exception> List<R> broadcast(List<A> replicas, int minSuccessful, final List<Boolean> recipients, final List<ThrowingFunction<A, R, E>> functions, R failureResponse) throws Exception {
        final AtomicReferenceArray<R> results = new AtomicReferenceArray<>(replicas.size());
        final ExecutorCompletionService<R> completionService = new ExecutorCompletionService<>(threadPool);
        final List<Future<R>> futures = new ArrayList<>();
        try {
            final int numRecipients = Collections.frequency(recipients, Boolean.TRUE);
            for (int i = 0; i < replicas.size(); i++) {
                if (recipients.get(i)) {
                    final int replicaIndex = i;
                    futures.add(completionService.submit(() -> {
                        R result = functions.get(replicaIndex).apply(replicas.get(replicaIndex));
                        results.set(replicaIndex, result);
                        return result;
                    }));
                }
            }
            int successes = 0;
            Throwable firstError = null;
            for (int i = 0; i < numRecipients; i++) {
                final Future<R> future = completionService.take();
                try {
                    future.get();
                    successes++;
                    if (successes >= minSuccessful) {
                        break;
                    }
                } catch (ExecutionException e) {
                    if (firstError == null) firstError = e.getCause();
                }
            }
            if (successes < minSuccessful) throw firstError != null ? new Exception(firstError) : new Exception("this shouldn't happen");
            final List<R> ret = new ArrayList<>();
            for (int i = 0; i < results.length(); i++) {
                final R result = results.get(i);
                ret.add(result == null ? failureResponse : result);
            }
            return ret;
        } finally {
            for (Future<R> future : futures) {
                future.cancel(true);
            }
        }
    }
}
