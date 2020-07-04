package com.jeffplaisance.caspia.register;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.jeffplaisance.caspia.common.Base;
import com.jeffplaisance.caspia.common.Quorum;
import com.jeffplaisance.caspia.common.ThrowingFunction;
import com.jeffplaisance.caspia.common.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class RegisterClient<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RegisterClient.class);
    private static final Comparator<RegisterReplicaResponse> MAX_ACCEPTED = Ordering.from((RegisterReplicaResponse a, RegisterReplicaResponse b) -> Longs.compare(a.getAccepted(), b.getAccepted())).nullsFirst();

    private List<RegisterReplicaClient> replicas;
    private final Function<Long, ? extends RegisterReplicaClient> replicaLoader;
    private int n;
    private int f;

    private final Transcoder<T> transcoder;
    private final Object id;

    private boolean fastPath = false;
    private long fastPathProposal = 0;
    private T fastPathPreviousValue = null;

    public RegisterClient(List<Long> replicas, Function<Long, ? extends RegisterReplicaClient> replicaLoader, Transcoder<T> transcoder, Object id) {

        this.transcoder = transcoder;
        this.id = id;
        this.replicas = replicas.stream().map(replicaLoader).collect(Collectors.toList());
        this.replicaLoader = replicaLoader;
        n = replicas.size();
        f = Base.lessThanHalf(n);
    }

    private void enableFastPath(long fastPathProposal, T fastPathPreviousValue, ReplicaUpdate replicaUpdate) {
        fastPath = true;
        this.fastPathProposal = fastPathProposal;
        this.fastPathPreviousValue = fastPathPreviousValue;
        if (replicaUpdate.getType() != ReplicaUpdate.UNMODIFIED) {
            if (replicaUpdate.getType() == ReplicaUpdate.REPLICA_REMOVED) {
                replicas = replicas.stream().filter(x -> x.getReplicaId() != replicaUpdate.getChangedReplica()).collect(Collectors.toList());
            } else if (replicaUpdate.getType() == ReplicaUpdate.REPLICA_ADDED) {
                if (replicas.stream().noneMatch(x -> x.getReplicaId() == replicaUpdate.getChangedReplica())) {
                    replicas.add(replicaLoader.apply(replicaUpdate.getChangedReplica()));
                }
            }
            n = replicas.size();
            f = Base.lessThanHalf(n);
        }
    }

    public @Nullable T write(Function<T, T> update) throws Exception {
        return write(update, x-> ReplicaUpdate.unmodified()).getValue();
    }

    public ReplicaUpdate modifyQuorum(Function<List<Long>, ReplicaUpdate> update) throws Exception {
        return write(x -> x, update).getReplicaUpdate();
    }

    public @Nullable T read() throws Exception {
        return write(x -> x);
    }

    private ValueAndReplicaUpdate<T> write(Function<T, T> updateValue, Function<List<Long>, ReplicaUpdate> updateReplicas) throws Exception {
        if (fastPath) {
            try {
                final T next = updateValue.apply(fastPathPreviousValue);
                final List<Long> replicaIds = replicas.stream().map(RegisterReplicaClient::getReplicaId).collect(Collectors.toList());
                final ReplicaUpdate replicaUpdate = updateReplicas.apply(replicaIds);
                final byte[] value = next == null ? null : transcoder.toBytes(next);
                final List<Boolean> responses = Quorum.broadcast(replicas, n - f, replica -> replica.writeAtomic(
                        id,
                        fastPathProposal + 1,
                        fastPathProposal,
                        value,
                        Longs.toArray(replicaIds),
                        replicaUpdate.getType(),
                        replicaUpdate.getChangedReplica(),
                        false,
                        fastPathProposal,
                        fastPathProposal - 1),
                        false);
                if (Base.sum(responses) >= n - f) {
                    enableFastPath(fastPathProposal + 1, next, replicaUpdate);
                    return new ValueAndReplicaUpdate<>(next, replicaUpdate);
                } else {
                    throw new Exception();
                }
            } catch (Throwable t) {
                fastPath = false;
                fastPathProposal = 0;
                fastPathPreviousValue = null;
                Throwables.propagateIfInstanceOf(t, Exception.class);
                throw Throwables.propagate(t);
            }
        } else {
            final List<RegisterReplicaResponse> initialValues = readInitial();
            return write2(updateValue, updateReplicas, initialValues);
        }
    }

    private List<RegisterReplicaResponse> readInitial() throws Exception {
        while (true) {
            final List<RegisterReplicaResponse> initialValues = Quorum.broadcast(replicas, n - f, replica -> replica.read(id), RegisterReplicaResponse.EMPTY);
            final RegisterReplicaResponse maxInitial = initialValues.stream().max(MAX_ACCEPTED).orElse(RegisterReplicaResponse.EMPTY);
            if (maxInitial.getAccepted() > 0) {
                final List<Long> maxAcceptedQuorum = Longs.asList(maxInitial.getReplicas());
                if (new HashSet<>(maxAcceptedQuorum).equals(replicas.stream().map(RegisterReplicaClient::getReplicaId).collect(Collectors.toSet()))) {
                    return initialValues;
                } else {
                    replicas = maxAcceptedQuorum.stream().map(replicaLoader).collect(Collectors.toList());
                }
            } else {
                return initialValues;
            }
        }
    }

    private ValueAndReplicaUpdate<T> write2(Function<T, T> update, Function<List<Long>, ReplicaUpdate> updateReplicas, List<RegisterReplicaResponse> initialValues) throws Exception {
        final long newProposal = initialValues.stream().map(RegisterReplicaResponse::getProposal).reduce(1L, Math::max)+1;
        final List<Boolean> proposeResponses = doPropose(initialValues, newProposal);
        return doAccept(update, updateReplicas, initialValues, newProposal, proposeResponses);
    }

    private List<Boolean> doPropose(List<RegisterReplicaResponse> initialValues, long newProposal) throws Exception {
        final List<ThrowingFunction<RegisterReplicaClient, Boolean, Exception>> proposeFunctions = initialValues.stream()
                .<ThrowingFunction<RegisterReplicaClient, Boolean, Exception>>map(
                        response -> (replica -> replica.writeAtomic(
                                id,
                                newProposal,
                                response.getAccepted(),
                                response.getValue(),
                                response.getReplicas() != null ? response.getReplicas() : new long[0],
                                response.getQuorumModified(),
                                response.getChangedReplica(),
                                response.getProposal() == 0,
                                response.getProposal(),
                                response.getAccepted())))
                .collect(Collectors.toList());
        final List<Boolean> proposeResponses = Quorum.broadcast(replicas, n-f, proposeFunctions, false);
        if (Base.sum(proposeResponses) < n-f) {
            throw new Exception();
        }
        return proposeResponses;
    }

    private ValueAndReplicaUpdate<T> doAccept(Function<T, T> update, Function<List<Long>, ReplicaUpdate> updateReplicas, List<RegisterReplicaResponse> initialValues, long newProposal, List<Boolean> proposeResponses) throws Exception {
        final RegisterReplicaResponse maxInitial = IntStream.range(0, initialValues.size())
                .filter(proposeResponses::get)
                .mapToObj(initialValues::get)
                .max(MAX_ACCEPTED)
                .orElse(RegisterReplicaResponse.EMPTY);
        final byte[] maxValue = maxInitial.getValue();
        final T next = update.apply(maxValue == null ? null : transcoder.fromBytes(maxValue));
        final byte[] nextValue = next == null ? null : transcoder.toBytes(next);
        final ReplicaUpdate replicaUpdate;
        final List<Long> replicaIds = replicas.stream().map(RegisterReplicaClient::getReplicaId).collect(Collectors.toList());
        if (maxInitial.getQuorumModified() == ReplicaUpdate.UNMODIFIED) {
            replicaUpdate = updateReplicas.apply(replicaIds);
        } else if (maxInitial.getQuorumModified() == ReplicaUpdate.REPLICA_REMOVED) {
            replicaUpdate = ReplicaUpdate.remove(maxInitial.getChangedReplica());
        } else if (maxInitial.getQuorumModified() == ReplicaUpdate.REPLICA_ADDED) {
            replicaUpdate = ReplicaUpdate.add(maxInitial.getChangedReplica());
        } else {
            throw new IllegalStateException();
        }
        final long nextFastPathProposal = newProposal + 1;
        final List<ThrowingFunction<RegisterReplicaClient, Boolean, Exception>> acceptFunctions = initialValues.stream()
                .<ThrowingFunction<RegisterReplicaClient, Boolean, Exception>>map(
                        response -> (replica -> replica.writeAtomic(
                                id,
                                nextFastPathProposal,
                                newProposal,
                                nextValue,
                                Longs.toArray(replicaIds),
                                replicaUpdate.getType(),
                                replicaUpdate.getChangedReplica(),
                                false,
                                newProposal,
                                response.getAccepted())))
                .collect(Collectors.toList());
        List<Boolean> acceptResponses = Quorum.broadcast(replicas, n-f, proposeResponses, acceptFunctions, Boolean.FALSE);
        if (Base.sum(acceptResponses) < n-f) {
            throw new Exception();
        }
        enableFastPath(nextFastPathProposal, next, replicaUpdate);
        return new ValueAndReplicaUpdate<>(next, replicaUpdate);
    }

    @Nullable
    public T readUnsafe() throws Exception {
        final List<RegisterReplicaResponse> responses = readInitial();
        final RegisterReplicaResponse maxResponse = responses.stream().max(MAX_ACCEPTED).orElse(RegisterReplicaResponse.EMPTY);
        if (maxResponse.getAccepted() == 0) return null;
        final long maxAcceptedCount = responses.stream().filter(r -> r.getAccepted() == maxResponse.getAccepted()).count();
        if (maxAcceptedCount >= n-f) {
            final byte[] maxValueBytes = maxResponse.getValue();
            return maxValueBytes == null ? null : transcoder.fromBytes(maxValueBytes);
        }
        return write2(x -> x, x -> ReplicaUpdate.unmodified(), responses).getValue();
    }

    private static final class ValueAndReplicaUpdate<T> {
        private final T value;
        private final ReplicaUpdate replicaUpdate;

        private ValueAndReplicaUpdate(T value, ReplicaUpdate replicaUpdate) {
            this.value = value;
            this.replicaUpdate = replicaUpdate;
        }

        public T getValue() {
            return value;
        }

        private ReplicaUpdate getReplicaUpdate() {
            return replicaUpdate;
        }
    }
}
