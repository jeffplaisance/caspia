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

public final class RegisterClient<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RegisterClient.class);
    private static final Comparator<RegisterReplicaState> MAX_ACCEPTED = Ordering.from((RegisterReplicaState a, RegisterReplicaState b) -> Longs.compare(a.getAccepted(), b.getAccepted())).nullsFirst();

    private List<RegisterReplicaClient> replicas;
    private final Function<Long, ? extends RegisterReplicaClient> replicaLoader;
    private int n;
    private int f;

    private final Transcoder<T> transcoder;
    private final Object id;

    private boolean fastPath = false;
    private RegisterReplicaState fastPathPreviousState;

    public RegisterClient(List<Long> replicas, Function<Long, ? extends RegisterReplicaClient> replicaLoader, Transcoder<T> transcoder, Object id) {

        this.transcoder = transcoder;
        this.id = id;
        this.replicas = replicas.stream().map(replicaLoader).collect(Collectors.toList());
        this.replicaLoader = replicaLoader;
        n = replicas.size();
        f = Base.lessThanHalf(n);
    }

    private void enableFastPath(RegisterReplicaState nextState) {
        fastPath = true;
        this.fastPathPreviousState = nextState;
        if (nextState.getQuorumModified() != ReplicaUpdate.UNMODIFIED) {
            if (nextState.getQuorumModified() == ReplicaUpdate.REPLICA_REMOVED) {
                replicas = replicas.stream().filter(x -> x.getReplicaId() != nextState.getChangedReplica()).collect(Collectors.toList());
            } else if (nextState.getQuorumModified() == ReplicaUpdate.REPLICA_ADDED) {
                if (replicas.stream().noneMatch(x -> x.getReplicaId() == nextState.getChangedReplica())) {
                    replicas.add(replicaLoader.apply(nextState.getChangedReplica()));
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
                final byte[] previousValue = fastPathPreviousState.getValue();
                final T next = updateValue.apply(previousValue == null ? null : transcoder.fromBytes(previousValue));
                final List<Long> replicaIds = replicas.stream().map(RegisterReplicaClient::getReplicaId).collect(Collectors.toList());
                final ReplicaUpdate replicaUpdate = updateReplicas.apply(replicaIds);
                final byte[] value = next == null ? null : transcoder.toBytes(next);
                final RegisterReplicaState nextState = new RegisterReplicaState(
                        fastPathPreviousState.getProposal()+1,
                        fastPathPreviousState.getProposal(),
                        value,
                        Longs.toArray(replicaIds),
                        replicaUpdate.getType(),
                        replicaUpdate.getChangedReplica());
                final List<Boolean> responses = Quorum.broadcast(replicas, n - f, replica -> replica.writeAtomic(
                        id,
                        nextState,
                        false,
                        fastPathPreviousState),
                        false);
                if (Base.sum(responses) >= n - f) {
                    enableFastPath(nextState);
                    return new ValueAndReplicaUpdate<>(next, replicaUpdate);
                } else {
                    throw new Exception();
                }
            } catch (Throwable t) {
                fastPath = false;
                fastPathPreviousState = null;
                Throwables.propagateIfInstanceOf(t, Exception.class);
                throw Throwables.propagate(t);
            }
        } else {
            final List<RegisterReplicaState> initialValues = readInitial();
            return write2(updateValue, updateReplicas, initialValues);
        }
    }

    private List<RegisterReplicaState> readInitial() throws Exception {
        while (true) {
            final List<RegisterReplicaState> initialValues = Quorum.broadcast(replicas, n - f, replica -> replica.read(id), RegisterReplicaState.EMPTY);
            final RegisterReplicaState maxInitial = initialValues.stream().max(MAX_ACCEPTED).orElse(RegisterReplicaState.EMPTY);
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

    private ValueAndReplicaUpdate<T> write2(Function<T, T> update, Function<List<Long>, ReplicaUpdate> updateReplicas, List<RegisterReplicaState> initialValues) throws Exception {
        final long newProposal = initialValues.stream().map(RegisterReplicaState::getProposal).reduce(1L, Math::max)+1;
        final List<Optional<RegisterReplicaState>> proposeResponses = doPropose(initialValues, newProposal);
        return doAccept(update, updateReplicas, newProposal, proposeResponses);
    }

    private List<Optional<RegisterReplicaState>> doPropose(List<RegisterReplicaState> initialValues, long newProposal) throws Exception {
        final List<ThrowingFunction<RegisterReplicaClient, Optional<RegisterReplicaState>, Exception>> proposeFunctions = initialValues.stream()
                .<ThrowingFunction<RegisterReplicaClient, Optional<RegisterReplicaState>, Exception>>map(
                        state -> (replica -> {
                            final RegisterReplicaState nextState = new RegisterReplicaState(
                                    newProposal,
                                    state.getAccepted(),
                                    state.getValue(),
                                    state.getReplicas() != null ? state.getReplicas() : new long[0],
                                    state.getQuorumModified(),
                                    state.getChangedReplica());
                            if (replica.writeAtomic(id, nextState,state.getProposal() == 0, state)) {
                                return Optional.of(nextState);
                            }
                            return Optional.empty();
                        }))
                .collect(Collectors.toList());
        final List<Optional<RegisterReplicaState>> proposeResponses = Quorum.broadcast(replicas, n-f, proposeFunctions, Optional.empty());
        if (proposeResponses.stream().filter(Optional::isPresent).count() < n-f) {
            throw new Exception();
        }
        return proposeResponses;
    }

    private ValueAndReplicaUpdate<T> doAccept(Function<T, T> update, Function<List<Long>, ReplicaUpdate> updateReplicas, long newProposal, List<Optional<RegisterReplicaState>> proposeResponses) throws Exception {
        final RegisterReplicaState maxInitial = proposeResponses.stream()
                .flatMap(Base::toStream)
                .max(MAX_ACCEPTED)
                .orElse(RegisterReplicaState.EMPTY);
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
        final RegisterReplicaState nextState = new RegisterReplicaState(
                nextFastPathProposal,
                newProposal,
                nextValue,
                Longs.toArray(replicaIds),
                replicaUpdate.getType(),
                replicaUpdate.getChangedReplica()
        );
        final List<ThrowingFunction<RegisterReplicaClient, Boolean, Exception>> acceptFunctions = proposeResponses.stream()
                .<ThrowingFunction<RegisterReplicaClient, Boolean, Exception>>map(state -> {
                    if (state.isPresent()) {
                        return replica -> replica.writeAtomic(id, nextState, false, state.get());
                    }
                    return replica -> false;
                })
                .collect(Collectors.toList());
        List<Boolean> acceptResponses = Quorum.broadcast(replicas, n-f, acceptFunctions, Boolean.FALSE);
        if (Base.sum(acceptResponses) < n-f) {
            throw new Exception();
        }
        enableFastPath(nextState);
        return new ValueAndReplicaUpdate<>(next, replicaUpdate);
    }

    @Nullable
    public T readUnsafe() throws Exception {
        final List<RegisterReplicaState> responses = readInitial();
        final RegisterReplicaState maxResponse = responses.stream().max(MAX_ACCEPTED).orElse(RegisterReplicaState.EMPTY);
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
