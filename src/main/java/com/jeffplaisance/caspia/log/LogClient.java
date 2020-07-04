package com.jeffplaisance.caspia.log;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.jeffplaisance.caspia.common.Base;
import com.jeffplaisance.caspia.common.Quorum;
import com.jeffplaisance.caspia.common.ThrowingFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class LogClient {

    private static final Logger LOG = LoggerFactory.getLogger(LogClient.class);
    private static final Comparator<LogReplicaResponse> MAX_ACCEPTED = Ordering.from((LogReplicaResponse a, LogReplicaResponse b) -> Ints.compare(a.getAccepted(), b.getAccepted())).nullsFirst();

    private final List<LogReplicaClient> replicas;
    private final int n;
    private final int f;

    private long fastPathIndex = -1;

    public LogClient(List<? extends LogReplicaClient> replicas) {
        n = replicas.size();
        f = Base.lessThanHalf(n);
        this.replicas = new ArrayList<>(replicas);
    }

    // a return value of true means that value was committed at index
    // a return value of false means that a value was already committed at index (could possibly be equal to value but it was already there)
    // an IOException is thrown if less than a quorum of responses is obtained (can be triggered by a conflicting client)
    // this method does not retry on failures or conflicts. retry logic should be handled by the caller.

    public boolean write(long index, byte[] value) throws Exception {
        // null is used as a sentinel value to signify that no value has been written at this index yet
        // it is valid for there to be a null accepted value which is allowed to transition to a real value later
        // accepted non-null values can never be changed
        Preconditions.checkNotNull(value);

        // this is true iff this client was the one to commit a non-null value at index-1
        // fast path always uses proposal number 1
        // any competing proposer will start at proposal number 2 and block fast path
        if (index == fastPathIndex) {
            final List<Boolean> responses = Quorum.broadcast(replicas, n-f, replica -> replica.writeAtomic(index, 1, 1, value, true, 0, 0), false);
            if (Base.sum(responses) >= n-f) {
                fastPathIndex = index+1;
                return true;
            }
        }

        final List<LogReplicaResponse> initialValues = Quorum.broadcast(replicas, n-f, replica -> replica.read(index), LogReplicaResponse.EMPTY);
        // reference equality check is intentional
        if (write2(index, value, initialValues) == value) {
            fastPathIndex = index+1;
            return true;
        }
        fastPathIndex = -1;
        return false;
    }

    @Nullable
    private byte[] write2(final long index, final byte[] value, final List<LogReplicaResponse> initialValues) throws Exception {

        // lowest possible value for newProposal is 2 since 1 is reserved for fast path
        final int newProposal = initialValues.stream().map(LogReplicaResponse::getProposal).reduce(1, Math::max)+1;
        // attempt to increase proposal to newProposal on all replicas leaving all other fields the same
        final List<ThrowingFunction<LogReplicaClient, Boolean, Exception>> proposeFunctions = initialValues.stream()
                .<ThrowingFunction<LogReplicaClient, Boolean, Exception>>map(
                        response -> (replica -> replica.writeAtomic(
                                index,
                                newProposal,
                                response.getAccepted(),
                                response.getValue(),
                                response.getProposal() == 0,
                                response.getProposal(),
                                response.getAccepted())))
                .collect(Collectors.toList());
        final List<Boolean> proposeResponses = Quorum.broadcast(replicas, n-f, proposeFunctions, false);

        // check for success on a quorum of replicas, throw exception on failure
        if (Base.sum(proposeResponses) < n-f) throw new IOException();

        // find the response with the highest accepted number from the replicas where propose succeeded
        final LogReplicaResponse maxInitial = IntStream.range(0, initialValues.size())
                .filter(proposeResponses::get)
                .mapToObj(initialValues::get)
                .max(MAX_ACCEPTED)
                .orElse(LogReplicaResponse.EMPTY);

        // if a value has already been written at this index we need to make sure it is propagated
        // if no value has been written we can write our own value
        final byte[] valueWritten = maxInitial.getValue() == null ? value : maxInitial.getValue();

        // attempt to update accepted to newProposal and value to valueWritten on replicas where propose succeeded
        final List<ThrowingFunction<LogReplicaClient, Boolean, Exception>> acceptFunctions = initialValues.stream()
                .<ThrowingFunction<LogReplicaClient, Boolean, Exception>>map(
                        response -> (replica -> replica.writeAtomic(
                                index,
                                newProposal,
                                newProposal,
                                valueWritten,
                                false,
                                newProposal,
                                response.getAccepted())))
                .collect(Collectors.toList());
        List<Boolean> acceptResponses = Quorum.broadcast(replicas, n-f, proposeResponses, acceptFunctions, Boolean.FALSE);

        // check for success on a quorum of replicas, return valueWritten on success and throw exception on failure
        if (Base.sum(acceptResponses) < n-f) throw new IOException();
        return valueWritten;
    }

    @Nullable
    public byte[] read(long index) throws Exception {
        final List<LogReplicaResponse> responses = Quorum.broadcast(replicas, n-f, replica -> replica.read(index), LogReplicaResponse.EMPTY);
        final LogReplicaResponse maxAccepted = responses.stream().max(MAX_ACCEPTED).orElse(LogReplicaResponse.EMPTY);
        final byte[] maxValue = maxAccepted.getValue();
        if (maxAccepted.getAccepted() > 0 && maxValue != null) {
            final long maxAcceptedCount = responses.stream().filter(r -> r.getAccepted() == maxAccepted.getAccepted()).count();
            if (maxAcceptedCount >= n - f) {
                // if there is a non-null value committed in the same round at a quorum of replicas it cannot change
                // and we can return it safely.
                return maxValue;
            }
        }
        // we need to attempt to write the value with the highest accept round number to a quorum of replicas before we
        // can be certain it is committed.
        // if maxValue is null, in order to prevent linearizability issues we have to commit a null value before we can
        // be certain that this index has not yet been written.
        // this prevents partially committed values either through fast path or normal path from becoming fully
        // committed by future read operations.
        // this null value must be overwritten before writing to any higher index.
        return write2(index, maxValue, responses);
    }

    public long readLastIndex() throws Exception {
        final List<Long> maxValues = Quorum.broadcast(replicas, n-f, LogReplicaClient::readLastIndex, 0L);
        return maxValues.stream().reduce(0L, Math::max);
    }

    @Nullable
    public String readString(long index) throws Exception {
        final byte[] bytes = read(index);
        return bytes == null ? null : new String(bytes, Base.UTF_8);
    }

    public boolean writeString(long index, String value) throws Exception {
        return write(index, value.getBytes(Base.UTF_8));
    }
}
