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

    // skip_propose must be false unless you previously called write with index-1 and it returned true
    // a return value of true means that value was committed at index
    // a return value of false means something went wrong. there are many possibilities but a few are that there is another value already committed at index or there is a conflicting client trying to write simultaneously
    // this method does not retry on failures or conflicts. retry logic should be handled by the caller.

    public boolean write(long index, byte[] value) throws Exception {
        Preconditions.checkNotNull(value);
        if (index == fastPathIndex) {
            final List<Boolean> responses = Quorum.broadcast(replicas, n-f, replica -> replica.writeAtomic(index, 1, 1, value, true, 0, 0), false);
            if (Base.sum(responses) >= n-f) {
                fastPathIndex = index+1;
                return true;
            }
        }
        final List<LogReplicaResponse> initialValues = Quorum.broadcast(replicas, n-f, replica -> replica.read(index), LogReplicaResponse.EMPTY);
        if (write2(index, value, initialValues) == value) {
            fastPathIndex = index+1;
            return true;
        }
        fastPathIndex = -1;
        return false;
    }

    @Nullable
    private byte[] write2(final long index, final byte[] value, final List<LogReplicaResponse> initialValues) throws Exception {

        final int newProposal = initialValues.stream().map(LogReplicaResponse::getProposal).reduce(1, Math::max)+1;
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
        if (Base.sum(proposeResponses) < n-f) return null;
        final LogReplicaResponse maxInitial = IntStream.range(0, initialValues.size())
                .filter(proposeResponses::get)
                .mapToObj(initialValues::get)
                .max(MAX_ACCEPTED)
                .orElse(LogReplicaResponse.EMPTY);
        final byte[] valueWritten = maxInitial.getAccepted() == 0 ? value : maxInitial.getValue();
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
        if (Base.sum(acceptResponses) < n-f) return null;
        return valueWritten;
    }

    @Nullable
    public byte[] read(long index) throws Exception {
        final List<LogReplicaResponse> responses = Quorum.broadcast(replicas, n-f, replica -> replica.read(index), LogReplicaResponse.EMPTY);
        final LogReplicaResponse maxResponse = responses.stream().max(MAX_ACCEPTED).orElse(LogReplicaResponse.EMPTY);
        if (maxResponse.getAccepted() == 0) return null;
        final long maxAcceptedCount = responses.stream().filter(r -> r.getAccepted() == maxResponse.getAccepted()).count();
        final byte[] maxValue = maxResponse.getValue();
        if (maxAcceptedCount >= n-f) return maxValue;
        final byte[] value = write2(index, maxValue, responses);
        if (value == null) throw new IOException();
        return value;
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
