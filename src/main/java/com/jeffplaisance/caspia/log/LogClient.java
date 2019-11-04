package com.jeffplaisance.caspia.log;

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

    public LogClient(List<? extends LogReplicaClient> replicas) {
        n = replicas.size();
        f = Base.lessThanHalf(n);
        this.replicas = new ArrayList<>(replicas);
    }

    public boolean write(long index, byte[] value) throws Exception {
        return write(index, value, false);
    }

    // skip_propose must be false unless you previously called write with index-1 and it returned true
    // a return value of true means that value was committed at index
    // a return value of false means something went wrong. there are many possibilities but a few are that there is another value already committed at index or there is a conflicting client trying to write simultaneously
    // this method does not retry on failures or conflicts. retry logic should be handled by the caller.

    public boolean write(long index, byte[] value, boolean skipPropose) throws Exception {
        if (skipPropose) {
            final List<Boolean> responses = Quorum.broadcast(replicas, n-f, replica -> replica.writeAtomic(index, 1, 1, value, true, 0, 0), false);
            if (Base.sum(responses) >= n-f) return true;
        }
        final List<LogReplicaResponse> initialValues = Quorum.broadcast(replicas, n-f, replica -> replica.read(index), LogReplicaResponse.FAILURE);
        return write2(index, value, 0, initialValues);
    }

    private boolean write2(long index, byte[] value, int priorAccepted, List<LogReplicaResponse> initialValues) throws Exception {

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
        if (Base.sum(proposeResponses) < n-f) return false;
        final LogReplicaResponse maxInitial = IntStream.range(0, initialValues.size())
                .filter(proposeResponses::get)
                .mapToObj(initialValues::get)
                .max(MAX_ACCEPTED)
                .orElse(LogReplicaResponse.EMPTY);
        final byte[] maxValue = maxInitial.getValue();
        final List<ThrowingFunction<LogReplicaClient, Boolean, Exception>> acceptFunctions = initialValues.stream()
                .<ThrowingFunction<LogReplicaClient, Boolean, Exception>>map(
                        response -> (replica -> replica.writeAtomic(
                                index,
                                newProposal,
                                newProposal,
                                maxInitial.getAccepted() == 0 ? value : maxValue,
                                false,
                                newProposal,
                                response.getAccepted())))
                .collect(Collectors.toList());
        List<Boolean> acceptResponses = Quorum.broadcast(replicas, n-f, proposeResponses, acceptFunctions, Boolean.FALSE);
        if (Base.sum(acceptResponses) < n-f) return false;
        return maxInitial.getAccepted() == priorAccepted;
    }

    @Nullable
    public byte[] read(long index) throws Exception {
        final List<LogReplicaResponse> responses = Quorum.broadcast(replicas, n-f, replica -> replica.read(index), LogReplicaResponse.FAILURE);
        final LogReplicaResponse maxResponse = responses.stream().max(MAX_ACCEPTED).orElse(LogReplicaResponse.EMPTY);
        if (maxResponse.getAccepted() == 0) return null;
        final long maxAcceptedCount = responses.stream().filter(r -> r.getAccepted() == maxResponse.getAccepted()).count();
        final byte[] maxValue = maxResponse.getValue();
        if (maxAcceptedCount >= n-f) return maxValue;
        //TODO this might be overly strict, maybe make write2 return the value it wrote
        if (write2(index, maxValue, maxResponse.getAccepted(), responses)) return maxValue;
        throw new IOException();
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
        return writeString(index, value, false);
    }

    public boolean writeString(long index, String value, boolean skipPropose) throws Exception {
        return write(index, value.getBytes(Base.UTF_8), skipPropose);
    }
}
