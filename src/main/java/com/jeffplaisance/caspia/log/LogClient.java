package com.jeffplaisance.caspia.log;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.jeffplaisance.caspia.common.Base;
import com.jeffplaisance.caspia.common.Quorum;
import com.jeffplaisance.caspia.common.ThrowingFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@NotThreadSafe
public final class LogClient {

    private static final Logger LOG = LoggerFactory.getLogger(LogClient.class);
    private static final Comparator<LogReplicaState> MAX_ACCEPTED = Ordering.from((LogReplicaState a, LogReplicaState b) -> Ints.compare(a.getAccepted(), b.getAccepted())).nullsFirst();
    private static final boolean fastPathEnabled;
    private static final boolean oneRoundTripReadsEnabled;

    static {
        fastPathEnabled = !Boolean.getBoolean("com.jeffplaisance.caspia.log.disableFastPath");
        oneRoundTripReadsEnabled = !Boolean.getBoolean("com.jeffplaisance.caspia.log.disableOneRoundTripRead");
    }

    private final List<LogReplicaClient> replicas;
    private final int n;
    private final int f;

    private long fastPathIndex = -1;

    public LogClient(List<? extends LogReplicaClient> replicas) {
        n = replicas.size();
        f = Base.lessThanHalf(n);
        this.replicas = new ArrayList<>(replicas);
    }

    /**
     * @param index the index
     * @param value the value that we will be attempting to write at index
     * @return a return value of true means that value was committed at index. a return value of false means that a
     * value was already committed at index. it is possible for the committed value to be binary equivalent to value and
     * for this method to still return false if the same value had been proposed and accepted in an earlier round.
     * @throws Exception if less than a quorum of responses is obtained (can be triggered by a conflicting client).
     * this method does not retry on failures or conflicts. retry logic should be handled by the caller.
     */
    public boolean write(long index, byte[] value) throws Exception {
        // null is used as a sentinel value to signify that no value has been written at this index yet.
        // it is valid for there to be a null accepted value which will transition to a non-null value later.
        // accepted non-null values can never be changed.
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(index > 0);

        if (tryFastPathWrite(index, value)) return true;

        final List<LogReplicaState> initialValues = readInitialValues(index);
        // reference equality check is intentional
        if (write2(index, value, initialValues) == value) {
            fastPathIndex = index+1;
            return true;
        }
        fastPathIndex = -1;
        return false;
    }

    private boolean tryFastPathWrite(long index, byte[] value) throws Exception {
        if (fastPathEnabled) {
            // fast path is an optimization to allow one round trip writes
            // this is true iff this client was the one to commit a non-null value at index-1
            // fast path always uses proposal number 1
            // any competing proposer will start at proposal number 2 and block fast path
            try {
                if (index == fastPathIndex) {
                    final List<Boolean> responses = Quorum.broadcast(replicas, n - f, replica -> replica.writeAtomic(index, new LogReplicaState(1, 1, value), true, LogReplicaState.EMPTY), false);
                    if (Base.sum(responses) >= n - f) {
                        fastPathIndex = index + 1;
                        return true;
                    } else {
                        fastPathIndex = -1;
                    }
                }
            } catch (Throwable t) {
                fastPathIndex = -1;
                Throwables.propagateIfInstanceOf(t, Exception.class);
                throw Throwables.propagate(t);
            }
        }
        return false;
    }

    private List<LogReplicaState> readInitialValues(long index) throws Exception {
        return Quorum.broadcast(replicas, n - f, replica -> replica.read(index), LogReplicaState.EMPTY);
    }

    /**
     * this function is the core of the algorithm. chooses next proposal number based on initial values and runs
     * propose and accept phases of the algorithm.
     * @param index the index
     * @param value the value to write at index
     * @param initialValues the state of the rows at index read from a quorum of replicas. response should be
     * LogReplicaResponse.EMPTY for replicas from which we did not receive a response.
     * @return the value that was written. reference equality of return value and input value is guaranteed if
     * input value was the value written.
     * @throws Exception if successful on less than a quorum or replicas
     */
    private @Nullable byte[] write2(final long index, final @Nullable byte[] value, final List<LogReplicaState> initialValues) throws Exception {

        // lowest possible value for newProposal is 2 since 1 is reserved for fast path
        final int newProposal = initialValues.stream().map(LogReplicaState::getProposal).reduce(1, Math::max)+1;
        final List<Optional<LogReplicaState>> proposeResponses = doPropose(index, initialValues, newProposal);
        return doAccept(index, value, newProposal, proposeResponses);
    }

    /**
     * @param index the index
     * @param initialValues the initial values
     * @param newProposal the proposal number to try, must be greater than all proposal numbers in initialValues
     * @return list containing state of each replica where propose succeeded or empty if propose failed
     * @throws Exception if successful on less than a quorum or replicas
     */
    private List<Optional<LogReplicaState>> doPropose(long index, List<LogReplicaState> initialValues, int newProposal) throws Exception {
        // attempt to increase proposal to newProposal on all replicas leaving all other fields the same
        final List<ThrowingFunction<LogReplicaClient, Optional<LogReplicaState>, Exception>> proposeFunctions = initialValues.stream()
                .<ThrowingFunction<LogReplicaClient, Optional<LogReplicaState>, Exception>>map(
                        state -> (replica -> {
                            final LogReplicaState nextState = new LogReplicaState(newProposal, state.getAccepted(), state.getValue());
                            if (replica.writeAtomic(index, nextState, state.getProposal() == 0, state)) {
                                return Optional.of(nextState);
                            }
                            return Optional.empty();
                        }))
                .collect(Collectors.toList());
        final List<Optional<LogReplicaState>> proposeResponses = Quorum.broadcast(replicas, n-f, proposeFunctions, Optional.empty());

        // check for success on a quorum of replicas, throw exception on failure
        if (proposeResponses.stream().filter(Optional::isPresent).count() < n-f) {
            throw new Exception();
        }
        return proposeResponses;
    }

    /**
     * @param index the index
     * @param value the value to attempt to write at index
     * @param newProposal the proposal number for which we successfully updated a quorum of replicas
     * @param proposeResponses list containing state of each replica where propose succeeded or empty if propose failed
     * @return the value that was written. reference equality of return value and input value is guaranteed if input
     * value was the value written.
     * @throws Exception if successful on less than a quorum or replicas
     */
    private @Nullable byte[] doAccept(long index, @Nullable byte[] value, int newProposal, List<Optional<LogReplicaState>> proposeResponses) throws Exception {
        // find the response with the highest accepted number from the replicas where propose succeeded
        final LogReplicaState maxInitial = proposeResponses.stream()
                .flatMap(Base::toStream)
                .max(MAX_ACCEPTED)
                .orElse(LogReplicaState.EMPTY);

        // if a value has already been written at this index we need to make sure it is propagated
        // if no value has been written we can write our own value
        final byte[] valueWritten = maxInitial.getValue() == null ? value : maxInitial.getValue();

        final LogReplicaState nextState = new LogReplicaState(newProposal, newProposal, valueWritten);
        // attempt to update accepted to newProposal and value to valueWritten on replicas where propose succeeded
        final List<Optional<ThrowingFunction<LogReplicaClient, Boolean, Exception>>> acceptFunctions = proposeResponses.stream()
                .map(optional -> optional
                        .<ThrowingFunction<LogReplicaClient, Boolean, Exception>>map(state ->
                                replica -> replica.writeAtomic(index, nextState, false, state)
                        ))
                .collect(Collectors.toList());
        List<Boolean> acceptResponses = Quorum.broadcast2(replicas, n-f, acceptFunctions, Boolean.FALSE);

        // check for success on a quorum of replicas, return valueWritten on success and throw exception on failure
        if (Base.sum(acceptResponses) < n-f) {
            throw new Exception();
        }
        return valueWritten;
    }

    /**
     * @param index the index
     * @return the value written at the index or null if no value has been written at the index. a return value of null
     * implies that the values at all indices greater than index are also null.
     * @throws Exception if less than a quorum of responses is obtained (can be triggered by a conflicting client).
     * this method does not retry on failures or conflicts. retry logic should be handled by the caller.
     */
    @Nullable
    public byte[] read(long index) throws Exception {
        Preconditions.checkArgument(index > 0);

        final List<LogReplicaState> responses = readInitialValues(index);

        if (oneRoundTripReadsEnabled) {
            // this is an optimization to allow one round trip reads at indexes which have a non-null committed value.
            // this block is not required for correctness.
            final LogReplicaState maxAccepted = responses.stream().max(MAX_ACCEPTED).orElse(LogReplicaState.EMPTY);
            final byte[] maxValue = maxAccepted.getValue();
            if (maxValue != null) {
                final long maxAcceptedCount = responses.stream().filter(r -> r.getAccepted() == maxAccepted.getAccepted()).count();
                if (maxAcceptedCount >= n - f) {
                    // if there is a non-null value committed in the same round at a quorum of replicas it cannot change
                    // and we can return it safely.
                    return maxValue;
                }
            }
        }

        // attempting to write null is the equivalent of writing a noop in multi paxos. we are able to take advantage
        // of the ability to create a compare-and-swap register from CASPaxos to enable us to overwrite the null value
        // later, which allows us to ensure a gap-free log. this null value must be overwritten before any non-null
        // write occurs at index+1.
        // writing on read is necessary to prevent linearizability issues that could otherwise arise due to the
        // potential for partially committed values to become fully committed as a result of future read operations.
        // if there is a partially committed value this will either force it to become fully committed or overwrite it
        // with a null value.
        return write2(index, null, responses);
    }

    /**
     * to find the end of the log, call this method and then call read in a loop starting from index and incrementing
     * by one each iteration until a null value is found.
     * @return reads the maximum index with a non-zero proposal from a quorum of replicas and returns the maximum of
     * those indices.
     * @throws Exception if less than a quorum of responses is obtained. this method does not retry on failures or
     * conflicts. retry logic should be handled by the caller.
     */
    public long readLastIndex() throws Exception {
        final List<Long> maxValues = Quorum.broadcast(replicas, n-f, LogReplicaClient::readLastIndex, 0L);
        return maxValues.stream().reduce(0L, Math::max);
    }

    /**
     * helper that calls read and parses the result as a UTF-8 string
     * @param index the index
     * @return the result of read converted to a string or null if the result of read was null
     * @throws Exception if read throws an exception or if the value is not null or a valid UTF-8 string
     */
    @Nullable
    public String readString(long index) throws Exception {
        final byte[] bytes = read(index);
        return bytes == null ? null : new String(bytes, Base.UTF_8);
    }

    /**
     * helper that converts string value to UTF-8 bytes and calls write
     * @param index the index
     * @param value the value to write at index
     * @return the result of calling write at index with value converted to UTF-8 bytes
     * @throws Exception if write throws an exception
     */
    public boolean writeString(long index, String value) throws Exception {
        return write(index, value.getBytes(Base.UTF_8));
    }
}
