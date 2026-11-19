/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * {@link BackpressureStrategy} that fixes a known race in Arrow's
 * {@code CallbackBackpressureStrategy} (apache/arrow-java#346): that strategy reads
 * {@code listener.isReady()} twice — once as the wait-loop predicate, then again
 * post-loop to decide which {@code WaitResult} to return. gRPC mutates
 * {@code isReady()} from Netty event-loop threads outside the strategy's lock, so
 * the two reads can disagree (e.g. predicate sees {@code true} and exits, post-loop
 * sees {@code false} after another producer's write re-filled the outbound buffer),
 * producing {@code "Invalid state when waiting for listener"} runtime exceptions.
 *
 * <p>This implementation reads {@code listener.isReady()} and
 * {@code listener.isCancelled()} once per loop iteration inside the lock and commits
 * to those values — there is no second read to disagree with. The handlers' {@link #signal()}
 * only wakes the waiters; each waiter reads fresh values on its next iteration.
 *
 * <p>Composes channel-level cancel cleanup: the cancel callback runs the channel's
 * cleanup (e.g. marking cancelled, releasing resources) before notifying parked
 * waiters, so a thread waking from {@code waitForListener} always observes the
 * channel in cancelled state.
 *
 * <p>A call the server ended itself is never ready again, and gRPC fires neither callback for it. The wait therefore also
 * ends once the channel is closed; the channel calls {@link #signal()} when it closes.
 */
final class CompositeBackpressureStrategy implements BackpressureStrategy {

    // A ReentrantLock rather than a monitor, so a virtual thread waiting for the consumer unmounts from its carrier.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition readyOrCancelled = lock.newCondition();
    private final Runnable channelCancelCleanup;
    private final BooleanSupplier channelClosed;
    private ServerStreamListener listener;

    /**
     * Creates the strategy for one server stream.
     *
     * @param channelCancelCleanup runs when gRPC reports the call cancelled, before parked waiters wake
     * @param channelClosed        whether the channel is closed, which ends a wait with {@link WaitResult#OTHER}
     */
    CompositeBackpressureStrategy(Runnable channelCancelCleanup, BooleanSupplier channelClosed) {
        this.channelCancelCleanup = channelCancelCleanup;
        this.channelClosed = channelClosed;
    }

    @Override
    public void register(ServerStreamListener listener) {
        this.listener = listener;
        listener.setOnReadyHandler(this::signal);
        listener.setOnCancelHandler(() -> {
            try {
                channelCancelCleanup.run();
            } finally {
                signal();
            }
        });
    }

    /** Wakes parked waiters so they re-check the listener and the channel. */
    void signal() {
        lock.lock();
        try {
            readyOrCancelled.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public WaitResult waitForListener(long timeoutMillis) {
        // Long.MAX_VALUE waits until ready, cancelled or closed; each signals the condition.
        final boolean unbounded = timeoutMillis == Long.MAX_VALUE;
        final long deadline = unbounded ? Long.MAX_VALUE : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        lock.lock();
        try {
            while (true) {
                if (listener.isCancelled()) {
                    return WaitResult.CANCELLED;
                }
                if (channelClosed.getAsBoolean()) {
                    return WaitResult.OTHER;
                }
                if (listener.isReady()) {
                    return WaitResult.READY;
                }
                if (unbounded) {
                    readyOrCancelled.await();
                } else {
                    final long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        return WaitResult.TIMEOUT;
                    }
                    readyOrCancelled.awaitNanos(remaining);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return WaitResult.OTHER;
        } finally {
            lock.unlock();
        }
    }
}
