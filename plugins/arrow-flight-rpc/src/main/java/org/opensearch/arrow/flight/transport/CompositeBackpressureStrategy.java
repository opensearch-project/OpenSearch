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
 * to those values — there is no second read to disagree with. The handlers'
 * {@code notifyAll()} simply wakes the waiter so the next iteration reads fresh.
 *
 * <p>Composes channel-level cancel cleanup: the cancel callback runs the channel's
 * cleanup (e.g. marking cancelled, releasing resources) before notifying parked
 * waiters, so a thread waking from {@code waitForListener} always observes the
 * channel in cancelled state.
 */
final class CompositeBackpressureStrategy implements BackpressureStrategy {

    private final Object lock = new Object();
    private final Runnable channelCancelCleanup;
    private ServerStreamListener listener;

    CompositeBackpressureStrategy(Runnable channelCancelCleanup) {
        this.channelCancelCleanup = channelCancelCleanup;
    }

    @Override
    public void register(ServerStreamListener listener) {
        this.listener = listener;
        listener.setOnReadyHandler(() -> {
            synchronized (lock) {
                lock.notifyAll();
            }
        });
        listener.setOnCancelHandler(() -> {
            try {
                channelCancelCleanup.run();
            } finally {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        });
    }

    @Override
    public WaitResult waitForListener(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (lock) {
            while (true) {
                if (listener.isCancelled()) {
                    return WaitResult.CANCELLED;
                }
                if (listener.isReady()) {
                    return WaitResult.READY;
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return WaitResult.TIMEOUT;
                }
                try {
                    lock.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return WaitResult.OTHER;
                }
            }
        }
    }
}
