/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.grpc;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.search.action.ArrowBatchSink;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.transport.grpc.spi.StreamSearchResponseChannel;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Sends Arrow data as IPC messages over a gRPC response stream. It sends one message at a time, and only while the call is
 * ready, so the transport never buffers more than its flow-control window.
 * <p>
 * Senders take turns in arrival order. They wait on {@link ReentrantLock}s, which unmount virtual threads. A client that
 * stops reading for longer than the idle timeout fails the search, which releases what the search holds on every node. The
 * messages already handed to gRPC stay queued, and keep their memory, until the client reads them and then sees
 * {@code DEADLINE_EXCEEDED}, or until the call is {@link #reset}.
 */
public final class GrpcArrowBatchSink implements ArrowBatchSink {

    /** How long a wait for readiness lasts before it checks again, in case a ready notification is missed. */
    private static final long READY_RECHECK_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    /** Marks that the client cancelled, so a cancel callback set later runs at once. */
    private static final Runnable FIRED = () -> {};

    private final StreamSearchResponseChannel channel;
    private final Supplier<TimeValue> idleTimeout;
    private final Executor executor;
    private final ReentrantLock sendLock = new ReentrantLock(true);
    private final ReentrantLock readyLock = new ReentrantLock();
    private final Condition readyOrEnded = readyLock.newCondition();
    private final AtomicReference<Exception> ended = new AtomicReference<>();
    private final AtomicReference<Runnable> onCancel = new AtomicReference<>();
    private volatile boolean schemaSent;

    /**
     * Creates the sink of one call and registers its ready and cancel handlers.
     *
     * @param channel     the response channel of the call
     * @param idleTimeout how long the client may stop reading
     * @param executor    runs the cancel callback, off the gRPC thread
     */
    public GrpcArrowBatchSink(StreamSearchResponseChannel channel, Supplier<TimeValue> idleTimeout, Executor executor) {
        this.channel = channel;
        this.idleTimeout = idleTimeout;
        this.executor = executor;
        channel.setOnReadyHandler(this::signal);
        channel.setOnCancelHandler(() -> {
            end(new TaskCancelledException("the client cancelled the arrow search"));
            final Runnable callback = onCancel.getAndSet(FIRED);
            if (callback != null) {
                executor.execute(callback);
            }
        });
    }

    @Override
    public void sendSchema(Schema schema) throws Exception {
        sendLock.lockInterruptibly();
        try {
            awaitReady();
            channel.sendPayload(ArrowIpcEncoder.schema(schema));
            schemaSent = true;
        } finally {
            sendLock.unlock();
        }
    }

    @Override
    public void sendBatch(VectorSchemaRoot batch) throws Exception {
        boolean owned = true;
        try {
            sendLock.lockInterruptibly();
            try {
                awaitReady();
                // The encoder, then the channel, owns the batch from here and closes it, also on failure.
                owned = false;
                channel.sendPayload(ArrowIpcEncoder.recordBatch(batch));
            } finally {
                sendLock.unlock();
            }
        } finally {
            if (owned) {
                batch.close();
            }
        }
    }

    /**
     * Waits until the call can take a message.
     *
     * @throws Exception the reason the stream ended, or a timeout if the client stopped reading for too long
     */
    private void awaitReady() throws Exception {
        final long timeoutNanos = idleTimeout.get().nanos();
        final long start = System.nanoTime();
        readyLock.lockInterruptibly();
        try {
            while (true) {
                final Exception end = ended.get();
                if (end != null) {
                    throw end;
                }
                if (channel.isReady()) {
                    return;
                }
                final long remaining = timeoutNanos - (System.nanoTime() - start);
                if (remaining <= 0) {
                    final OpenSearchStatusException timeout = new OpenSearchStatusException(
                        "the client did not read the arrow search results for [" + idleTimeout.get() + "]",
                        RestStatus.REQUEST_TIMEOUT
                    );
                    end(timeout);
                    throw timeout;
                }
                readyOrEnded.awaitNanos(Math.min(remaining, READY_RECHECK_NANOS));
            }
        } finally {
            readyLock.unlock();
        }
    }

    @Override
    public void complete(SearchResponse summary) {
        if (schemaSent == false) {
            // Without a schema, something ran the request as a regular search, e.g. the security plugin's filter-level
            // document-level security. The client would get a summary without its rows.
            fail(
                new OpenSearchStatusException(
                    "the search did not run as an Arrow search; Arrow search does not support filter-level document-level security",
                    RestStatus.NOT_IMPLEMENTED
                )
            );
            return;
        }
        end(new IllegalStateException("the arrow search already completed"));
        channel.complete(summary);
    }

    @Override
    public void fail(Exception failure) {
        end(failure);
        channel.fail(failure);
    }

    @Override
    public void reset() {
        end(new IllegalStateException("the arrow search was reset"));
        channel.reset();
    }

    @Override
    public void setOnCancel(Runnable callback) {
        final Runnable previous = onCancel.getAndUpdate(current -> current == FIRED ? FIRED : callback);
        if (previous == FIRED) {
            executor.execute(callback);
        }
    }

    /** Ends the stream for senders, waking those that wait. */
    private void end(Exception reason) {
        if (ended.compareAndSet(null, reason)) {
            signal();
        }
    }

    private void signal() {
        readyLock.lock();
        try {
            readyOrEnded.signalAll();
        } finally {
            readyLock.unlock();
        }
    }
}
