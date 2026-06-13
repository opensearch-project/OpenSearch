/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Type-safe wrapper around a native {@code PartitionStreamSender} pointer. Closing
 * the sender signals EOF to the DataFusion receiver side.
 *
 * <p>The {@code lifecycle} read-write lock serialises {@link #send} / {@link #close}: the
 * native {@code sender_send_async} read-validates the heap-allocated sender pointer against
 * {@code sender_close}'s {@code Box} reclaim. The send is now <b>initiated</b> synchronously
 * under the lock (no native borrow is held across an {@code await}); a full-channel send owns a
 * <b>cloned</b> {@code mpsc::Sender}, so {@code close()} can never UAF an in-flight send and no
 * longer waits for one. The park on a pending send happens <b>outside</b> the lock — on a virtual
 * thread it unmounts the carrier. EOF defers until the pending batch lands (the cloned sender
 * keeps the channel open until it drops).
 */
public final class DatafusionPartitionSender extends NativeHandle {

    private static final Logger logger = LogManager.getLogger(DatafusionPartitionSender.class);
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();

    /**
     * Latched once a send reports {@link NativeBridge#SENDER_SEND_RECEIVER_DROPPED} — the
     * consumer (e.g. a LimitExec above the ExchangeReducer) satisfied its fetch and tore down
     * this channel's receiver. Monotonic; once set, no further batch on this channel will be
     * consumed. Per-sender (not per-sink) so a multi-input reduce only stops the input whose
     * receiver is actually gone.
     */
    private volatile boolean receiverDropped;

    public DatafusionPartitionSender(long senderPtr) {
        super(senderPtr);
    }

    /**
     * Sends one exported batch. Returns {@code 0} on a normal send or
     * {@link NativeBridge#SENDER_SEND_RECEIVER_DROPPED} if the consumer already dropped the
     * receiver (benign — the caller should discard the batch and stop feeding).
     */
    public long send(long arrayAddr, long schemaAddr) {
        CompletableFuture<Long> done = new CompletableFuture<>();
        long rc;
        lifecycle.readLock().lock();
        try {
            rc = NativeBridge.senderSendAsync(
                getPointer(),
                arrayAddr,
                schemaAddr,
                ActionListener.wrap(done::complete, done::completeExceptionally)
            );
        } finally {
            lifecycle.readLock().unlock();
        }
        if (rc == NativeBridge.SENDER_SEND_PENDING) {
            // Channel full: park outside the lock until the deferred send completes. On a virtual
            // thread this unmounts the carrier — no platform thread is held for backpressure.
            rc = done.join();
        }
        if (rc == NativeBridge.SENDER_SEND_RECEIVER_DROPPED) {
            receiverDropped = true;
        }
        return rc;
    }

    /** True once the consumer dropped this channel's receiver (see {@link #receiverDropped}). */
    public boolean isReceiverDropped() {
        return receiverDropped;
    }

    @Override
    public void close() {
        lifecycle.writeLock().lock();
        try {
            super.close();
            logger.debug("[sender] closed ptr={}", ptr);
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    protected void doClose() {
        NativeBridge.senderClose(ptr);
    }
}
