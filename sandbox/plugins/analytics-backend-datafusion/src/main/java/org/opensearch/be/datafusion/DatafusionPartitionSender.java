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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Type-safe wrapper around a native {@code PartitionStreamSender} pointer. Closing
 * the sender signals EOF to the DataFusion receiver side.
 *
 * <p>The {@code lifecycle} read-write lock serialises {@link #send} / {@link #close}:
 * native {@code sender_send} holds an immutable borrow of the heap-allocated sender
 * across an {@code mpsc::Sender::send().await}, while {@code sender_close} reclaims
 * the {@code Box} — a use-after-free if these overlap.
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
        lifecycle.readLock().lock();
        try {
            long rc = NativeBridge.senderSend(getPointer(), arrayAddr, schemaAddr);
            if (rc == NativeBridge.SENDER_SEND_RECEIVER_DROPPED) {
                receiverDropped = true;
            }
            return rc;
        } finally {
            lifecycle.readLock().unlock();
        }
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
