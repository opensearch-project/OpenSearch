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

    /**
     * Set when the owning reduce sink has finished accepting input. A send that is already in
     * progress completes or is released by native early termination, then closes this handle after
     * it relinquishes the read lock.
     */
    private volatile boolean closeRequested;

    public DatafusionPartitionSender(long senderPtr) {
        super(senderPtr);
    }

    /**
     * Sends one exported batch. Returns {@code 0} on a normal send or
     * {@link NativeBridge#SENDER_SEND_RECEIVER_DROPPED} if the consumer already dropped or
     * gracefully terminated the receiver.
     */
    public long send(long arrayAddr, long schemaAddr) {
        lifecycle.readLock().lock();
        try {
            if (closeRequested) {
                throw new IllegalStateException("sender close requested");
            }
            long rc = NativeBridge.senderSend(getPointer(), arrayAddr, schemaAddr);
            if (rc == NativeBridge.SENDER_SEND_RECEIVER_DROPPED) {
                receiverDropped = true;
            }
            return rc;
        } finally {
            lifecycle.readLock().unlock();
            closeAfterInFlightSend();
        }
    }

    /** True once the consumer dropped this channel's receiver (see {@link #receiverDropped}). */
    public boolean isReceiverDropped() {
        return receiverDropped;
    }

    /** True once {@link #close()} or {@link #requestEarlyTermination()} has been invoked. */
    public boolean isCloseRequested() {
        return closeRequested;
    }

    @Override
    public void close() {
        closeRequested = true;
        lifecycle.writeLock().lock();
        try {
            closeUnderWriteLock("close");
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    /**
     * Signals normal end-of-input for this partition without cancelling the whole query.
     *
     * <p>If no send is active, this drops the native sender immediately and its receiver drains
     * buffered batches before EOF. If a feeder holds the read lock inside a blocking native send,
     * the native receiver is closed instead: that unblocks the send with
     * {@link NativeBridge#SENDER_SEND_RECEIVER_DROPPED}; the feeder then performs the deferred
     * native-sender close after releasing its read lock.
     */
    public void requestEarlyTermination() {
        closeRequested = true;
        if (lifecycle.writeLock().tryLock()) {
            try {
                closeUnderWriteLock("early termination");
            } finally {
                lifecycle.writeLock().unlock();
            }
            return;
        }

        // An active send has an immutable native borrow. Hold a shared Java read lock while
        // signalling its receiver so no concurrent close can reclaim that borrowed sender.
        lifecycle.readLock().lock();
        try {
            try {
                NativeBridge.senderTerminateEarly(getPointer());
            } catch (IllegalStateException ignored) {
                // The in-flight send completed and performed the deferred close first.
            }
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private void closeAfterInFlightSend() {
        if (closeRequested == false) {
            return;
        }
        lifecycle.writeLock().lock();
        try {
            closeUnderWriteLock("deferred close");
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    private void closeUnderWriteLock(String reason) {
        super.close();
        logger.debug("[sender] closed ptr={} ({})", ptr, reason);
    }

    @Override
    protected void doClose() {
        NativeBridge.senderClose(ptr);
    }
}
