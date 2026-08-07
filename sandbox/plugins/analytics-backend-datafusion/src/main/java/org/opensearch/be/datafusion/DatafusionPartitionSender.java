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
     * Non-null once {@link #fail} was called: the reason to push into the partition stream as an error
     * before teardown. Read by {@link #doClose} to pick {@code senderFail} (error then free) over
     * {@code senderClose} (clean-EOF free). Set under the write lock before the once-only close runs.
     */
    private volatile String failReason;

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

    /**
     * Fails the partition stream instead of closing it cleanly: records {@code reason} and runs the
     * once-only teardown, which {@link #doClose} routes to {@code senderFail} — pushing an error into
     * the channel so the consumer's stream yields an ERROR (failing the join/agg) rather than a clean
     * EOF. Use this when the producer/drain died mid-stream and the partition is TRUNCATED; a clean
     * close would let the consumer silently compute a result from partial input. Idempotent (the
     * NativeHandle once-only close guard ensures the native sender is freed exactly once); a {@link
     * #close} after a {@link #fail} is a no-op (and vice-versa).
     */
    public void fail(String reason) {
        lifecycle.writeLock().lock();
        try {
            // Set the reason BEFORE close() so doClose (which runs inside close()'s once-only guard)
            // sees it. If already closed, this is a harmless no-op (close() won't re-run doClose).
            this.failReason = reason == null ? "unknown" : reason;
            super.close();
            logger.debug("[sender] failed ptr={} reason={}", ptr, reason);
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    protected void doClose() {
        // Routed by failReason: an explicit fail() pushes an error into the stream (consumer sees an
        // ERROR, not clean EOF) then frees the sender; a normal close just frees it (clean EOF).
        String reason = failReason;
        if (reason != null) {
            NativeBridge.senderFail(ptr, reason);
        } else {
            NativeBridge.senderClose(ptr);
        }
    }
}
