/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

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

    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock();

    public DatafusionPartitionSender(long senderPtr) {
        super(senderPtr);
    }

    public void send(long arrayAddr, long schemaAddr) {
        lifecycle.readLock().lock();
        try {
            NativeBridge.senderSend(getPointer(), arrayAddr, schemaAddr);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    @Override
    public void close() {
        lifecycle.writeLock().lock();
        try {
            assert lifecycle.isWriteLockedByCurrentThread() : "close must hold the write lock across super.close()";
            super.close();
        } finally {
            lifecycle.writeLock().unlock();
        }
    }

    @Override
    protected void doClose() {
        NativeBridge.senderClose(ptr);
    }
}
