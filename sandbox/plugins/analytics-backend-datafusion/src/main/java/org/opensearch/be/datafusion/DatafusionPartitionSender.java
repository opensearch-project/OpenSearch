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

/**
 * Type-safe wrapper around a native {@code PartitionStreamSender} pointer.
 *
 * <p>Produced by {@link NativeBridge#registerPartitionStream(long, String, byte[])} and used
 * by {@link DatafusionReduceSink#feed} to push Arrow C Data batches into the reduce input
 * stream. Closing the sender signals EOF to the DataFusion receiver side.
 */
public final class DatafusionPartitionSender extends NativeHandle {

    /**
     * Wraps the given sender pointer.
     *
     * @param senderPtr pointer returned by {@link NativeBridge#registerPartitionStream}
     */
    public DatafusionPartitionSender(long senderPtr) {
        super(senderPtr);
    }

    @Override
    protected void doClose() {
        NativeBridge.senderClose(ptr);
    }
}
