/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.core.transport.TransportResponse;

/**
 * Marker interface for {@link org.opensearch.transport.TransportResponseHandler} instances
 * that can consume native Arrow columnar data. When the Arrow Flight transport detects a handler
 * implementing this interface, it passes the received {@link VectorSchemaRoot} directly —
 * bypassing the byte-oriented {@link VectorStreamInput} deserialization path.
 *
 * <p>Implementations must also implement {@code read(StreamInput)} as a fallback
 * for non-Flight transports (e.g., Netty4).
 *
 * @opensearch.experimental
 */
public interface ArrowStreamHandler<T extends TransportResponse> {

    /**
     * Reads a response directly from a native Arrow {@link VectorSchemaRoot}.
     * Called instead of {@code read(StreamInput)} when the Flight transport
     * receives native Arrow data (i.e., the server sent an {@link ArrowBatchResponse}).
     *
     * @param root the Arrow columnar data for this batch
     * @return the deserialized response
     */
    T readArrow(VectorSchemaRoot root);
}
