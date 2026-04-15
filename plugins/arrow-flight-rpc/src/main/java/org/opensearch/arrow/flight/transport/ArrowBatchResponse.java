/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Marker interface for {@link org.opensearch.core.transport.TransportResponse} instances
 * that carry native Arrow columnar data. When the Arrow Flight transport detects a response
 * implementing this interface, it sends the {@link VectorSchemaRoot} directly via
 * {@code putNext()} — bypassing the byte-oriented {@link VectorStreamOutput} serialization path.
 *
 * <p>The caller retains ownership of the {@link VectorSchemaRoot}. The Flight transport
 * reads from it during {@code putNext()} but does not close it. The caller is responsible
 * for closing the root after the response has been sent.
 *
 * <p>Implementations must also implement {@code writeTo(StreamOutput)} as a fallback
 * for non-Flight transports (e.g., Netty4).
 *
 * @opensearch.experimental
 */
public interface ArrowBatchResponse {

    /**
     * Returns the native Arrow data for this batch.
     */
    VectorSchemaRoot getArrowRoot();

    /**
     * Returns the Arrow schema for this response.
     */
    Schema getArrowSchema();
}
