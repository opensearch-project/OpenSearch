/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Example native Arrow response — just extend {@link ArrowBatchResponse}.
 *
 * <p>The framework handles everything:
 * <ul>
 *   <li>Send side: zero-copy transfers the root's buffers into the Flight stream</li>
 *   <li>Receive side: provides the root via {@link #getRoot()} — no deserialization</li>
 * </ul>
 *
 * <p>No writeTo/read override needed. The base class handles both.
 */
class NativeArrowStreamDataResponse extends ArrowBatchResponse {

    NativeArrowStreamDataResponse(VectorSchemaRoot root) {
        super(root);
    }

    NativeArrowStreamDataResponse(StreamInput in) throws IOException {
        super(in);
    }
}
