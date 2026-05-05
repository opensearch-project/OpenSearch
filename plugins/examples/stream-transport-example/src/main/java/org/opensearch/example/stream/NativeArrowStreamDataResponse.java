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
 * Example native Arrow response. Extend {@link ArrowBatchResponse} and provide two constructors:
 * one wrapping a {@link VectorSchemaRoot} (send side) and one taking {@link StreamInput} (receive side).
 */
class NativeArrowStreamDataResponse extends ArrowBatchResponse {

    NativeArrowStreamDataResponse(VectorSchemaRoot root) {
        super(root);
    }

    NativeArrowStreamDataResponse(StreamInput in) throws IOException {
        super(in);
    }
}
