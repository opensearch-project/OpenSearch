/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.flight.transport.ArrowBatchResponse;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Streaming Arrow response for the QTF fetch phase.
 * Carries a single Arrow batch from the data node back to the coordinator
 * via the streaming transport — zero-copy, no IPC serialization.
 *
 * @opensearch.internal
 */
public class FetchByRowIdsArrowResponse extends ArrowBatchResponse {

    public FetchByRowIdsArrowResponse(VectorSchemaRoot root) {
        super(root);
    }

    public FetchByRowIdsArrowResponse(StreamInput in) throws IOException {
        super(in);
    }
}
