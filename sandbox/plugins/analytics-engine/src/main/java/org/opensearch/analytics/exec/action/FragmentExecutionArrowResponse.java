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
 * Transport response carrying a single Arrow {@link VectorSchemaRoot} batch produced by
 * fragment execution on a data node, streamed back to the coordinator.
 *
 * @opensearch.internal
 */
public class FragmentExecutionArrowResponse extends ArrowBatchResponse {

    public FragmentExecutionArrowResponse(VectorSchemaRoot root) {
        super(root);
    }

    public FragmentExecutionArrowResponse(StreamInput in) throws IOException {
        super(in);
    }
}
