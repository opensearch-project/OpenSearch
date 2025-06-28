/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stats;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Request for Flight transport statistics
 */
class FlightStatsRequest extends BaseNodesRequest<FlightStatsRequest> {

    public FlightStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    public FlightStatsRequest(String... nodeIds) {
        super(nodeIds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    public static class NodeRequest extends TransportRequest {
        public NodeRequest() {}

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
