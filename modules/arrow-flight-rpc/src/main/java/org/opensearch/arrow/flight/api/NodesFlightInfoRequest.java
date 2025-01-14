/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

public class NodesFlightInfoRequest extends BaseNodesRequest<NodesFlightInfoRequest> {

    public NodesFlightInfoRequest(StreamInput in) throws IOException {
        super(in);
    }

    public NodesFlightInfoRequest(String... nodesIds) {
        super(nodesIds);
    }

    public static class NodeFlightInfoRequest extends TransportRequest {
        NodesFlightInfoRequest request;

        public NodeFlightInfoRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeFlightInfoRequest(NodesFlightInfoRequest request) {
            this.request = request;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
