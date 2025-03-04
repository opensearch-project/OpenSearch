/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api.flightinfo;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Flight Info Request
 */
public class NodesFlightInfoRequest extends BaseNodesRequest<NodesFlightInfoRequest> {

    /**
     * Constructor for NodesFlightInfoRequest
     * @param in StreamInput
     * @throws IOException If an I/O error occurs
     */
    public NodesFlightInfoRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor for NodesFlightInfoRequest
     * @param nodesIds String array of node IDs
     */
    public NodesFlightInfoRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Writes the request to the given StreamOutput
     */
    public static class NodeFlightInfoRequest extends TransportRequest {
        NodesFlightInfoRequest request;

        /**
         * Constructor for NodeFlightInfoRequest
         * @param in StreamInput to read from
         * @throws IOException If an I/O error occurs
         */
        public NodeFlightInfoRequest(StreamInput in) throws IOException {
            super(in);
        }

        /**
         * Constructor for NodeFlightInfoRequest
         * @param request NodesFlightInfoRequest
         */
        public NodeFlightInfoRequest(NodesFlightInfoRequest request) {
            this.request = request;
        }
    }

    /**
     * Writes the request to the given StreamOutput
     * @param out StreamOutput to write to
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
