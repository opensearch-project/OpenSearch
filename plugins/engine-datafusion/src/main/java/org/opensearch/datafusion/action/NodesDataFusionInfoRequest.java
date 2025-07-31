/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for retrieving DataFusion information from nodes
 */
public class NodesDataFusionInfoRequest extends BaseNodesRequest<NodesDataFusionInfoRequest> {

    /**
     * Default constructor for NodesDataFusionInfoRequest.
     */
    public NodesDataFusionInfoRequest() {
        super((String[]) null);
    }

    /**
     * Constructor for NodesDataFusionInfoRequest with specific node IDs.
     * @param nodeIds The node IDs to query.
     */
    public NodesDataFusionInfoRequest(String... nodeIds) {
        super(nodeIds);
    }

    /**
     * Constructor for NodesDataFusionInfoRequest from stream input.
     * @param in The stream input.
     * @throws IOException If an I/O error occurs.
     */
    public NodesDataFusionInfoRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Writes the request to the stream output.
     * @param out The stream output.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }


    /**
     * Node-level request for DataFusion information
     */
    public static class NodeDataFusionInfoRequest extends org.opensearch.transport.TransportRequest {

        /**
         * Default constructor for NodeDataFusionInfoRequest.
         */
        public NodeDataFusionInfoRequest() {}

        /**
         * Constructor for NodeDataFusionInfoRequest from stream input.
         * @param in The stream input.
         * @throws IOException If an I/O error occurs.
         */
        public NodeDataFusionInfoRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
