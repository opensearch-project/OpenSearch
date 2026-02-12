/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Example class
 */
/** Example */
public class StreamNodesDataResponse extends BaseNodesResponse<NodeStreamDataResponse> {

    /**
     * Constructor
     */
    /** Method */
    public StreamNodesDataResponse(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor
     */
    /** Method */
    public StreamNodesDataResponse(ClusterName clusterName, List<NodeStreamDataResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeStreamDataResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeStreamDataResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeStreamDataResponse> nodes) throws IOException {
        out.writeList(nodes);
    }
}
