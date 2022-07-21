/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to get all active PIT IDs from all nodes of cluster
 */
public class GetAllPitNodesRequest extends BaseNodesRequest<GetAllPitNodesRequest> {

    boolean includeAll;

    @Inject
    public GetAllPitNodesRequest(boolean includeAll, DiscoveryNode... concreteNodes) {
        super(concreteNodes);
        this.includeAll = includeAll;

    }

    public GetAllPitNodesRequest(StreamInput in) throws IOException {
        super(in);
        this.includeAll = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(includeAll);
    }

    public boolean getIncludeAll() { return includeAll; }
}
