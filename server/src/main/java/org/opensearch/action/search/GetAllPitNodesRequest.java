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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to get all active PIT IDs from all nodes of cluster
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public class GetAllPitNodesRequest extends BaseNodesRequest<GetAllPitNodesRequest> {

    @Inject
    public GetAllPitNodesRequest(DiscoveryNode... concreteNodes) {
        super(false, concreteNodes);
    }

    public GetAllPitNodesRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
