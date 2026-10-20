/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/** Aggregated cluster-wide response for the clear-scoped-cache broadcast. @opensearch.internal */
public class ClearCacheNodesResponse extends BaseNodesResponse<ClearCacheNodeResponse> implements ToXContentFragment {

    public ClearCacheNodesResponse(ClusterName clusterName, List<ClearCacheNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public ClearCacheNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<ClearCacheNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ClearCacheNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClearCacheNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("acknowledged", failures() == null || failures().isEmpty());
        builder.startArray("cleared_nodes");
        for (ClearCacheNodeResponse node : getNodes()) {
            builder.value(node.getNode().getId());
        }
        builder.endArray();
        return builder;
    }
}
