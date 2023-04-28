/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.filecache.clear;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * The response of a clear filecache action aggregated across nodes.
 *
 * @opensearch.internal
 */
public class ClearNodesFileCacheResponse extends BaseNodesResponse<ClearNodeFileCacheResponse> implements ToXContentFragment {

    ClearNodesFileCacheResponse(StreamInput in) throws IOException {
        super(in);
    }

    ClearNodesFileCacheResponse(
        ClusterName clusterName,
        List<ClearNodeFileCacheResponse> clearNodeFileCachResponses,
        List<FailedNodeException> failures
    ) {
        super(clusterName, clearNodeFileCachResponses, failures);
    }

    @Override
    protected List<ClearNodeFileCacheResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ClearNodeFileCacheResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClearNodeFileCacheResponse> clearNodeFileCacheResponse) throws IOException {
        out.writeList(clearNodeFileCacheResponse);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (ClearNodeFileCacheResponse clearNodeFileCacheResponse : getNodes()) {
            builder.startObject(clearNodeFileCacheResponse.getNode().getId());
            clearNodeFileCacheResponse.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}
