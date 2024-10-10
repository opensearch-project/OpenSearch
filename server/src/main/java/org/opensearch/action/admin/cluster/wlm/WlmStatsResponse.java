/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.wlm;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.wlm.stats.QueryGroupStats;
import org.opensearch.wlm.stats.WlmStats;

import java.io.IOException;
import java.util.List;

/**
 * A response for obtaining Workload Management Stats
 */
@ExperimentalApi
public class WlmStatsResponse extends BaseNodesResponse<WlmStats> implements ToXContentFragment {

    WlmStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    WlmStatsResponse(ClusterName clusterName, List<WlmStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<WlmStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(WlmStats::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<WlmStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (WlmStats wlmStats : getNodes()) {
            builder.startObject(wlmStats.getNode().getId());
            QueryGroupStats queryGroupStats = wlmStats.getQueryGroupStats();
            queryGroupStats.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
