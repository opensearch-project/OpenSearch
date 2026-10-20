/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response for per-format node-level stats.
 *
 * @param <T> concrete shard-stats type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatNodeStatsResponse<T extends DataFormatShardStats<T>> extends BaseNodesResponse<NodeFormatStats<T>>
    implements
        ToXContentObject {

    private final Writeable.Reader<T> reader;

    public FormatNodeStatsResponse(ClusterName clusterName, List<NodeFormatStats<T>> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        this.reader = null;
    }

    public FormatNodeStatsResponse(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        super(in);
        this.reader = reader;
    }

    @Override
    protected List<NodeFormatStats<T>> readNodesFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<NodeFormatStats<T>> nodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            nodes.add(new NodeFormatStats<>(in, reader));
        }
        return nodes;
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeFormatStats<T>> nodes) throws IOException {
        out.writeVInt(nodes.size());
        for (NodeFormatStats<T> node : nodes) {
            node.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("nodes");
        for (NodeFormatStats<T> nodeStats : getNodes()) {
            builder.startObject(nodeStats.getNode().getId());
            builder.field("name", nodeStats.getNode().getName());
            nodeStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
