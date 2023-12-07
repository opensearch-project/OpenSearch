/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.admin.cluster.insights.top_queries;

import org.opensearch.action.search.SearchQueryLatencyRecord;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Top Queries by resource usage / latency on a node
 * <p>
 * Mainly used in the top N queries node response workflow.
 *
 * @opensearch.internal
 */
public class TopQueries extends BaseNodeResponse implements ToXContentObject {
    /** The store to keep the top N queries with latency records */
    @Nullable
    private final List<SearchQueryLatencyRecord> latencyRecords;

    public TopQueries(StreamInput in) throws IOException {
        super(in);
        latencyRecords = in.readList(SearchQueryLatencyRecord::new);
    }

    public TopQueries(
        DiscoveryNode node,
        @Nullable List<SearchQueryLatencyRecord> latencyRecords
    ) {
        super(node);
        this.latencyRecords = latencyRecords;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (latencyRecords != null) {
            for (SearchQueryLatencyRecord record : latencyRecords) {
                record.toXContent(builder, params);
            }
        }
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (latencyRecords != null) {
            out.writeList(latencyRecords);
        }
    }

    /**
     * Get all latency records
     *
     * @return the latency records in this node response
     */
    public List<SearchQueryLatencyRecord> getLatencyRecords() {
        return latencyRecords;
    }
}
