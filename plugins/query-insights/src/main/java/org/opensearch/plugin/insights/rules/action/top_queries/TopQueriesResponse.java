/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transport response for cluster/node level top queries information.
 *
 * @opensearch.internal
 */
@PublicApi(since = "1.0.0")
public class TopQueriesResponse extends BaseNodesResponse<TopQueries> implements ToXContentFragment {

    private static final String CLUSTER_LEVEL_RESULTS_KEY = "top_queries";

    public TopQueriesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public TopQueriesResponse(ClusterName clusterName, List<TopQueries> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<TopQueries> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(TopQueries::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<TopQueries> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        List<TopQueries> results = getNodes();
        builder.startObject();
        toClusterLevelResult(builder, params, results);
        return builder.endObject();
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            this.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    /**
     * Merge top n queries results from nodes into cluster level results in XContent format.
     *
     * @param builder XContent builder
     * @param params serialization parameters
     * @param results top queries results from all nodes
     * @throws IOException if an error occurs
     */
    private void toClusterLevelResult(XContentBuilder builder, Params params, List<TopQueries> results) throws IOException {
        List<SearchQueryLatencyRecord> all_records = results.stream()
            .map(TopQueries::getLatencyRecords)
            .flatMap(Collection::stream)
            .sorted(Collections.reverseOrder())
            .collect(Collectors.toList());
        builder.startArray(CLUSTER_LEVEL_RESULTS_KEY);
        for (SearchQueryLatencyRecord record : all_records) {
            record.toXContent(builder, params);
        }
        builder.endArray();
    }

    /**
     * build node level top n queries results in XContent format.
     *
     * @param builder XContent builder
     * @param params serialization parameters
     * @param results top queries results from all nodes
     * @throws IOException if an error occurs
     */
    private void toNodeLevelResult(XContentBuilder builder, Params params, List<TopQueries> results) throws IOException {
        builder.startObject(CLUSTER_LEVEL_RESULTS_KEY);
        for (TopQueries topQueries : results) {
            builder.startArray(topQueries.getNode().getId());
            topQueries.toXContent(builder, params);
            builder.endArray();
        }
        builder.endObject();
    }
}
