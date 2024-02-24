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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Transport response for cluster/node level top queries information.
 *
 * @opensearch.internal
 */
public class SearchMetadataResponse extends BaseNodesResponse<SearchMetadata> implements ToXContentObject {
    /**
     * Constructor for TopQueriesResponse.
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public SearchMetadataResponse(final StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor for TopQueriesResponse
     *
     * @param clusterName The current cluster name
     * @param nodes A list that contains top queries results from all nodes
     * @param failures A list that contains FailedNodeException
     */
    public SearchMetadataResponse(
        final ClusterName clusterName,
        final List<SearchMetadata> nodes,
        final List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<SearchMetadata> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(SearchMetadata::new);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<SearchMetadata> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        final List<SearchMetadata> results = getNodes();
        builder.startObject();
        builder.startArray();
        for (SearchMetadata result : results) {
            result.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

    @Override
    public String toString() {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            this.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
