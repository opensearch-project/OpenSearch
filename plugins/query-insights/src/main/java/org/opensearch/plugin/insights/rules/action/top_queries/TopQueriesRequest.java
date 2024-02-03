/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.MetricType;

import java.io.IOException;

/**
 * A request to get cluster/node level top queries information.
 *
 * @opensearch.internal
 */
public class TopQueriesRequest extends BaseNodesRequest<TopQueriesRequest> {

    final MetricType metricType;

    /**
     * Constructor for TopQueriesRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public TopQueriesRequest(final StreamInput in) throws IOException {
        super(in);
        this.metricType = MetricType.readFromStream(in);
    }

    /**
     * Get top queries from nodes based on the nodes ids specified.
     * If none are passed, cluster level top queries will be returned.
     *
     * @param metricType {@link MetricType}
     * @param nodesIds the nodeIds specified in the request
     */
    public TopQueriesRequest(final MetricType metricType, final String... nodesIds) {
        super(nodesIds);
        this.metricType = metricType;
    }

    /**
     * Get the type of requested metrics
     */
    public MetricType getMetricType() {
        return metricType;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(metricType.toString());
    }
}
