/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugin.insights.rules.model.MetricType;

import java.io.IOException;

/**
 * A request to get cluster/node level top queries information.
 *
 * @opensearch.internal
 */
@PublicApi(since = "1.0.0")
public class TopQueriesRequest extends BaseNodesRequest<TopQueriesRequest> {

    MetricType metricType;

    /**
     * Constructor for TopQueriesRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public TopQueriesRequest(StreamInput in) throws IOException {
        super(in);
        MetricType metricType = MetricType.readFromStream(in);
        if (false == MetricType.allMetricTypes().contains(metricType)) {
            throw new IllegalStateException("Invalid metric used in top queries request: " + metricType);
        }
        this.metricType = metricType;
    }

    /**
     * Get top queries from nodes based on the nodes ids specified.
     * If none are passed, cluster level top queries will be returned.
     *
     * @param metricType {@link MetricType}
     * @param nodesIds the nodeIds specified in the request
     */
    public TopQueriesRequest(MetricType metricType, String... nodesIds) {
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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(metricType.toString());
    }
}
