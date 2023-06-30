/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.support.nodes.ProtobufNodesOperationRequestBuilder;
import org.opensearch.client.ProtobufOpenSearchClient;

/**
 * Transport builder for obtaining OpenSearch Node Stats
*
* @opensearch.internal
*/
public class ProtobufNodesStatsRequestBuilder extends ProtobufNodesOperationRequestBuilder<
    ProtobufNodesStatsRequest,
    ProtobufNodesStatsResponse,
    ProtobufNodesStatsRequestBuilder> {

    public ProtobufNodesStatsRequestBuilder(ProtobufOpenSearchClient client, ProtobufNodesStatsAction action) {
        super(client, action, new ProtobufNodesStatsRequest());
    }

    /**
     * Sets all the request flags.
    */
    public ProtobufNodesStatsRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Clears all stats flags.
    */
    public ProtobufNodesStatsRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Add a single metric to the request.
    *
    * @param metric Name of metric as a string.
    * @return This, for request chaining.
    */
    public ProtobufNodesStatsRequestBuilder addMetric(String metric) {
        request.addMetric(metric);
        return this;
    }

    /**
     * Add an array of metrics to the request.
    *
    * @param metrics Metric names as strings.
    * @return This, for request chaining.
    */
    public ProtobufNodesStatsRequestBuilder addMetrics(String... metrics) {
        request.addMetrics(metrics);
        return this;
    }

    /**
     * Should the node indices stats be returned.
    */
    public ProtobufNodesStatsRequestBuilder setIndices(boolean indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Should the node indices stats be returned.
    */
    public ProtobufNodesStatsRequestBuilder setIndices(CommonStatsFlags indices) {
        request.indices(indices);
        return this;
    }
}
