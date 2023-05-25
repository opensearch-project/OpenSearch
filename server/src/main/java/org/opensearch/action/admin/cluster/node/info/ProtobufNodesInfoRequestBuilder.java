/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.action.support.nodes.ProtobufNodesOperationRequestBuilder;
import org.opensearch.client.ProtobufOpenSearchClient;

/**
 * Transport action for OpenSearch Node Information
*
* @opensearch.internal
*/
public class ProtobufNodesInfoRequestBuilder extends ProtobufNodesOperationRequestBuilder<
    ProtobufNodesInfoRequest,
    ProtobufNodesInfoResponse,
    ProtobufNodesInfoRequestBuilder> {

    public ProtobufNodesInfoRequestBuilder(ProtobufOpenSearchClient client, ProtobufNodesInfoAction action) {
        super(client, action, new ProtobufNodesInfoRequest());
    }

    /**
     * Clears all info flags.
    */
    public ProtobufNodesInfoRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Sets to return all the data.
    */
    public ProtobufNodesInfoRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Add a single metric to the request.
    *
    * @param metric Name of metric as a string.
    * @return This, for request chaining.
    */
    public ProtobufNodesInfoRequestBuilder addMetric(String metric) {
        request.addMetric(metric);
        return this;
    }

    /**
     * Add an array of metrics to the request.
    *
    * @param metrics Metric names as strings.
    * @return This, for request chaining.
    */
    public ProtobufNodesInfoRequestBuilder addMetrics(String... metrics) {
        request.addMetrics(metrics);
        return this;
    }
}
