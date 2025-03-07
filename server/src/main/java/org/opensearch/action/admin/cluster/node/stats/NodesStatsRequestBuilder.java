/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.support.nodes.NodesOperationRequestBuilder;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Transport builder for obtaining OpenSearch Node Stats
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodesStatsRequestBuilder extends NodesOperationRequestBuilder<
    NodesStatsRequest,
    NodesStatsResponse,
    NodesStatsRequestBuilder> {

    public NodesStatsRequestBuilder(OpenSearchClient client, NodesStatsAction action) {
        super(client, action, new NodesStatsRequest());
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Clears all stats flags.
     */
    public NodesStatsRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Add a single metric to the request.
     *
     * @param metric Name of metric as a string.
     * @return This, for request chaining.
     */
    public NodesStatsRequestBuilder addMetric(String metric) {
        request.addMetric(metric);
        return this;
    }

    /**
     * Add an array of metrics to the request.
     *
     * @param metrics Metric names as strings.
     * @return This, for request chaining.
     */
    public NodesStatsRequestBuilder addMetrics(String... metrics) {
        request.addMetrics(metrics);
        return this;
    }

    /**
     * Should the node indices stats be returned.
     */
    public NodesStatsRequestBuilder setIndices(boolean indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Should the node indices stats be returned.
     */
    public NodesStatsRequestBuilder setIndices(CommonStatsFlags indices) {
        request.indices(indices);
        return this;
    }
}
