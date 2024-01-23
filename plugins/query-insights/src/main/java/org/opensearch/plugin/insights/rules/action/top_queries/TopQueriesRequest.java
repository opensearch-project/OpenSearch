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

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A request to get cluster/node level top queries information.
 *
 * @opensearch.internal
 */
@PublicApi(since = "1.0.0")
public class TopQueriesRequest extends BaseNodesRequest<TopQueriesRequest> {

    Metric metricType = Metric.LATENCY;

    /**
     * Constructor for TopQueriesRequest
     *
     * @param in A {@link StreamInput} object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public TopQueriesRequest(StreamInput in) throws IOException {
        super(in);
        setMetricType(in.readString());
    }

    /**
     * Get top queries from nodes based on the nodes ids specified.
     * If none are passed, cluster level top queries will be returned.
     *
     * @param nodesIds the nodeIds specified in the request
     */
    public TopQueriesRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Get the type of requested metrics
     */
    public Metric getMetricType() {
        return metricType;
    }

    /**
     * Validate and set the metric type of requested metrics
     *
     * @param metricType the metric type to set to
     * @return The current TopQueriesRequest
     */
    public TopQueriesRequest setMetricType(String metricType) {
        metricType = metricType.toUpperCase(Locale.ROOT);
        if (false == Metric.allMetrics().contains(metricType)) {
            throw new IllegalStateException("Invalid metric used in top queries request: " + metricType);
        }
        this.metricType = Metric.valueOf(metricType);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(metricType.metricName());
    }

    /**
     * ALl supported metrics for top queries
     */
    public enum Metric {
        /**
         * Latency metric type, used by top queries by latency
         */
        LATENCY("LATENCY");

        private final String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        /**
         * Get the metric name of the Metric
         * @return the metric name
         */
        public String metricName() {
            return this.metricName;
        }

        /**
         * Get all valid metrics
         * @return A set of String that contains all valid metrics
         */
        public static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }
}
