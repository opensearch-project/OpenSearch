/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.admin.cluster.insights.top_queries;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A request to get cluster/node level top queries information.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TopQueriesRequest extends BaseNodesRequest<TopQueriesRequest> {

    Metric metricType = Metric.LATENCY;

    /**
     * Create a new TopQueriesRequest from a {@link StreamInput} object.
     *
     * @param in A stream input object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public TopQueriesRequest(StreamInput in) throws IOException {
        super(in);
        setMetricType(in.readString());
    }

    /**
     * Get top queries from nodes based on the nodes ids specified.
     * If none are passed, cluster level top queries will be returned.
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
     * Set the type of requested metrics
     */
    public TopQueriesRequest setMetricType(String metricType) {
        metricType = metricType.toUpperCase();
        if (Metric.allMetrics().contains(metricType) == false) {
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
        LATENCY("LATENCY");

        private final String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }

        public static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }
}
