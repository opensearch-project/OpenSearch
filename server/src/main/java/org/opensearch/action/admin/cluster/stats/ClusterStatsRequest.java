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

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A request to get cluster level stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {

    private final Set<String> requestedMetrics = new HashSet<>();
    private final Set<String> indexMetricsRequested = new HashSet<>();
    private Boolean applyMetricFiltering = false;

    public ClusterStatsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_2_16_0)) {
            useAggregatedNodeLevelResponses = in.readOptionalBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            applyMetricFiltering = in.readOptionalBoolean();
            requestedMetrics.addAll(in.readStringList());
            indexMetricsRequested.addAll(in.readStringList());
        }
    }

    private Boolean useAggregatedNodeLevelResponses = false;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        super(false, nodesIds);
    }

    public boolean useAggregatedNodeLevelResponses() {
        return useAggregatedNodeLevelResponses;
    }

    public void useAggregatedNodeLevelResponses(boolean useAggregatedNodeLevelResponses) {
        this.useAggregatedNodeLevelResponses = useAggregatedNodeLevelResponses;
    }

    public boolean applyMetricFiltering() {
        return applyMetricFiltering;
    }

    public void applyMetricFiltering(boolean honourMetricFiltering) {
        this.applyMetricFiltering = honourMetricFiltering;
    }

    /**
     * Add Metric
     */
    public ClusterStatsRequest addMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal Metric: " + metric);
        }
        requestedMetrics.add(metric);
        return this;
    }

    /**
     * Get the names of requested metrics
     */
    public Set<String> requestedMetrics() {
        return new HashSet<>(requestedMetrics);
    }

    /**
     * Add IndexMetric
     */
    public ClusterStatsRequest addIndexMetric(String indexMetric) {
        if (IndexMetrics.allIndicesMetrics().contains(indexMetric) == false) {
            throw new IllegalStateException("Used an illegal index metric: " + indexMetric);
        }
        indexMetricsRequested.add(indexMetric);
        return this;
    }

    public Set<String> indicesMetrics() {
        return new HashSet<>(indexMetricsRequested);
    }

    public void clearRequestedMetrics() {
        requestedMetrics.clear();
    }

    public void clearIndicesMetrics() {
        indexMetricsRequested.clear();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_16_0)) {
            out.writeOptionalBoolean(useAggregatedNodeLevelResponses);
        }
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeOptionalBoolean(applyMetricFiltering);
            out.writeStringArray(requestedMetrics.toArray(new String[0]));
            out.writeStringArray(indexMetricsRequested.toArray(new String[0]));
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the cluster stats endpoint.
     */
    @PublicApi(since = "3.0.0")
    public enum Metric {
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        FS("fs"),
        PLUGINS("plugins"),
        INGEST("ingest"),
        NETWORK_TYPES("network_types"),
        DISCOVERY_TYPES("discovery_types"),
        PACKAGING_TYPES("packaging_types"),
        INDICES("indices");

        private String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }

        public static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }

        public boolean containedIn(Set<String> metricNames) {
            return metricNames.contains(this.metricName());
        }

    }

    /**
     * An enumeration of the "core" sections of indices metrics that may be requested
     * from the cluster stats endpoint.
     */
    @PublicApi(since = "3.0.0")
    public enum IndexMetrics {
        SHARDS("shards"),
        DOCS("docs"),
        STORE("store"),
        FIELDDATA("fielddata"),
        QUERY_CACHE("query_cache"),
        COMPLETION("completion"),
        SEGMENTS("segments"),
        ANALYSIS("analysis"),
        MAPPINGS("mappings");

        private String metricName;

        IndexMetrics(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }

        public boolean containedIn(Set<String> metricNames) {
            return metricNames.contains(this.metricName());
        }

        public static Set<String> allIndicesMetrics() {
            return Arrays.stream(values()).map(IndexMetrics::metricName).collect(Collectors.toSet());
        }
    }
}
