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
import java.util.HashSet;
import java.util.Set;

/**
 * A request to get cluster level stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {

    private final Set<Metric> requestedMetrics = new HashSet<>();
    private final Set<IndexMetric> indexMetricsRequested = new HashSet<>();
    private Boolean computeAllMetrics = true;

    public ClusterStatsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_2_16_0)) {
            useAggregatedNodeLevelResponses = in.readOptionalBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_2_18_0)) {
            computeAllMetrics = in.readOptionalBoolean();
            final long longMetricsFlags = in.readLong();
            for (Metric metric : Metric.values()) {
                if ((longMetricsFlags & (1 << metric.getIndex())) != 0) {
                    requestedMetrics.add(metric);
                }
            }
            final long longIndexMetricFlags = in.readLong();
            for (IndexMetric indexMetric : IndexMetric.values()) {
                if ((longIndexMetricFlags & (1 << indexMetric.getIndex())) != 0) {
                    indexMetricsRequested.add(indexMetric);
                }
            }
        }
    }

    private Boolean useAggregatedNodeLevelResponses = false;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    public boolean useAggregatedNodeLevelResponses() {
        return useAggregatedNodeLevelResponses;
    }

    public void useAggregatedNodeLevelResponses(boolean useAggregatedNodeLevelResponses) {
        this.useAggregatedNodeLevelResponses = useAggregatedNodeLevelResponses;
    }

    public boolean computeAllMetrics() {
        return computeAllMetrics;
    }

    public void computeAllMetrics(boolean computeAllMetrics) {
        this.computeAllMetrics = computeAllMetrics;
    }

    /**
     * Add Metric
     */
    public ClusterStatsRequest addMetric(Metric metric) {
        requestedMetrics.add(metric);
        return this;
    }

    /**
     * Get the names of requested metrics
     */
    public Set<Metric> requestedMetrics() {
        return new HashSet<>(requestedMetrics);
    }

    /**
     * Add IndexMetric
     */
    public ClusterStatsRequest addIndexMetric(IndexMetric indexMetric) {
        indexMetricsRequested.add(indexMetric);
        return this;
    }

    public Set<IndexMetric> indicesMetrics() {
        return new HashSet<>(indexMetricsRequested);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_16_0)) {
            out.writeOptionalBoolean(useAggregatedNodeLevelResponses);
        }
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            out.writeOptionalBoolean(computeAllMetrics);
            long longMetricFlags = 0;
            for (Metric metric : requestedMetrics) {
                longMetricFlags |= (1 << metric.getIndex());
            }
            out.writeLong(longMetricFlags);
            long longIndexMetricFlags = 0;
            for (IndexMetric indexMetric : indexMetricsRequested) {
                longIndexMetricFlags |= (1 << indexMetric.getIndex());
            }
            out.writeLong(longIndexMetricFlags);
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the cluster stats endpoint.
     */
    @PublicApi(since = "2.18.0")
    public enum Metric {
        OS("os", 0),
        JVM("jvm", 1),
        FS("fs", 2),
        PROCESS("process", 3),
        INGEST("ingest", 4),
        PLUGINS("plugins", 5),
        NETWORK_TYPES("network_types", 6),
        DISCOVERY_TYPES("discovery_types", 7),
        PACKAGING_TYPES("packaging_types", 8),
        INDICES("indices", 9);

        private String metricName;

        private int index;

        Metric(String name, int index) {
            this.metricName = name;
            this.index = index;
        }

        public String metricName() {
            return this.metricName;
        }

        public int getIndex() {
            return index;
        }

    }

    /**
     * An enumeration of the "core" sections of indices metrics that may be requested
     * from the cluster stats endpoint.
     *
     * When no value is provided for param index_metric, default filter is set to _all.
     */
    @PublicApi(since = "2.18.0")
    public enum IndexMetric {
        // Metrics computed from ShardStats
        SHARDS("shards", 0),
        DOCS("docs", 1),
        STORE("store", 2),
        FIELDDATA("fielddata", 3),
        QUERY_CACHE("query_cache", 4),
        COMPLETION("completion", 5),
        SEGMENTS("segments", 6),
        // Metrics computed from ClusterState
        ANALYSIS("analysis", 7),
        MAPPINGS("mappings", 8);

        private String metricName;

        private int index;

        IndexMetric(String name, int index) {
            this.metricName = name;
            this.index = index;
        }

        public String metricName() {
            return this.metricName;
        }

        public int getIndex() {
            return this.index;
        }
    }
}
