/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.support.nodes.ProtobufBaseNodesRequest;
import org.opensearch.server.proto.NodesStatsRequestProto.NodesStatsRequest;
import org.opensearch.server.proto.NodesStatsRequestProto;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level stats.
*
* @opensearch.internal
*/
public class ProtobufNodesStatsRequest extends ProtobufBaseNodesRequest<ProtobufNodesStatsRequest> {

    private CommonStatsFlags indices = new CommonStatsFlags();
    private final Set<String> requestedMetrics = new HashSet<>();
    private NodesStatsRequestProto.NodesStatsRequest nodesStatsRequest;

    public ProtobufNodesStatsRequest() {
        super((String[]) null);
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
    * for all nodes will be returned.
    */
    public ProtobufNodesStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Sets all the request flags.
    */
    public ProtobufNodesStatsRequest all() {
        this.indices.all();
        this.requestedMetrics.addAll(Metric.allMetrics());
        return this;
    }

    /**
     * Clears all the request flags.
    */
    public ProtobufNodesStatsRequest clear() {
        this.indices.clear();
        this.requestedMetrics.clear();
        return this;
    }

    /**
     * Get indices. Handles separately from other metrics because it may or
    * may not have submetrics.
    * @return flags indicating which indices stats to return
    */
    public CommonStatsFlags indices() {
        return indices;
    }

    /**
     * Set indices. Handles separately from other metrics because it may or
    * may not involve submetrics.
    * @param indices flags indicating which indices stats to return
    * @return This object, for request chaining.
    */
    public ProtobufNodesStatsRequest indices(CommonStatsFlags indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Should indices stats be returned.
    */
    public ProtobufNodesStatsRequest indices(boolean indices) {
        if (indices) {
            this.indices.all();
        } else {
            this.indices.clear();
        }
        return this;
    }

    /**
     * Get the names of requested metrics, excluding indices, which are
    * handled separately.
    */
    public Set<String> requestedMetrics() {
        return new HashSet<>(this.nodesStatsRequest.getRequestedMetricsList());
    }

    /**
     * Add metric
    */
    public ProtobufNodesStatsRequest addMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.add(metric);
        return this;
    }

    /**
     * Add an array of metric names
    */
    public ProtobufNodesStatsRequest addMetrics(String... metrics) {
        // use sorted set for reliable ordering in error messages
        SortedSet<String> metricsSet = new TreeSet<>(Arrays.asList(metrics));
        if (Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        this.nodesStatsRequest = NodesStatsRequestProto.NodesStatsRequest.newBuilder().addAllRequestedMetrics(metricsSet).build();
        requestedMetrics.addAll(metricsSet);
        return this;
    }

    /**
     * Remove metric
    */
    public ProtobufNodesStatsRequest removeMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.remove(metric);
        return this;
    }

    public ProtobufNodesStatsRequest(byte[] data) throws IOException {
        super(data);
        this.nodesStatsRequest = NodesStatsRequestProto.NodesStatsRequest.parseFrom(data);
    }

    public ProtobufNodesStatsRequest(NodesStatsRequestProto.NodesStatsRequest nodesStatsRequest) throws IOException {
        super(nodesStatsRequest.toByteArray());
        this.nodesStatsRequest = nodesStatsRequest;
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
    * from the nodes stats endpoint. Eventually this list will be pluggable.
    */
    public enum Metric {
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("thread_pool"),
        FS("fs"),
        TRANSPORT("transport"),
        HTTP("http"),
        BREAKER("breaker"),
        SCRIPT("script"),
        DISCOVERY("discovery"),
        INGEST("ingest"),
        ADAPTIVE_SELECTION("adaptive_selection"),
        SCRIPT_CACHE("script_cache"),
        INDEXING_PRESSURE("indexing_pressure"),
        SHARD_INDEXING_PRESSURE("shard_indexing_pressure"),
        SEARCH_BACKPRESSURE("search_backpressure"),
        CLUSTER_MANAGER_THROTTLING("cluster_manager_throttling"),
        WEIGHTED_ROUTING_STATS("weighted_routing"),
        FILE_CACHE_STATS("file_cache");

        private String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
            return this.metricName;
        }

        boolean containedIn(Set<String> metricNames) {
            return metricNames.contains(this.metricName());
        }

        static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        this.nodesStatsRequest.writeTo(out);
    }

    public NodesStatsRequest request() {
        return this.nodesStatsRequest;
    }
}
