/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.action.support.nodes.ProtobufBaseNodesRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.server.proto.NodesInfoRequestProto.NodesInfoRequest;
import org.opensearch.server.proto.NodesInfoRequestProto;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level information.
 *
 * @opensearch.internal
 */
public class ProtobufNodesInfoRequest extends ProtobufBaseNodesRequest<ProtobufNodesInfoRequest> {

    private NodesInfoRequestProto.NodesInfoRequest nodesInfoRequest;
    private final TimeValue DEFAULT_TIMEOUT_SECS = TimeValue.timeValueSeconds(30);

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     */
    public ProtobufNodesInfoRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Get the names of requested metrics
     */
    public Set<String> requestedMetrics() {
        return new HashSet<>(this.nodesInfoRequest.getRequestedMetricsList());
    }

    /**
     * Add multiple metrics
     */
    public ProtobufNodesInfoRequest addMetrics(String timeout, String... metrics) {
        SortedSet<String> metricsSet = new TreeSet<>(Arrays.asList(metrics));
        if (Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        this.nodesInfoRequest = NodesInfoRequestProto.NodesInfoRequest.newBuilder()
            .addAllRequestedMetrics(metricsSet)
            .setTimeout(DEFAULT_TIMEOUT_SECS.toString())
            .build();
        return this;
    }

    public ProtobufNodesInfoRequest(byte[] data) throws IOException {
        super(data);
        this.nodesInfoRequest = NodesInfoRequestProto.NodesInfoRequest.parseFrom(data);
    }

    public ProtobufNodesInfoRequest(NodesInfoRequestProto.NodesInfoRequest nodesInfoRequest) throws IOException {
        super(nodesInfoRequest.toByteArray());
        this.nodesInfoRequest = nodesInfoRequest;
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes information endpoint. Eventually this list list will be
     * pluggable.
     */
    public enum Metric {
        SETTINGS("settings"),
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("thread_pool"),
        TRANSPORT("transport"),
        HTTP("http"),
        PLUGINS("plugins"),
        INGEST("ingest"),
        AGGREGATIONS("aggregations"),
        INDICES("indices"),
        SEARCH_PIPELINES("search_pipelines");

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

        public static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        this.nodesInfoRequest.writeTo(out);
    }

    public NodesInfoRequest request() {
        return this.nodesInfoRequest;
    }
}
