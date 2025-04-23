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

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level information.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NodesInfoRequest extends BaseNodesRequest<NodesInfoRequest> {

    private Set<String> requestedMetrics = Metric.defaultMetrics();

    /**
     * Create a new NodeInfoRequest from a {@link StreamInput} object.
     *
     * @param in A stream input object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public NodesInfoRequest(StreamInput in) throws IOException {
        super(in);
        requestedMetrics.clear();
        requestedMetrics.addAll(Arrays.asList(in.readStringArray()));
    }

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     */
    public NodesInfoRequest(String... nodesIds) {
        super(nodesIds);
        defaultMetrics();
    }

    /**
     * Clears all info flags.
     */
    public NodesInfoRequest clear() {
        requestedMetrics.clear();
        return this;
    }

    /**
     * Sets to return data for all the metrics.
     * See {@link Metric}
     */
    public NodesInfoRequest all() {
        requestedMetrics.addAll(Metric.allMetrics());
        return this;
    }

    /**
     * Sets to return data for default metrics only.
     * See {@link Metric}
     * See {@link Metric#defaultMetrics()}.
     */
    public NodesInfoRequest defaultMetrics() {
        requestedMetrics.addAll(Metric.defaultMetrics());
        return this;
    }

    /**
     * Get the names of requested metrics
     */
    public Set<String> requestedMetrics() {
        return new HashSet<>(requestedMetrics);
    }

    /**
     * Add metric
     */
    public NodesInfoRequest addMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.add(metric);
        return this;
    }

    /**
     * Add multiple metrics
     */
    public NodesInfoRequest addMetrics(String... metrics) {
        SortedSet<String> metricsSet = new TreeSet<>(Arrays.asList(metrics));
        if (Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        requestedMetrics.addAll(metricsSet);
        return this;
    }

    /**
     * Remove metric
     */
    public NodesInfoRequest removeMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.remove(metric);
        return this;
    }

    /**
     * Helper method for adding and removing metrics. Used when deserializing
     * a NodesInfoRequest from an ordered list of booleans.
     *
     * @param addMetric Whether or not to include a metric.
     * @param metricName Name of the metric to include or remove.
     */
    private void optionallyAddMetric(boolean addMetric, String metricName) {
        if (addMetric) {
            requestedMetrics.add(metricName);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(requestedMetrics.toArray(new String[0]));
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes information endpoint. Eventually this list will be
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

        /**
         * Return all available metrics.
         * See {@link Metric}
         */
        public static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }

        /**
         * Return "the default" set of metrics.
         * Similar to {@link #allMetrics()} except {@link Metric#SEARCH_PIPELINES} metric is not included.
         * <br>
         * The motivation to define the default set of metrics was to keep the default response
         * size at bay. Metrics that are NOT included in the default set were typically introduced later
         * and are considered to contain specific type of information that is not usually useful unless you
         * know that you really need it.
         */
        public static Set<String> defaultMetrics() {
            return allMetrics().stream().filter(metric -> !(metric.equals(SEARCH_PIPELINES.metricName()))).collect(Collectors.toSet());
        }
    }
}
