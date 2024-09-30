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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get cluster stats
 *
 * @opensearch.api
 */
public class RestClusterStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_cluster/stats"),
                new Route(GET, "/_cluster/stats/nodes/{nodeId}"),
                new Route(GET, "/_cluster/stats/{metric}/nodes/{nodeId}"),
                new Route(GET, "/_cluster/stats/{metric}/{index_metric}/nodes/{nodeId}")
            )
        );
    }

    static final Map<String, Consumer<ClusterStatsRequest>> INDEX_METRIC_TO_REQUEST_CONSUMER_MAP;

    static final Map<String, Consumer<ClusterStatsRequest>> METRIC_REQUEST_CONSUMER_MAP;

    static {
        Map<String, Consumer<ClusterStatsRequest>> metricRequestConsumerMap = new HashMap<>();
        for (ClusterStatsRequest.Metric metric : ClusterStatsRequest.Metric.values()) {
            metricRequestConsumerMap.put(metric.metricName(), request -> request.addMetric(metric));
        }
        METRIC_REQUEST_CONSUMER_MAP = Collections.unmodifiableMap(metricRequestConsumerMap);
    }

    static {
        Map<String, Consumer<ClusterStatsRequest>> metricMap = new HashMap<>();
        for (ClusterStatsRequest.IndexMetric indexMetric : ClusterStatsRequest.IndexMetric.values()) {
            metricMap.put(indexMetric.metricName(), request -> request.addIndexMetric(indexMetric));
        }
        INDEX_METRIC_TO_REQUEST_CONSUMER_MAP = Collections.unmodifiableMap(metricMap);
    }

    @Override
    public String getName() {
        return "cluster_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterStatsRequest clusterStatsRequest = fromRequest(request);
        return channel -> client.admin().cluster().clusterStats(clusterStatsRequest, new NodesResponseRestListener<>(channel));
    }

    public static ClusterStatsRequest fromRequest(final RestRequest request) {
        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));
        String[] nodeIds = request.paramAsStringArray("nodeId", null);

        ClusterStatsRequest clusterStatsRequest = new ClusterStatsRequest().nodesIds(nodeIds);
        clusterStatsRequest.timeout(request.param("timeout"));
        clusterStatsRequest.useAggregatedNodeLevelResponses(true);

        if (!metrics.isEmpty()) {
            if (metrics.size() > 1 && metrics.contains("_all")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains _all and individual metrics [%s]",
                        request.path(),
                        request.param("metric")
                    )
                );
            }

            final Set<String> metricsRequested = new HashSet<>();
            if (metrics.contains("_all")) {
                metricsRequested.addAll(METRIC_REQUEST_CONSUMER_MAP.keySet());
            } else {
                metricsRequested.addAll(metrics);
            }

            Set<String> indexMetrics = Strings.tokenizeByCommaToSet(
                request.param("index_metric", metricsRequested.contains(ClusterStatsRequest.Metric.INDICES.metricName()) ? "_all" : null)
            );

            if (indexMetrics.size() > 1 && indexMetrics.contains("_all")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains _all and individual index metrics [%s]",
                        request.path(),
                        request.param("index_metric")
                    )
                );
            }

            final Set<String> invalidMetrics = new TreeSet<>();
            for (String metric : metricsRequested) {
                Consumer<ClusterStatsRequest> clusterStatsRequestConsumer = METRIC_REQUEST_CONSUMER_MAP.get(metric);
                if (clusterStatsRequestConsumer != null) {
                    clusterStatsRequestConsumer.accept(clusterStatsRequest);
                } else {
                    invalidMetrics.add(metric);
                }
            }
            if (!invalidMetrics.isEmpty()) {
                throw new IllegalArgumentException(
                    unrecognizedStrings(request, invalidMetrics, METRIC_REQUEST_CONSUMER_MAP.keySet(), "metric")
                );
            }

            if (!metricsRequested.contains(ClusterStatsRequest.Metric.INDICES.metricName()) && !indexMetrics.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains index metrics [%s] but indices stats not requested",
                        request.path(),
                        request.param("index_metric")
                    )
                );
            }

            if (metricsRequested.contains(ClusterStatsRequest.Metric.INDICES.metricName())) {
                final Set<String> indexMetricsRequested = new HashSet<>();
                if (indexMetrics.contains("_all")) {
                    indexMetricsRequested.addAll(INDEX_METRIC_TO_REQUEST_CONSUMER_MAP.keySet());
                } else {
                    indexMetricsRequested.addAll(indexMetrics);
                }
                final Set<String> invalidIndexMetrics = new TreeSet<>();
                for (String indexMetric : indexMetricsRequested) {
                    Consumer<ClusterStatsRequest> clusterStatsRequestConsumer = INDEX_METRIC_TO_REQUEST_CONSUMER_MAP.get(indexMetric);
                    if (clusterStatsRequestConsumer != null) {
                        clusterStatsRequestConsumer.accept(clusterStatsRequest);
                    } else {
                        invalidIndexMetrics.add(indexMetric);
                    }
                }

                if (!invalidIndexMetrics.isEmpty()) {
                    throw new IllegalArgumentException(
                        unrecognizedStrings(request, invalidIndexMetrics, INDEX_METRIC_TO_REQUEST_CONSUMER_MAP.keySet(), "index metric")
                    );
                }
            }
            clusterStatsRequest.computeAllMetrics(false);
        }
        return clusterStatsRequest;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
