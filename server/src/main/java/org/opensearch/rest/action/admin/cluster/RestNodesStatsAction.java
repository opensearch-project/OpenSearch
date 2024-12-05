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

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.cache.CacheType;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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
 * Transport action to get nodes stats
 *
 * @opensearch.api
 */
public class RestNodesStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_nodes/stats"),
                new Route(GET, "/_nodes/{nodeId}/stats"),
                new Route(GET, "/_nodes/stats/{metric}"),
                new Route(GET, "/_nodes/{nodeId}/stats/{metric}"),
                new Route(GET, "/_nodes/stats/{metric}/{index_metric}"),
                new Route(GET, "/_nodes/{nodeId}/stats/{metric}/{index_metric}")
            )
        );
    }

    static final Map<String, Consumer<NodesStatsRequest>> METRICS;

    static {
        Map<String, Consumer<NodesStatsRequest>> map = new HashMap<>();
        for (NodesStatsRequest.Metric metric : NodesStatsRequest.Metric.values()) {
            map.put(metric.metricName(), request -> request.addMetric(metric.metricName()));
        }
        map.put("indices", request -> request.indices(true));
        METRICS = Collections.unmodifiableMap(map);
    }

    static final Map<String, Consumer<CommonStatsFlags>> FLAGS;

    static {
        final Map<String, Consumer<CommonStatsFlags>> flags = new HashMap<>();
        for (final Flag flag : CommonStatsFlags.Flag.values()) {
            flags.put(flag.getRestName(), f -> f.set(flag, true));
        }
        FLAGS = Collections.unmodifiableMap(flags);
    }

    @Override
    public String getName() {
        return "nodes_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> metrics = Strings.tokenizeByCommaToSet(request.param("metric", "_all"));

        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodesIds);
        nodesStatsRequest.timeout(request.param("timeout"));

        if (metrics.size() == 1 && metrics.contains("_all")) {
            if (request.hasParam("index_metric")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains index metrics [%s] but all stats requested",
                        request.path(),
                        request.param("index_metric")
                    )
                );
            }
            nodesStatsRequest.all();
            nodesStatsRequest.indices(CommonStatsFlags.ALL);
        } else if (metrics.contains("_all")) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "request [%s] contains _all and individual metrics [%s]",
                    request.path(),
                    request.param("metric")
                )
            );
        } else {
            nodesStatsRequest.clear();

            // use a sorted set so the unrecognized parameters appear in a reliable sorted order
            final Set<String> invalidMetrics = new TreeSet<>();
            for (final String metric : metrics) {
                final Consumer<NodesStatsRequest> handler = METRICS.get(metric);
                if (handler != null) {
                    handler.accept(nodesStatsRequest);
                } else {
                    invalidMetrics.add(metric);
                }
            }

            if (!invalidMetrics.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidMetrics, METRICS.keySet(), "metric"));
            }

            // check for index specific metrics
            if (metrics.contains("indices")) {
                Set<String> indexMetrics = Strings.tokenizeByCommaToSet(request.param("index_metric", "_all"));
                if (indexMetrics.size() == 1 && indexMetrics.contains("_all")) {
                    nodesStatsRequest.indices(CommonStatsFlags.ALL);
                } else {
                    CommonStatsFlags flags = new CommonStatsFlags();
                    flags.clear();
                    // use a sorted set so the unrecognized parameters appear in a reliable sorted order
                    final Set<String> invalidIndexMetrics = new TreeSet<>();
                    for (final String indexMetric : indexMetrics) {
                        final Consumer<CommonStatsFlags> handler = FLAGS.get(indexMetric);
                        if (handler != null) {
                            handler.accept(flags);
                        } else {
                            invalidIndexMetrics.add(indexMetric);
                        }
                    }

                    if (!invalidIndexMetrics.isEmpty()) {
                        throw new IllegalArgumentException(unrecognized(request, invalidIndexMetrics, FLAGS.keySet(), "index metric"));
                    }

                    nodesStatsRequest.indices(flags);
                }
            } else if (metrics.contains("caches")) {
                // Extract the list of caches we want to get stats for from the submetrics (which we get from index_metric)
                Set<String> cacheMetrics = Strings.tokenizeByCommaToSet(request.param("index_metric", "_all"));
                CommonStatsFlags cacheFlags = new CommonStatsFlags();
                cacheFlags.clear();
                if (cacheMetrics.contains("_all")) {
                    cacheFlags.includeAllCacheTypes();
                } else {
                    for (String cacheName : cacheMetrics) {
                        try {
                            cacheFlags.includeCacheType(CacheType.getByValue(cacheName));
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                unrecognized(request, Set.of(cacheName), CacheType.allValues(), "cache type")
                            );
                        }
                    }
                }
                nodesStatsRequest.indices(cacheFlags);
            } else if (request.hasParam("index_metric")) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "request [%s] contains index metrics [%s] but indices stats not requested",
                        request.path(),
                        request.param("index_metric")
                    )
                );
            }
        }

        if (nodesStatsRequest.indices().isSet(Flag.FieldData) && (request.hasParam("fields") || request.hasParam("fielddata_fields"))) {
            nodesStatsRequest.indices()
                .fieldDataFields(request.paramAsStringArray("fielddata_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Completion) && (request.hasParam("fields") || request.hasParam("completion_fields"))) {
            nodesStatsRequest.indices()
                .completionDataFields(request.paramAsStringArray("completion_fields", request.paramAsStringArray("fields", null)));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Search) && (request.hasParam("groups"))) {
            nodesStatsRequest.indices().groups(request.paramAsStringArray("groups", null));
        }
        if (nodesStatsRequest.indices().isSet(Flag.Segments)) {
            nodesStatsRequest.indices().includeSegmentFileSizes(request.paramAsBoolean("include_segment_file_sizes", false));
        }
        if (request.hasParam("include_all")) {
            nodesStatsRequest.indices().includeAllShardIndexingPressureTrackers(request.paramAsBoolean("include_all", false));
        }

        if (request.hasParam("top")) {
            nodesStatsRequest.indices().includeOnlyTopIndexingPressureMetrics(request.paramAsBoolean("top", false));
        }

        // If no levels are passed in this results in an empty array.
        String[] levels = Strings.splitStringByCommaToArray(request.param("level"));
        nodesStatsRequest.indices().setLevels(levels);
        nodesStatsRequest.indices().setIncludeIndicesStatsByLevel(true);

        return channel -> client.admin().cluster().nodesStats(nodesStatsRequest, new NodesResponseRestListener<>(channel));
    }

    private final Set<String> RESPONSE_PARAMS = Collections.singleton("level");

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
