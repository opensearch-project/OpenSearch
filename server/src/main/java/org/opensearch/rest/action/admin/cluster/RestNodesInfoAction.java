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

import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get node info
 *
 * @opensearch.api
 */
public class RestNodesInfoAction extends BaseRestHandler {
    static final Set<String> ALLOWED_METRICS = NodesInfoRequest.Metric.allMetrics();

    private final SettingsFilter settingsFilter;

    public RestNodesInfoAction(SettingsFilter settingsFilter) {
        this.settingsFilter = settingsFilter;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_nodes"),
                // this endpoint is used for metrics, not for node IDs, like /_nodes/fs
                new Route(GET, "/_nodes/{nodeId}"),
                new Route(GET, "/_nodes/{nodeId}/{metrics}"),
                // added this endpoint to be aligned with stats
                new Route(GET, "/_nodes/{nodeId}/info/{metrics}")
            )
        );
    }

    @Override
    public String getName() {
        return "nodes_info_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final NodesInfoRequest nodesInfoRequest = prepareRequest(request);
        nodesInfoRequest.timeout(request.param("timeout"));
        settingsFilter.addFilterSettingParams(request);
        return channel -> client.admin().cluster().nodesInfo(nodesInfoRequest, new NodesResponseRestListener<>(channel));
    }

    static NodesInfoRequest prepareRequest(final RestRequest request) {
        String[] nodeIds;
        Set<String> metrics;

        // special case like /_nodes/os (in this case os are metrics and not the nodeId)
        // still, /_nodes/_local (or any other node id) should work and be treated as usual
        // this means one must differentiate between allowed metrics and arbitrary node ids in the same place
        if (request.hasParam("nodeId") && !request.hasParam("metrics")) {
            String nodeId = request.param("nodeId", "_all");
            Set<String> metricsOrNodeIds = Strings.tokenizeByCommaToSet(nodeId);
            boolean isMetricsOnly = ALLOWED_METRICS.containsAll(metricsOrNodeIds);
            if (isMetricsOnly) {
                nodeIds = new String[] { "_all" };
                metrics = metricsOrNodeIds;
            } else {
                nodeIds = Strings.tokenizeToStringArray(nodeId, ",");
                metrics = Sets.newHashSet("_all");
            }
        } else {
            nodeIds = Strings.tokenizeToStringArray(request.param("nodeId", "_all"), ",");
            metrics = Strings.tokenizeByCommaToSet(request.param("metrics", "_all"));
        }

        final NodesInfoRequest nodesInfoRequest = new NodesInfoRequest(nodeIds);
        nodesInfoRequest.timeout(request.param("timeout"));
        // shortcut, don't do checks if only all is specified
        if (metrics.size() == 1 && metrics.contains("_all")) {
            nodesInfoRequest.all();
        } else {
            nodesInfoRequest.clear();
            // disregard unknown metrics
            metrics.retainAll(ALLOWED_METRICS);
            nodesInfoRequest.addMetrics(metrics.stream().toArray(String[]::new));
        }
        return nodesInfoRequest;
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
