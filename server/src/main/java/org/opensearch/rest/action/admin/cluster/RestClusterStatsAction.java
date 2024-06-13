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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get cluster stats
 *
 * @opensearch.api
 */
public class RestClusterStatsAction extends BaseRestHandler {

    /**
     * Setting up the response in the form of levels - for experimental testings -
     *          level 1 excludes Mapping and Analysis Stats.
     *
     * This will determine the default behavior of the cluster stats call.
     */
    public static final String CLUSTER_STATS_RESPONSE_LEVEL = "opensearch.experimental.optimization.cluster_stats.response_level";

    public static final Setting<Integer> CLUSTER_STATS_LEVEL_SETTING = Setting.intSetting(
        CLUSTER_STATS_RESPONSE_LEVEL,
        2,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public volatile boolean defaultMappingAndAnalysisStats;

    public RestClusterStatsAction(Settings settings, ClusterSettings clusterSettings) {
        updateMappingAndAnalysisStatsFromLevel(CLUSTER_STATS_LEVEL_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_STATS_LEVEL_SETTING, this::updateMappingAndAnalysisStatsFromLevel);
    }

    private void updateMappingAndAnalysisStatsFromLevel(int level) {
        switch (level) {
            case 1:
                this.defaultMappingAndAnalysisStats = false;
                break;
            case 2:
                this.defaultMappingAndAnalysisStats = true;
                break;
        }
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cluster/stats"), new Route(GET, "/_cluster/stats/nodes/{nodeId}")));
    }

    @Override
    public String getName() {
        return "cluster_stats_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterStatsRequest clusterStatsRequest = new ClusterStatsRequest().nodesIds(request.paramAsStringArray("nodeId", null));
        clusterStatsRequest.timeout(request.param("timeout"));

        clusterStatsRequest.setIncludeMappingStats(request.paramAsBoolean("include_mapping_stats", defaultMappingAndAnalysisStats));
        clusterStatsRequest.setIncludeAnalysisStats(request.paramAsBoolean("include_analysis_stats", defaultMappingAndAnalysisStats));

        return channel -> client.admin().cluster().clusterStats(clusterStatsRequest, new NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
