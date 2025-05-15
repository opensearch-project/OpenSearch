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

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.IndexMetric;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.Metric;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.gateway.GatewayService;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.node.NodeRoleSettings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.OS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class ClusterStatsIT extends OpenSearchIntegTestCase {

    private void assertCounts(ClusterStatsNodes.Counts counts, int total, Map<String, Integer> roles) {
        assertThat(counts.getTotal(), equalTo(total));
        assertThat(counts.getRoles(), equalTo(roles));
    }

    private void waitForNodes(int numNodes) {
        ClusterHealthResponse actionGet = client().admin()
            .cluster()
            .health(Requests.clusterHealthRequest().waitForEvents(Priority.LANGUID).waitForNodes(Integer.toString(numNodes)))
            .actionGet();
        assertThat(actionGet.isTimedOut(), is(false));
    }

    public void testNodeCounts() {
        int total = 1;
        internalCluster().startNode();
        Map<String, Integer> expectedCounts = getExpectedCounts(1, 1, 1, 1, 1, 0, 0, 0);
        int numNodes = randomIntBetween(1, 5);

        ClusterStatsResponse response = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertCounts(response.getNodesStats().getCounts(), total, expectedCounts);

        for (int i = 0; i < numNodes; i++) {
            boolean isDataNode = randomBoolean();
            boolean isIngestNode = randomBoolean();
            boolean isClusterManagerNode = randomBoolean();
            boolean isRemoteClusterClientNode = false;
            final Set<DiscoveryNodeRole> roles = new HashSet<>();
            if (isDataNode) {
                roles.add(DiscoveryNodeRole.DATA_ROLE);
            }
            if (isIngestNode) {
                roles.add(DiscoveryNodeRole.INGEST_ROLE);
            }
            if (isClusterManagerNode) {
                roles.add(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
            }
            if (isRemoteClusterClientNode) {
                roles.add(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
            }
            Settings settings = Settings.builder()
                .putList(
                    NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                    roles.stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toList())
                )
                .build();
            internalCluster().startNode(settings);
            total++;
            waitForNodes(total);

            if (isDataNode) {
                incrementCountForRole(DiscoveryNodeRole.DATA_ROLE.roleName(), expectedCounts);
            }
            if (isIngestNode) {
                incrementCountForRole(DiscoveryNodeRole.INGEST_ROLE.roleName(), expectedCounts);
            }
            if (isClusterManagerNode) {
                incrementCountForRole(DiscoveryNodeRole.MASTER_ROLE.roleName(), expectedCounts);
                incrementCountForRole(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName(), expectedCounts);
            }
            if (isRemoteClusterClientNode) {
                incrementCountForRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName(), expectedCounts);
            }
            if (!isDataNode && !isClusterManagerNode && !isIngestNode && !isRemoteClusterClientNode) {
                incrementCountForRole(ClusterStatsNodes.Counts.COORDINATING_ONLY, expectedCounts);
            }

            response = client().admin().cluster().prepareClusterStats().get();
            assertCounts(response.getNodesStats().getCounts(), total, expectedCounts);
        }
    }

    // Validate assigning value "master" to setting "node.roles" can get correct count in Node Stats response after MASTER_ROLE deprecated.
    public void testNodeCountsWithDeprecatedMasterRole() throws ExecutionException, InterruptedException {
        int total = 1;
        Settings settings = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), Collections.singletonList(DiscoveryNodeRole.MASTER_ROLE.roleName()))
            .build();
        internalCluster().startNode(settings);
        waitForNodes(total);

        Map<String, Integer> expectedCounts = getExpectedCounts(0, 1, 1, 0, 0, 0, 0, 0);

        Client client = client();
        ClusterStatsResponse response = client.admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertCounts(response.getNodesStats().getCounts(), total, expectedCounts);

        Set<String> expectedRoles = Set.of(DiscoveryNodeRole.MASTER_ROLE.roleName());
        assertEquals(expectedRoles, getNodeRoles(client, 0));
    }

    private static void incrementCountForRole(String role, Map<String, Integer> counts) {
        Integer count = counts.get(role);
        if (count == null) {
            counts.put(role, 1);
        } else {
            counts.put(role, ++count);
        }
    }

    private void assertShardStats(ClusterStatsIndices.ShardStats stats, int indices, int total, int primaries, double replicationFactor) {
        assertThat(stats.getIndices(), Matchers.equalTo(indices));
        assertThat(stats.getTotal(), Matchers.equalTo(total));
        assertThat(stats.getPrimaries(), Matchers.equalTo(primaries));
        assertThat(stats.getReplication(), Matchers.equalTo(replicationFactor));
    }

    public void testIndicesShardStatsWithoutNodeLevelAggregations() {
        internalCluster().startNode();
        ensureGreen();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(false).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));

        prepareCreate("test1").setSettings(Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 1)).get();

        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(false).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.YELLOW));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(0L));
        assertThat(response.indicesStats.getIndexCount(), Matchers.equalTo(1));
        assertShardStats(response.getIndicesStats().getShards(), 1, 2, 2, 0.0);

        // add another node, replicas should get assigned
        internalCluster().startNode();
        ensureGreen();
        index("test1", "type", "1", "f", "f");
        refresh(); // make the doc visible
        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(false).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(1L));
        assertShardStats(response.getIndicesStats().getShards(), 1, 4, 2, 1.0);

        prepareCreate("test2").setSettings(Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 0)).get();
        ensureGreen();
        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(false).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertThat(response.indicesStats.getIndexCount(), Matchers.equalTo(2));
        assertShardStats(response.getIndicesStats().getShards(), 2, 7, 5, 2.0 / 5);

        assertThat(response.getIndicesStats().getShards().getAvgIndexPrimaryShards(), Matchers.equalTo(2.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexPrimaryShards(), Matchers.equalTo(2));
        assertThat(response.getIndicesStats().getShards().getMaxIndexPrimaryShards(), Matchers.equalTo(3));

        assertThat(response.getIndicesStats().getShards().getAvgIndexShards(), Matchers.equalTo(3.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexShards(), Matchers.equalTo(3));
        assertThat(response.getIndicesStats().getShards().getMaxIndexShards(), Matchers.equalTo(4));

        assertThat(response.getIndicesStats().getShards().getAvgIndexReplication(), Matchers.equalTo(0.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexReplication(), Matchers.equalTo(0.0));
        assertThat(response.getIndicesStats().getShards().getMaxIndexReplication(), Matchers.equalTo(1.0));

    }

    public void testIndicesShardStatsWithNodeLevelAggregations() {

        internalCluster().startNode();
        ensureGreen();
        ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(true).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));

        prepareCreate("test1").setSettings(Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 1)).get();

        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(true).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.YELLOW));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(0L));
        assertThat(response.indicesStats.getIndexCount(), Matchers.equalTo(1));
        assertShardStats(response.getIndicesStats().getShards(), 1, 2, 2, 0.0);

        // add another node, replicas should get assigned
        internalCluster().startNode();
        ensureGreen();
        index("test1", "type", "1", "f", "f");
        refresh(); // make the doc visible
        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(true).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertThat(response.indicesStats.getDocs().getCount(), Matchers.equalTo(1L));
        assertShardStats(response.getIndicesStats().getShards(), 1, 4, 2, 1.0);

        prepareCreate("test2").setSettings(Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 0)).get();
        ensureGreen();
        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(true).get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertThat(response.indicesStats.getIndexCount(), Matchers.equalTo(2));
        assertShardStats(response.getIndicesStats().getShards(), 2, 7, 5, 2.0 / 5);

        assertThat(response.getIndicesStats().getShards().getAvgIndexPrimaryShards(), Matchers.equalTo(2.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexPrimaryShards(), Matchers.equalTo(2));
        assertThat(response.getIndicesStats().getShards().getMaxIndexPrimaryShards(), Matchers.equalTo(3));

        assertThat(response.getIndicesStats().getShards().getAvgIndexShards(), Matchers.equalTo(3.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexShards(), Matchers.equalTo(3));
        assertThat(response.getIndicesStats().getShards().getMaxIndexShards(), Matchers.equalTo(4));

        assertThat(response.getIndicesStats().getShards().getAvgIndexReplication(), Matchers.equalTo(0.5));
        assertThat(response.getIndicesStats().getShards().getMinIndexReplication(), Matchers.equalTo(0.0));
        assertThat(response.getIndicesStats().getShards().getMaxIndexReplication(), Matchers.equalTo(1.0));

    }

    public void testValuesSmokeScreen() throws IOException, ExecutionException, InterruptedException {
        internalCluster().startNodes(randomIntBetween(1, 3));
        index("test1", "type", "1", "f", "f");

        ClusterStatsResponse response = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        String msg = response.toString();
        assertThat(msg, response.getTimestamp(), Matchers.greaterThan(946681200000L)); // 1 Jan 2000
        assertThat(msg, response.indicesStats.getStore().getSizeInBytes(), Matchers.greaterThan(0L));

        assertThat(msg, response.nodesStats.getFs().getTotal().getBytes(), Matchers.greaterThan(0L));
        assertThat(msg, response.nodesStats.getJvm().getVersions().size(), Matchers.greaterThan(0));

        assertThat(msg, response.nodesStats.getVersions().size(), Matchers.greaterThan(0));
        assertThat(msg, response.nodesStats.getVersions().contains(Version.CURRENT), Matchers.equalTo(true));
        assertThat(msg, response.nodesStats.getPlugins().size(), Matchers.greaterThanOrEqualTo(0));

        assertThat(msg, response.nodesStats.getProcess().count, Matchers.greaterThan(0));
        // 0 happens when not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getAvgOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(0L));
        // these can be -1 if not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getMinOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));
        assertThat(msg, response.nodesStats.getProcess().getMaxOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));

        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().addMetric(OS.metricName()).get();
        long total = 0;
        long free = 0;
        long used = 0;
        for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
            total += nodeStats.getOs().getMem().getTotal().getBytes();
            free += nodeStats.getOs().getMem().getFree().getBytes();
            used += nodeStats.getOs().getMem().getUsed().getBytes();
        }
        assertEquals(msg, free, response.nodesStats.getOs().getMem().getFree().getBytes());
        assertEquals(msg, total, response.nodesStats.getOs().getMem().getTotal().getBytes());
        assertEquals(msg, used, response.nodesStats.getOs().getMem().getUsed().getBytes());
        assertEquals(msg, OsStats.calculatePercentage(used, total), response.nodesStats.getOs().getMem().getUsedPercent());
        assertEquals(msg, OsStats.calculatePercentage(free, total), response.nodesStats.getOs().getMem().getFreePercent());
    }

    public void testValuesSmokeScreenWithNodeStatsAndIndicesStatsMetricsFilter() throws IOException, ExecutionException,
        InterruptedException {
        internalCluster().startNodes(randomIntBetween(1, 3));
        index("test1", "type", "1", "f", "f");

        ClusterStatsResponse response = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .computeAllMetrics(false)
            .requestMetrics(Set.of(Metric.values()))
            .indexMetrics(Set.of(IndexMetric.values()))
            .get();
        String msg = response.toString();
        assertThat(msg, response.getTimestamp(), Matchers.greaterThan(946681200000L)); // 1 Jan 2000
        assertThat(msg, response.indicesStats.getStore().getSizeInBytes(), Matchers.greaterThan(0L));

        assertThat(msg, response.nodesStats.getFs().getTotal().getBytes(), Matchers.greaterThan(0L));
        assertThat(msg, response.nodesStats.getJvm().getVersions().size(), Matchers.greaterThan(0));

        assertThat(msg, response.nodesStats.getVersions().size(), Matchers.greaterThan(0));
        assertThat(msg, response.nodesStats.getVersions().contains(Version.CURRENT), Matchers.equalTo(true));
        assertThat(msg, response.nodesStats.getPlugins().size(), Matchers.greaterThanOrEqualTo(0));

        assertThat(msg, response.nodesStats.getProcess().count, Matchers.greaterThan(0));
        // 0 happens when not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getAvgOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(0L));
        // these can be -1 if not supported on platform
        assertThat(msg, response.nodesStats.getProcess().getMinOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));
        assertThat(msg, response.nodesStats.getProcess().getMaxOpenFileDescriptors(), Matchers.greaterThanOrEqualTo(-1L));

        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().addMetric(OS.metricName()).get();
        long total = 0;
        long free = 0;
        long used = 0;
        for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
            total += nodeStats.getOs().getMem().getTotal().getBytes();
            free += nodeStats.getOs().getMem().getFree().getBytes();
            used += nodeStats.getOs().getMem().getUsed().getBytes();
        }
        assertEquals(msg, free, response.nodesStats.getOs().getMem().getFree().getBytes());
        assertEquals(msg, total, response.nodesStats.getOs().getMem().getTotal().getBytes());
        assertEquals(msg, used, response.nodesStats.getOs().getMem().getUsed().getBytes());
        assertEquals(msg, OsStats.calculatePercentage(used, total), response.nodesStats.getOs().getMem().getUsedPercent());
        assertEquals(msg, OsStats.calculatePercentage(free, total), response.nodesStats.getOs().getMem().getFreePercent());
    }

    public void testAllocatedProcessors() throws Exception {
        // start one node with 7 processors.
        internalCluster().startNode(Settings.builder().put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 7).build());
        waitForNodes(1);

        ClusterStatsResponse response = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertThat(response.getNodesStats().getOs().getAllocatedProcessors(), equalTo(7));
    }

    public void testClusterStatusWhenStateNotRecovered() throws Exception {
        internalCluster().startClusterManagerOnlyNode(
            Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2).build()
        );
        ClusterStatsResponse response = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.RED));

        internalCluster().startDataOnlyNodes(2);

        // wait for the cluster status to settle
        ensureGreen();
        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(randomBoolean()).get();
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }

    public void testFieldTypes() {
        internalCluster().startNode();
        ensureGreen();
        ClusterStatsResponse response = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertTrue(response.getIndicesStats().getMappings().getFieldTypeStats().isEmpty());

        client().admin().indices().prepareCreate("test1").setMapping("{\"properties\":{\"foo\":{\"type\": \"keyword\"}}}").get();
        client().admin()
            .indices()
            .prepareCreate("test2")
            .setMapping(
                "{\"properties\":{\"foo\":{\"type\": \"keyword\"},\"bar\":{\"properties\":{\"baz\":{\"type\":\"keyword\"},"
                    + "\"eggplant\":{\"type\":\"integer\"}}}}}"
            )
            .get();
        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(randomBoolean()).get();
        assertThat(response.getIndicesStats().getMappings().getFieldTypeStats().size(), equalTo(3));
        Set<IndexFeatureStats> stats = response.getIndicesStats().getMappings().getFieldTypeStats();
        for (IndexFeatureStats stat : stats) {
            if (stat.getName().equals("integer")) {
                assertThat(stat.getCount(), greaterThanOrEqualTo(1));
            } else if (stat.getName().equals("keyword")) {
                assertThat(stat.getCount(), greaterThanOrEqualTo(3));
            } else if (stat.getName().equals("object")) {
                assertThat(stat.getCount(), greaterThanOrEqualTo(1));
            }
        }
    }

    public void testFieldTypesWithMappingsFilter() {
        internalCluster().startNode();
        ensureGreen();
        ClusterStatsResponse response = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .computeAllMetrics(randomBoolean())
            .requestMetrics(Set.of(Metric.INDICES))
            .indexMetrics(Set.of(IndexMetric.MAPPINGS))
            .get();
        assertThat(response.getStatus(), Matchers.equalTo(ClusterHealthStatus.GREEN));
        assertTrue(response.getIndicesStats().getMappings().getFieldTypeStats().isEmpty());

        client().admin().indices().prepareCreate("test1").setMapping("{\"properties\":{\"foo\":{\"type\": \"keyword\"}}}").get();
        client().admin()
            .indices()
            .prepareCreate("test2")
            .setMapping(
                "{\"properties\":{\"foo\":{\"type\": \"keyword\"},\"bar\":{\"properties\":{\"baz\":{\"type\":\"keyword\"},"
                    + "\"eggplant\":{\"type\":\"integer\"}}}}}"
            )
            .get();
        response = client().admin().cluster().prepareClusterStats().useAggregatedNodeLevelResponses(randomBoolean()).get();
        assertThat(response.getIndicesStats().getMappings().getFieldTypeStats().size(), equalTo(3));
        Set<IndexFeatureStats> stats = response.getIndicesStats().getMappings().getFieldTypeStats();
        for (IndexFeatureStats stat : stats) {
            if (stat.getName().equals("integer")) {
                assertThat(stat.getCount(), greaterThanOrEqualTo(1));
            } else if (stat.getName().equals("keyword")) {
                assertThat(stat.getCount(), greaterThanOrEqualTo(3));
            } else if (stat.getName().equals("object")) {
                assertThat(stat.getCount(), greaterThanOrEqualTo(1));
            }
        }
    }

    public void testNodeRolesWithMasterLegacySettings() throws ExecutionException, InterruptedException {
        int total = 1;
        Settings legacyMasterSettings = Settings.builder()
            .put("node.master", true)
            .put("node.data", false)
            .put("node.ingest", false)
            .build();

        internalCluster().startNodes(legacyMasterSettings);
        waitForNodes(total);

        Map<String, Integer> expectedCounts = getExpectedCounts(0, 1, 1, 0, 1, 0, 0, 0);

        Client client = client();
        ClusterStatsResponse clusterStatsResponse = client.admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertCounts(clusterStatsResponse.getNodesStats().getCounts(), total, expectedCounts);

        Set<String> expectedRoles = Set.of(
            DiscoveryNodeRole.MASTER_ROLE.roleName(),
            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName()
        );
        assertEquals(expectedRoles, getNodeRoles(client, 0));
    }

    public void testNodeRolesWithClusterManagerRole() throws ExecutionException, InterruptedException {
        int total = 1;
        Settings clusterManagerNodeRoleSettings = Settings.builder()
            .put(
                "node.roles",
                String.format(
                    Locale.ROOT,
                    "%s, %s",
                    DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName(),
                    DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName()
                )
            )
            .build();

        internalCluster().startNodes(clusterManagerNodeRoleSettings);
        waitForNodes(total);

        Map<String, Integer> expectedCounts = getExpectedCounts(0, 1, 1, 0, 1, 0, 0, 0);

        Client client = client();
        ClusterStatsResponse clusterStatsResponse = client.admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertCounts(clusterStatsResponse.getNodesStats().getCounts(), total, expectedCounts);

        Set<String> expectedRoles = Set.of(
            DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName(),
            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName()
        );
        assertEquals(expectedRoles, getNodeRoles(client, 0));
    }

    public void testNodeRolesWithSeedDataNodeLegacySettings() throws ExecutionException, InterruptedException {
        int total = 1;
        Settings legacySeedDataNodeSettings = Settings.builder()
            .put("node.master", true)
            .put("node.data", true)
            .put("node.ingest", false)
            .build();

        internalCluster().startNodes(legacySeedDataNodeSettings);
        waitForNodes(total);

        Map<String, Integer> expectedRoleCounts = getExpectedCounts(1, 1, 1, 0, 1, 0, 0, 0);

        Client client = client();
        ClusterStatsResponse clusterStatsResponse = client.admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertCounts(clusterStatsResponse.getNodesStats().getCounts(), total, expectedRoleCounts);

        Set<String> expectedRoles = Set.of(
            DiscoveryNodeRole.MASTER_ROLE.roleName(),
            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName(),
            DiscoveryNodeRole.DATA_ROLE.roleName()
        );
        assertEquals(expectedRoles, getNodeRoles(client, 0));
    }

    public void testNodeRolesWithDataNodeLegacySettings() throws ExecutionException, InterruptedException {
        int total = 2;
        Settings legacyDataNodeSettings = Settings.builder()
            .put("node.master", false)
            .put("node.data", true)
            .put("node.ingest", false)
            .build();

        // can't start data-only node without assigning cluster-manager
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startNodes(legacyDataNodeSettings);
        waitForNodes(total);

        Map<String, Integer> expectedRoleCounts = getExpectedCounts(1, 1, 1, 0, 1, 0, 0, 0);

        Client client = client();
        ClusterStatsResponse clusterStatsResponse = client.admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertCounts(clusterStatsResponse.getNodesStats().getCounts(), total, expectedRoleCounts);

        Set<Set<String>> expectedNodesRoles = Set.of(
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName()),
            Set.of(DiscoveryNodeRole.DATA_ROLE.roleName(), DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName())
        );
        assertEquals(expectedNodesRoles, Set.of(getNodeRoles(client, 0), getNodeRoles(client, 1)));
    }

    public void testNodeRolesWithSearchNode() throws ExecutionException, InterruptedException {
        int total = 2;
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startSearchOnlyNode();
        waitForNodes(total);

        Map<String, Integer> expectedRoleCounts = getExpectedCounts(0, 1, 1, 0, 0, 0, 1, 0);

        Client client = client();
        ClusterStatsResponse clusterStatsResponse = client.admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .get();
        assertCounts(clusterStatsResponse.getNodesStats().getCounts(), total, expectedRoleCounts);

        Set<Set<String>> expectedNodesRoles = Set.of(
            Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName()),
            Set.of(DiscoveryNodeRole.SEARCH_ROLE.roleName())
        );
        assertEquals(expectedNodesRoles, Set.of(getNodeRoles(client, 0), getNodeRoles(client, 1)));
    }

    public void testClusterStatsWithNodeMetricsFilter() {
        internalCluster().startNode();
        ensureGreen();

        client().admin().indices().prepareCreate("test1").setMapping("{\"properties\":{\"foo\":{\"type\": \"keyword\"}}}").get();

        ClusterStatsRequestBuilder clusterStatsRequestBuilder = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean());
        assertTrue(clusterStatsRequestBuilder.request().computeAllMetrics());

        ClusterStatsResponse response = clusterStatsRequestBuilder.get();
        assertNotNull(response);
        assertNotNull(response.getNodesStats());
        assertNotNull(response.getIndicesStats());

        ClusterStatsResponse statsResponseWithAllNodeStatsMetrics = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .requestMetrics(ClusterStatsNodes.NODE_STATS_METRICS)
            .computeAllMetrics(false)
            .get();
        assertNotNull(statsResponseWithAllNodeStatsMetrics);
        assertNotNull(statsResponseWithAllNodeStatsMetrics.getNodesStats());
        assertNull(statsResponseWithAllNodeStatsMetrics.getIndicesStats());
        validateNodeStatsOutput(ClusterStatsNodes.NODE_STATS_METRICS, statsResponseWithAllNodeStatsMetrics);
        assertEquals(
            response.getNodesStats().getCounts().getTotal(),
            statsResponseWithAllNodeStatsMetrics.getNodesStats().getCounts().getTotal()
        );
        assertEquals(
            response.getNodesStats().getCounts().getRoles(),
            statsResponseWithAllNodeStatsMetrics.getNodesStats().getCounts().getRoles()
        );
        assertEquals(response.getNodesStats().getVersions(), statsResponseWithAllNodeStatsMetrics.getNodesStats().getVersions());
        assertEquals(response.getNodesStats().getPlugins(), statsResponseWithAllNodeStatsMetrics.getNodesStats().getPlugins());
    }

    public void testClusterStatsWithIndicesOnlyMetricsFilter() {
        internalCluster().startNode();
        ensureGreen();

        client().admin().indices().prepareCreate("test1").setMapping("{\"properties\":{\"foo\":{\"type\": \"keyword\"}}}").get();

        ClusterStatsRequestBuilder clusterStatsRequestBuilder = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean());
        assertTrue(clusterStatsRequestBuilder.request().computeAllMetrics());

        ClusterStatsResponse response = clusterStatsRequestBuilder.get();
        assertNotNull(response);
        assertNotNull(response.getNodesStats());
        assertNotNull(response.getIndicesStats());

        ClusterStatsResponse statsResponseWithIndicesRequestMetrics = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .requestMetrics(Set.of(Metric.INDICES))
            .indexMetrics(Set.of(IndexMetric.values()))
            .computeAllMetrics(false)
            .get();
        assertNotNull(statsResponseWithIndicesRequestMetrics);
        assertNull(statsResponseWithIndicesRequestMetrics.getNodesStats());
        assertNotNull(statsResponseWithIndicesRequestMetrics.getIndicesStats());
        validateIndicesStatsOutput(Set.of(IndexMetric.values()), statsResponseWithIndicesRequestMetrics);
    }

    public void testClusterStatsWithSelectiveNodeMetricAndIndexMetricsFilter() {
        internalCluster().startNode();
        ensureGreen();

        client().admin().indices().prepareCreate("test1").setMapping("{\"properties\":{\"foo\":{\"type\": \"keyword\"}}}").get();
        IndexRequest indexRequest = new IndexRequest("test1").id("doc_id").source(Map.of("test_type", "metrics_filter"));
        client().index(indexRequest);

        ClusterStatsRequestBuilder clusterStatsRequestBuilder = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean());
        assertTrue(clusterStatsRequestBuilder.request().computeAllMetrics());

        ClusterStatsResponse response = clusterStatsRequestBuilder.get();
        assertNotNull(response);
        assertNotNull(response.getNodesStats());
        assertNotNull(response.getIndicesStats());

        ClusterStatsResponse statsResponseWithAllIndicesMetrics = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .requestMetrics(Set.of(Metric.OS, Metric.FS, Metric.INDICES))
            .indexMetrics(Set.of(IndexMetric.FIELDDATA, IndexMetric.SHARDS, IndexMetric.SEGMENTS, IndexMetric.DOCS, IndexMetric.STORE))
            .computeAllMetrics(false)
            .get();
        assertNotNull(statsResponseWithAllIndicesMetrics);
        assertNotNull(statsResponseWithAllIndicesMetrics.getNodesStats());
        assertNotNull(statsResponseWithAllIndicesMetrics.getIndicesStats());
        validateNodeStatsOutput(Set.of(Metric.FS, Metric.OS), statsResponseWithAllIndicesMetrics);
        validateIndicesStatsOutput(
            Set.of(IndexMetric.FIELDDATA, IndexMetric.SHARDS, IndexMetric.SEGMENTS, IndexMetric.DOCS, IndexMetric.STORE),
            statsResponseWithAllIndicesMetrics
        );
        assertEquals(response.getIndicesStats().getFieldData(), statsResponseWithAllIndicesMetrics.getIndicesStats().getFieldData());
        assertEquals(response.getIndicesStats().getIndexCount(), statsResponseWithAllIndicesMetrics.getIndicesStats().getIndexCount());
        assertEquals(
            response.getIndicesStats().getShards().getTotal(),
            statsResponseWithAllIndicesMetrics.getIndicesStats().getShards().getTotal()
        );
        assertEquals(
            response.getIndicesStats().getShards().getPrimaries(),
            statsResponseWithAllIndicesMetrics.getIndicesStats().getShards().getPrimaries()
        );
    }

    public void testClusterStatsWithMappingsAndAnalysisStatsIndexMetricsFilter() {
        internalCluster().startNode();
        ensureGreen();

        client().admin().indices().prepareCreate("test1").setMapping("{\"properties\":{\"foo\":{\"type\": \"keyword\"}}}").get();
        IndexRequest indexRequest = new IndexRequest("test1").id("doc_id").source(Map.of("test_type", "metrics_filter"));
        client().index(indexRequest);

        ClusterStatsRequestBuilder clusterStatsRequestBuilder = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean());
        assertTrue(clusterStatsRequestBuilder.request().computeAllMetrics());

        ClusterStatsResponse response = clusterStatsRequestBuilder.get();
        assertNotNull(response);
        assertNotNull(response.getNodesStats());
        assertNotNull(response.getIndicesStats());

        ClusterStatsResponse statsResponseWithSpecificIndicesMetrics = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .requestMetrics(Set.of(Metric.INDICES))
            .indexMetrics(Set.of(IndexMetric.MAPPINGS, IndexMetric.ANALYSIS))
            .computeAllMetrics(false)
            .get();
        assertNotNull(statsResponseWithSpecificIndicesMetrics);
        assertNull(statsResponseWithSpecificIndicesMetrics.getNodesStats());
        assertNotNull(statsResponseWithSpecificIndicesMetrics.getIndicesStats());
        validateIndicesStatsOutput(Set.of(IndexMetric.MAPPINGS, IndexMetric.ANALYSIS), statsResponseWithSpecificIndicesMetrics);
        assertEquals(response.getIndicesStats().getIndexCount(), statsResponseWithSpecificIndicesMetrics.getIndicesStats().getIndexCount());
        assertEquals(response.getIndicesStats().getMappings(), statsResponseWithSpecificIndicesMetrics.getIndicesStats().getMappings());
        assertEquals(response.getIndicesStats().getAnalysis(), statsResponseWithSpecificIndicesMetrics.getIndicesStats().getAnalysis());
    }

    public void testClusterStatsWithIndexMetricWithDocsFilter() throws IOException {
        internalCluster().startNode();
        createIndex("test1");

        client().prepareIndex("test1").setId(Integer.toString(1)).setSource("field1", "value1").execute().actionGet();
        client().prepareIndex("test1").setId(Integer.toString(2)).setSource("field2", "value2").execute().actionGet();
        refreshAndWaitForReplication();

        ClusterStatsResponse statsResponseWithAllIndicesMetrics = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .requestMetrics(Set.of(Metric.INDICES))
            .indexMetrics(Set.of(IndexMetric.DOCS))
            .computeAllMetrics(false)
            .get();
        assertNotNull(statsResponseWithAllIndicesMetrics);
        assertNull(statsResponseWithAllIndicesMetrics.getNodesStats());
        assertNotNull(statsResponseWithAllIndicesMetrics.getIndicesStats());
        validateIndicesStatsOutput(Set.of(IndexMetric.DOCS), statsResponseWithAllIndicesMetrics);
        assertEquals(2, statsResponseWithAllIndicesMetrics.getIndicesStats().getDocs().getCount());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getDocs().getDeleted());
        assertTrue(statsResponseWithAllIndicesMetrics.getIndicesStats().getDocs().getAverageSizeInBytes() > 0);
    }

    public void testClusterStatsWithSelectiveMetricsFilterAndNoIndex() {
        internalCluster().startNode();
        ensureGreen();
        ClusterStatsResponse statsResponseWithAllIndicesMetrics = client().admin()
            .cluster()
            .prepareClusterStats()
            .useAggregatedNodeLevelResponses(randomBoolean())
            .requestMetrics(Set.of(Metric.OS, Metric.FS, Metric.INDICES))
            .indexMetrics(Set.of(IndexMetric.FIELDDATA, IndexMetric.SHARDS, IndexMetric.SEGMENTS, IndexMetric.DOCS, IndexMetric.STORE))
            .computeAllMetrics(false)
            .get();
        assertNotNull(statsResponseWithAllIndicesMetrics);
        assertNotNull(statsResponseWithAllIndicesMetrics.getNodesStats());
        assertNotNull(statsResponseWithAllIndicesMetrics.getIndicesStats());
        validateNodeStatsOutput(Set.of(Metric.FS, Metric.OS), statsResponseWithAllIndicesMetrics);
        validateIndicesStatsOutput(
            Set.of(IndexMetric.FIELDDATA, IndexMetric.SHARDS, IndexMetric.SEGMENTS, IndexMetric.DOCS, IndexMetric.STORE),
            statsResponseWithAllIndicesMetrics
        );
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getShards().getIndices());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getShards().getTotal());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getShards().getPrimaries());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getDocs().getCount());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getDocs().getDeleted());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getDocs().getTotalSizeInBytes());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getStore().getSizeInBytes());
        assertEquals(new ByteSizeValue(0), statsResponseWithAllIndicesMetrics.getIndicesStats().getStore().getReservedSize());
        assertEquals(new ByteSizeValue(0), statsResponseWithAllIndicesMetrics.getIndicesStats().getFieldData().getMemorySize());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getFieldData().getEvictions());
        assertNull(statsResponseWithAllIndicesMetrics.getIndicesStats().getFieldData().getFields());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getSegments().getCount());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getSegments().getIndexWriterMemoryInBytes());
        assertEquals(0, statsResponseWithAllIndicesMetrics.getIndicesStats().getSegments().getVersionMapMemoryInBytes());
    }

    private void validateNodeStatsOutput(Set<ClusterStatsRequest.Metric> expectedMetrics, ClusterStatsResponse clusterStatsResponse) {
        // Ingest, network types, discovery types and packaging types stats are not included here as they don't have a get method exposed.
        Set<Metric> NodeMetrics = Set.of(Metric.OS, Metric.JVM, Metric.FS, Metric.PROCESS, Metric.PLUGINS);
        for (Metric metric : NodeMetrics) {
            Object object = null;
            switch (metric) {
                case OS:
                    object = clusterStatsResponse.getNodesStats().getOs();
                    break;
                case JVM:
                    object = clusterStatsResponse.getNodesStats().getJvm();
                    break;
                case FS:
                    object = clusterStatsResponse.getNodesStats().getFs();
                    break;
                case PROCESS:
                    object = clusterStatsResponse.getNodesStats().getProcess();
                    break;
                case PLUGINS:
                    object = clusterStatsResponse.getNodesStats().getPlugins();
                    break;
            }
            if (expectedMetrics.contains(metric)) {
                assertNotNull(object);
            } else {
                assertNull(object);
            }
        }
    }

    private void validateIndicesStatsOutput(
        Set<ClusterStatsRequest.IndexMetric> expectedMetrics,
        ClusterStatsResponse clusterStatsResponse
    ) {
        for (IndexMetric indexMetric : IndexMetric.values()) {
            Object object = null;
            switch (indexMetric) {
                case SHARDS:
                    object = clusterStatsResponse.getIndicesStats().getShards();
                    break;
                case DOCS:
                    object = clusterStatsResponse.getIndicesStats().getDocs();
                    break;
                case STORE:
                    object = clusterStatsResponse.getIndicesStats().getStore();
                    break;
                case FIELDDATA:
                    object = clusterStatsResponse.getIndicesStats().getFieldData();
                    break;
                case QUERY_CACHE:
                    object = clusterStatsResponse.getIndicesStats().getQueryCache();
                    break;
                case COMPLETION:
                    object = clusterStatsResponse.getIndicesStats().getCompletion();
                    break;
                case SEGMENTS:
                    object = clusterStatsResponse.getIndicesStats().getSegments();
                    break;
                case ANALYSIS:
                    object = clusterStatsResponse.getIndicesStats().getAnalysis();
                    break;
                case MAPPINGS:
                    object = clusterStatsResponse.getIndicesStats().getMappings();
                    break;
            }
            if (expectedMetrics.contains(indexMetric)) {
                assertNotNull(object);
            } else {
                assertNull(object);
            }
        }
    }

    private Map<String, Integer> getExpectedCounts(
        int dataRoleCount,
        int masterRoleCount,
        int clusterManagerRoleCount,
        int ingestRoleCount,
        int remoteClusterClientRoleCount,
        int warmRoleCount,
        int searchRoleCount,
        int coordinatingOnlyCount
    ) {
        Map<String, Integer> expectedCounts = new HashMap<>();
        expectedCounts.put(DiscoveryNodeRole.DATA_ROLE.roleName(), dataRoleCount);
        expectedCounts.put(DiscoveryNodeRole.MASTER_ROLE.roleName(), masterRoleCount);
        expectedCounts.put(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName(), clusterManagerRoleCount);
        expectedCounts.put(DiscoveryNodeRole.INGEST_ROLE.roleName(), ingestRoleCount);
        expectedCounts.put(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE.roleName(), remoteClusterClientRoleCount);
        expectedCounts.put(DiscoveryNodeRole.WARM_ROLE.roleName(), warmRoleCount);
        expectedCounts.put(DiscoveryNodeRole.SEARCH_ROLE.roleName(), searchRoleCount);
        expectedCounts.put(ClusterStatsNodes.Counts.COORDINATING_ONLY, coordinatingOnlyCount);
        return expectedCounts;
    }

    private Set<String> getNodeRoles(Client client, int nodeNumber) throws ExecutionException, InterruptedException {
        NodesStatsResponse nodesStatsResponse = client.admin().cluster().nodesStats(new NodesStatsRequest()).get();
        return nodesStatsResponse.getNodes()
            .get(nodeNumber)
            .getNode()
            .getRoles()
            .stream()
            .map(DiscoveryNodeRole::roleName)
            .collect(Collectors.toSet());
    }
}
