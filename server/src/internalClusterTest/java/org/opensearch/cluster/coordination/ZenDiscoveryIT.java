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

package org.opensearch.cluster.coordination;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.discovery.Discovery;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.TestCustomMetadata;
import org.opensearch.transport.RemoteTransportException;

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.DISCOVERY;
import static org.opensearch.cluster.coordination.JoinHelper.CLUSTER_MANAGER_VALIDATE_JOIN_CACHE_INTERVAL;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.NodeRoles.clusterManagerOnlyNode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ZenDiscoveryIT extends OpenSearchIntegTestCase {

    public void testNoShardRelocationsOccurWhenElectedClusterManagerNodeFails() throws Exception {

        Settings clusterManagerNodeSettings = clusterManagerOnlyNode();
        internalCluster().startNodes(2, clusterManagerNodeSettings);
        Settings dateNodeSettings = dataNode();
        internalCluster().startNodes(2, dateNodeSettings);
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("4")
            .setWaitForNoRelocatingShards(true)
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), is(false));

        createIndex("test");
        ensureSearchable("test");
        RecoveryResponse r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesBeforeNewClusterManager = r.shardRecoveryStates().get("test").size();

        final String oldClusterManager = internalCluster().getClusterManagerName();
        internalCluster().stopCurrentClusterManagerNode();
        assertBusy(() -> {
            String current = internalCluster().getClusterManagerName();
            assertThat(current, notNullValue());
            assertThat(current, not(equalTo(oldClusterManager)));
        });
        ensureSearchable("test");

        r = client().admin().indices().prepareRecoveries("test").get();
        int numRecoveriesAfterNewClusterManager = r.shardRecoveryStates().get("test").size();
        assertThat(numRecoveriesAfterNewClusterManager, equalTo(numRecoveriesBeforeNewClusterManager));
    }

    public void testHandleNodeJoin_incompatibleClusterState() throws InterruptedException, ExecutionException, TimeoutException {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(
            Settings.builder().put(CLUSTER_MANAGER_VALIDATE_JOIN_CACHE_INTERVAL.getKey(), TimeValue.timeValueMillis(0)).build()
        );
        String node1 = internalCluster().startNode();
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node1);
        Coordinator coordinator = (Coordinator) internalCluster().getInstance(Discovery.class, clusterManagerNode);
        final ClusterState state = clusterService.state();
        Metadata.Builder mdBuilder = Metadata.builder(state.metadata());
        mdBuilder.putCustom(CustomMetadata.TYPE, new CustomMetadata("data"));
        ClusterState stateWithCustomMetadata = ClusterState.builder(state).metadata(mdBuilder).build();

        final CompletableFuture<Throwable> future = new CompletableFuture<>();
        DiscoveryNode node = state.nodes().getLocalNode();
        coordinator.sendValidateJoinRequest(
            stateWithCustomMetadata,
            new JoinRequest(node, 0L, Optional.empty()),
            new JoinHelper.JoinCallback() {
                @Override
                public void onSuccess() {
                    future.completeExceptionally(new AssertionError("onSuccess should not be called"));
                }

                @Override
                public void onFailure(Exception e) {
                    future.complete(e);
                }
            }
        );

        Throwable t = future.get(10, TimeUnit.SECONDS);

        assertTrue(t instanceof IllegalStateException);
        assertTrue(t.getCause() instanceof RemoteTransportException);
        assertTrue(t.getCause().getCause() instanceof IllegalArgumentException);
        assertThat(t.getCause().getCause().getMessage(), containsString("Unknown NamedWriteable"));
    }

    public static class CustomMetadata extends TestCustomMetadata {
        public static final String TYPE = "custom_md";

        CustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }
    }

    public void testDiscoveryStats() throws Exception {
        internalCluster().startNode();
        ensureGreen(); // ensures that all events are processed (in particular state recovery fully completed)
        assertBusy(
            () -> assertThat(
                internalCluster().clusterService(internalCluster().getClusterManagerName())
                    .getClusterManagerService()
                    .numberOfPendingTasks(),
                equalTo(0)
            )
        ); // see https://github.com/elastic/elasticsearch/issues/24388

        logger.info("--> request node discovery stats");
        NodesStatsResponse statsResponse = client().admin().cluster().prepareNodesStats().clear().addMetric(DISCOVERY.metricName()).get();
        assertThat(statsResponse.getNodes().size(), equalTo(1));

        DiscoveryStats stats = statsResponse.getNodes().get(0).getDiscoveryStats();
        assertThat(stats.getQueueStats(), notNullValue());
        assertThat(stats.getQueueStats().getTotal(), equalTo(0));
        assertThat(stats.getQueueStats().getCommitted(), equalTo(0));
        assertThat(stats.getQueueStats().getPending(), equalTo(0));

        assertThat(stats.getPublishStats(), notNullValue());
        assertThat(stats.getPublishStats().getFullClusterStateReceivedCount(), greaterThanOrEqualTo(0L));
        assertThat(stats.getPublishStats().getIncompatibleClusterStateDiffReceivedCount(), equalTo(0L));
        assertThat(stats.getPublishStats().getCompatibleClusterStateDiffReceivedCount(), greaterThanOrEqualTo(0L));

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
    }
}
