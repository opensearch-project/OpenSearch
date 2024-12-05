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

package org.opensearch.cluster;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.health.ClusterShardHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ClusterHealthIT extends OpenSearchIntegTestCase {

    public void testSimpleLocalHealth() {
        createIndex("test");
        ensureGreen(); // cluster-manager should think it's green now.

        for (final String node : internalCluster().getNodeNames()) {
            // a very high time out, which should never fire due to the local flag
            logger.info("--> getting cluster health on [{}]", node);
            final ClusterHealthResponse health = client(node).admin()
                .cluster()
                .prepareHealth()
                .setLocal(true)
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout("30s")
                .get("10s");
            logger.info("--> got cluster health on [{}]", node);
            assertFalse("timed out on " + node, health.isTimedOut());
            assertThat("health status on " + node, health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
    }

    public void testHealth() {
        logger.info("--> running cluster health on an index that does not exists");
        ClusterHealthResponse healthResponse = client().admin()
            .cluster()
            .prepareHealth("test1")
            .setWaitForYellowStatus()
            .setTimeout("1s")
            .execute()
            .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> running cluster wide health");
        healthResponse = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        logger.info("--> Creating index test1 with zero replicas");
        createIndex("test1");

        logger.info("--> running cluster health on an index that does exists");
        healthResponse = client().admin().cluster().prepareHealth("test1").setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> running cluster health on an index that does exists and an index that doesn't exists");
        healthResponse = client().admin()
            .cluster()
            .prepareHealth("test1", "test2")
            .setWaitForYellowStatus()
            .setTimeout("1s")
            .execute()
            .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().get("test1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getIndices().size(), equalTo(1));
    }

    public void testHealthWithClosedIndices() {
        createIndex("index-1");
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }

        createIndex("index-2");
        assertAcked(client().admin().indices().prepareClose("index-2"));

        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-1").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-2").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin()
                .cluster()
                .prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2"), nullValue());
        }
        {
            ClusterHealthResponse response = client().admin()
                .cluster()
                .prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, false, true))
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1"), nullValue());
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }

        createIndex("index-3", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 50).build());
        assertAcked(client().admin().indices().prepareClose("index-3"));

        {
            ClusterHealthResponse response = client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNoInitializingShards(true)
                .setWaitForYellowStatus()
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-1").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-2").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-3").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth("index-*").get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }
        {
            ClusterHealthResponse response = client().admin()
                .cluster()
                .prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(1));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2"), nullValue());
            assertThat(response.getIndices().get("index-3"), nullValue());
        }
        {
            ClusterHealthResponse response = client().admin()
                .cluster()
                .prepareHealth("index-*")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, false, true))
                .get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(2));
            assertThat(response.getIndices().get("index-1"), nullValue());
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        }

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("index-3")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas()).build())
        );
        {
            ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
            assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.isTimedOut(), equalTo(false));
            assertThat(response.getIndices().size(), equalTo(3));
            assertThat(response.getIndices().get("index-1").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-2").getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(response.getIndices().get("index-3").getStatus(), equalTo(ClusterHealthStatus.GREEN));
        }
    }

    public void testHealthOnIndexCreation() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread clusterHealthThread = new Thread() {
            @Override
            public void run() {
                while (finished.get() == false) {
                    ClusterHealthResponse health = client().admin().cluster().prepareHealth().get();
                    assertThat(health.getStatus(), not(equalTo(ClusterHealthStatus.RED)));
                }
            }
        };
        clusterHealthThread.start();
        for (int i = 0; i < 10; i++) {
            createIndex("test" + i);
        }
        finished.set(true);
        clusterHealthThread.join();
    }

    public void testWaitForEventsRetriesIfOtherConditionsNotMet() {
        final ActionFuture<ClusterHealthResponse> healthResponseFuture = client().admin()
            .cluster()
            .prepareHealth("index")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .execute();

        final AtomicBoolean keepSubmittingTasks = new AtomicBoolean(true);
        final ClusterService clusterService = internalCluster().getInstance(
            ClusterService.class,
            internalCluster().getClusterManagerName()
        );
        final PlainActionFuture<Void> completionFuture = new PlainActionFuture<>();
        clusterService.submitStateUpdateTask("looping task", new ClusterStateUpdateTask(Priority.LOW) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                completionFuture.onFailure(e);
                throw new AssertionError(source, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (keepSubmittingTasks.get()) {
                    clusterService.submitStateUpdateTask("looping task", this);
                } else {
                    completionFuture.onResponse(null);
                }
            }
        });

        try {
            createIndex("index");
            assertFalse(client().admin().cluster().prepareHealth("index").setWaitForGreenStatus().get().isTimedOut());

            // at this point the original health response should not have returned: there was never a point where the index was green AND
            // the cluster-manager had processed all pending tasks above LANGUID priority.
            assertFalse(healthResponseFuture.isDone());
            keepSubmittingTasks.set(false);
            assertFalse(healthResponseFuture.actionGet(TimeValue.timeValueSeconds(30)).isTimedOut());
        } finally {
            keepSubmittingTasks.set(false);
            completionFuture.actionGet(TimeValue.timeValueSeconds(30));
        }
    }

    public void testHealthOnClusterManagerFailover() throws Exception {
        final String node = internalCluster().startDataOnlyNode();
        final boolean withIndex = randomBoolean();
        if (withIndex) {
            // Create index with many shards to provoke the health request to wait (for green) while cluster-manager is being shut down.
            // Notice that this is set to 0 after the test completed starting a number of health requests and cluster-manager restarts.
            // This ensures that the cluster is yellow when the health request is made, making the health request wait on the observer,
            // triggering a call to observer.onClusterServiceClose when cluster-manager is shutdown.
            createIndex(
                "test",
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 10))
                    // avoid full recoveries of index, just wait for replica to reappear
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "5m")
                    .build()
            );
        }
        final List<ActionFuture<ClusterHealthResponse>> responseFutures = new ArrayList<>();
        // Run a few health requests concurrent to cluster-manager fail-overs against a data-node
        // to make sure cluster-manager failover is handled without exceptions
        final int iterations = withIndex ? 10 : 20;
        for (int i = 0; i < iterations; ++i) {
            responseFutures.add(
                client(node).admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForGreenStatus()
                    .setClusterManagerNodeTimeout(TimeValue.timeValueMinutes(3))
                    .execute()
            );
            internalCluster().restartNode(internalCluster().getClusterManagerName(), InternalTestCluster.EMPTY_CALLBACK);
        }
        if (withIndex) {
            assertAcked(
                client().admin()
                    .indices()
                    .updateSettings(
                        new UpdateSettingsRequest("test").settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                    )
                    .get()
            );
        }
        for (ActionFuture<ClusterHealthResponse> responseFuture : responseFutures) {
            assertSame(responseFuture.get().getStatus(), ClusterHealthStatus.GREEN);
        }
    }

    public void testWaitForEventsTimesOutIfClusterManagerBusy() {
        final AtomicBoolean keepSubmittingTasks = new AtomicBoolean(true);
        final ClusterService clusterService = internalCluster().getInstance(
            ClusterService.class,
            internalCluster().getClusterManagerName()
        );
        final PlainActionFuture<Void> completionFuture = new PlainActionFuture<>();
        clusterService.submitStateUpdateTask("looping task", new ClusterStateUpdateTask(Priority.LOW) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                completionFuture.onFailure(e);
                throw new AssertionError(source, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (keepSubmittingTasks.get()) {
                    clusterService.submitStateUpdateTask("looping task", this);
                } else {
                    completionFuture.onResponse(null);
                }
            }
        });

        try {
            final ClusterHealthResponse clusterHealthResponse = client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(TimeValue.timeValueSeconds(1))
                .get(TimeValue.timeValueSeconds(30));
            assertTrue(clusterHealthResponse.isTimedOut());
        } finally {
            keepSubmittingTasks.set(false);
            completionFuture.actionGet(TimeValue.timeValueSeconds(30));
        }
    }

    public void testHealthWithClusterLevelAppliedAtTransportLayer() {
        createIndex(
            "test1",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen();
        ClusterHealthResponse healthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setApplyLevelAtTransportLayer(true)
            .execute()
            .actionGet();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());
        assertTrue(healthResponse.getIndices().isEmpty());
        assertEquals(1, healthResponse.getActiveShards());
        assertEquals(1, healthResponse.getActivePrimaryShards());
        assertEquals(0, healthResponse.getUnassignedShards());
        assertEquals(0, healthResponse.getInitializingShards());
        assertEquals(0, healthResponse.getRelocatingShards());
        assertEquals(0, healthResponse.getDelayedUnassignedShards());
    }

    public void testHealthWithIndicesLevelAppliedAtTransportLayer() {
        createIndex(
            "test1",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen();
        ClusterHealthResponse healthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setLevel("indices")
            .setApplyLevelAtTransportLayer(true)
            .execute()
            .actionGet();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());

        assertEquals(1, healthResponse.getActiveShards());
        assertEquals(1, healthResponse.getActivePrimaryShards());
        assertEquals(0, healthResponse.getUnassignedShards());
        assertEquals(0, healthResponse.getInitializingShards());
        assertEquals(0, healthResponse.getRelocatingShards());
        assertEquals(0, healthResponse.getDelayedUnassignedShards());

        Map<String, ClusterIndexHealth> indices = healthResponse.getIndices();
        assertFalse(indices.isEmpty());
        assertEquals(1, indices.size());
        for (Map.Entry<String, ClusterIndexHealth> indicesHealth : indices.entrySet()) {
            String indexName = indicesHealth.getKey();
            assertEquals("test1", indexName);
            ClusterIndexHealth indicesHealthValue = indicesHealth.getValue();
            assertEquals(1, indicesHealthValue.getActiveShards());
            assertEquals(1, indicesHealthValue.getActivePrimaryShards());
            assertEquals(0, indicesHealthValue.getInitializingShards());
            assertEquals(0, indicesHealthValue.getUnassignedShards());
            assertEquals(0, indicesHealthValue.getDelayedUnassignedShards());
            assertEquals(0, indicesHealthValue.getRelocatingShards());
            assertEquals(ClusterHealthStatus.GREEN, indicesHealthValue.getStatus());
            assertTrue(indicesHealthValue.getShards().isEmpty());
        }
    }

    public void testHealthWithShardLevelAppliedAtTransportLayer() {
        int dataNodes = internalCluster().getDataNodeNames().size();
        int greenClusterReplicaCount = dataNodes - 1;
        int yellowClusterReplicaCount = dataNodes;

        createIndex(
            "test1",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, greenClusterReplicaCount)
                .build()
        );
        ensureGreen(TimeValue.timeValueSeconds(120), "test1");
        createIndex(
            "test2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, greenClusterReplicaCount)
                .build()
        );
        ensureGreen(TimeValue.timeValueSeconds(120));
        client().admin()
            .indices()
            .prepareUpdateSettings()
            .setIndices("test2")
            .setSettings(Settings.builder().put("index.number_of_replicas", yellowClusterReplicaCount).build())
            .execute()
            .actionGet();
        ClusterHealthResponse healthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setLevel("shards")
            .setApplyLevelAtTransportLayer(true)
            .execute()
            .actionGet();
        assertEquals(ClusterHealthStatus.YELLOW, healthResponse.getStatus());

        assertEquals(2 * dataNodes, healthResponse.getActiveShards());
        assertEquals(2, healthResponse.getActivePrimaryShards());
        assertEquals(1, healthResponse.getUnassignedShards());
        assertEquals(0, healthResponse.getInitializingShards());
        assertEquals(0, healthResponse.getRelocatingShards());
        assertEquals(0, healthResponse.getDelayedUnassignedShards());

        Map<String, ClusterIndexHealth> indices = healthResponse.getIndices();
        assertFalse(indices.isEmpty());
        assertEquals(2, indices.size());
        for (Map.Entry<String, ClusterIndexHealth> indicesHealth : indices.entrySet()) {
            String indexName = indicesHealth.getKey();
            boolean indexHasMoreReplicas = indexName.equals("test2");
            ClusterIndexHealth indicesHealthValue = indicesHealth.getValue();
            assertEquals(dataNodes, indicesHealthValue.getActiveShards());
            assertEquals(1, indicesHealthValue.getActivePrimaryShards());
            assertEquals(0, indicesHealthValue.getInitializingShards());
            assertEquals(indexHasMoreReplicas ? 1 : 0, indicesHealthValue.getUnassignedShards());
            assertEquals(0, indicesHealthValue.getDelayedUnassignedShards());
            assertEquals(0, indicesHealthValue.getRelocatingShards());
            assertEquals(indexHasMoreReplicas ? ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN, indicesHealthValue.getStatus());
            Map<Integer, ClusterShardHealth> shards = indicesHealthValue.getShards();
            assertFalse(shards.isEmpty());
            assertEquals(1, shards.size());
            for (Map.Entry<Integer, ClusterShardHealth> shardHealth : shards.entrySet()) {
                ClusterShardHealth clusterShardHealth = shardHealth.getValue();
                assertEquals(dataNodes, clusterShardHealth.getActiveShards());
                assertEquals(indexHasMoreReplicas ? 1 : 0, clusterShardHealth.getUnassignedShards());
                assertEquals(0, clusterShardHealth.getDelayedUnassignedShards());
                assertEquals(0, clusterShardHealth.getRelocatingShards());
                assertEquals(0, clusterShardHealth.getInitializingShards());
                assertTrue(clusterShardHealth.isPrimaryActive());
                assertEquals(indexHasMoreReplicas ? ClusterHealthStatus.YELLOW : ClusterHealthStatus.GREEN, clusterShardHealth.getStatus());
            }
        }
    }

    public void testHealthWithAwarenessAttributesLevelAppliedAtTransportLayer() {
        createIndex(
            "test1",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen();
        ClusterHealthResponse healthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setLevel("awareness_attributes")
            .setApplyLevelAtTransportLayer(true)
            .execute()
            .actionGet();
        assertEquals(ClusterHealthStatus.GREEN, healthResponse.getStatus());
        assertTrue(healthResponse.getIndices().isEmpty());
        assertNotNull(healthResponse.getClusterAwarenessHealth());
        assertEquals(1, healthResponse.getActiveShards());
        assertEquals(1, healthResponse.getActivePrimaryShards());
        assertEquals(0, healthResponse.getUnassignedShards());
        assertEquals(0, healthResponse.getInitializingShards());
        assertEquals(0, healthResponse.getRelocatingShards());
        assertEquals(0, healthResponse.getDelayedUnassignedShards());
    }
}
