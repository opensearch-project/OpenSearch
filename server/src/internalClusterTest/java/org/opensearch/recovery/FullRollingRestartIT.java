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

package org.opensearch.recovery;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.Priority;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class FullRollingRestartIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public FullRollingRestartIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return replicationSettings;
    }

    protected void assertTimeout(ClusterHealthRequestBuilder requestBuilder) {
        ClusterHealthResponse clusterHealth = requestBuilder.get();
        if (clusterHealth.isTimedOut()) {
            logger.info("cluster health request timed out:\n{}", clusterHealth);
            fail("cluster health request timed out");
        }
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testFullRollingRestart() throws Exception {
        internalCluster().startNode();
        createIndex("test");

        final String healthTimeout = "1m";

        for (int i = 0; i < 1000; i++) {
            client().prepareIndex("test")
                .setId(Long.toString(i))
                .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map())
                .execute()
                .actionGet();
        }
        flush();
        for (int i = 1000; i < 2000; i++) {
            client().prepareIndex("test")
                .setId(Long.toString(i))
                .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map())
                .execute()
                .actionGet();
        }

        logger.info("--> now start adding nodes");
        internalCluster().startNode();
        internalCluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("3")
        );

        logger.info("--> add two more nodes");
        internalCluster().startNode();
        internalCluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("5")
        );

        logger.info("--> refreshing and checking data");
        refreshAndWaitForReplication();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2000L);
        }

        // now start shutting nodes down
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("4")
        );

        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("3")
        );

        logger.info("--> stopped two nodes, verifying data");
        refreshAndWaitForReplication();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2000L);
        }

        // closing the 3rd node
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("2")
        );

        internalCluster().stopRandomDataNode();

        // make sure the cluster state is yellow, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForYellowStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("1")
        );

        logger.info("--> one node left, verifying data");
        refreshAndWaitForReplication();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2000L);
        }
    }

    public void testNoRebalanceOnRollingRestart() throws Exception {
        // see https://github.com/elastic/elasticsearch/issues/14387
        internalCluster().startClusterManagerOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(3);
        /*
          We start 3 nodes and a dedicated cluster-manager. Restart on of the data-nodes and ensure that we got no relocations.
          Yet we have 6 shards 0 replica so that means if the restarting node comes back both other nodes are subject
          to relocating to the restarting node since all had 2 shards and now one node has nothing allocated.
          We have a fix for this to wait until we have allocated unallocated shards now so this shouldn't happen.
         */
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "6")
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0")
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(1))
        ).get();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test")
                .setId(Long.toString(i))
                .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map())
                .execute()
                .actionGet();
        }
        ensureGreen();
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            assertTrue(
                "relocated from: " + recoveryState.getSourceNode() + " to: " + recoveryState.getTargetNode() + "\n" + state,
                recoveryState.getRecoverySource().getType() != RecoverySource.Type.PEER || recoveryState.getPrimary() == false
            );
        }
        internalCluster().restartRandomDataNode();
        ensureGreen();
        client().admin().cluster().prepareState().get().getState();

        recoveryResponse = client().admin().indices().prepareRecoveries("test").get();
        for (RecoveryState recoveryState : recoveryResponse.shardRecoveryStates().get("test")) {
            assertTrue(
                "relocated from: " + recoveryState.getSourceNode() + " to: " + recoveryState.getTargetNode() + "-- \nbefore: \n" + state,
                recoveryState.getRecoverySource().getType() != RecoverySource.Type.PEER || recoveryState.getPrimary() == false
            );
        }
    }

    public void testFullRollingRestart_withNoRecoveryPayloadAndSource() throws Exception {
        internalCluster().startNode();
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_source")
            .field("enabled")
            .value(false)
            .field("recovery_source_enabled")
            .value(false)
            .endObject()
            .endObject();
        CreateIndexResponse response = prepareCreate("test").setMapping(builder).get();
        logger.info("Create index response is : {}", response);

        final String healthTimeout = "1m";

        for (int i = 0; i < 1000; i++) {
            client().prepareIndex("test")
                .setId(Long.toString(i))
                .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map())
                .execute()
                .actionGet();
        }

        for (int i = 1000; i < 2000; i++) {
            client().prepareIndex("test")
                .setId(Long.toString(i))
                .setSource(MapBuilder.<String, Object>newMapBuilder().put("test", "value" + i).map())
                .execute()
                .actionGet();
        }
        // ensuring all docs are committed to file system
        flush();

        logger.info("--> now start adding nodes");
        internalCluster().startNode();
        internalCluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("3")
        );

        logger.info("--> add two more nodes");
        internalCluster().startNode();
        internalCluster().startNode();

        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("5")
        );

        logger.info("--> refreshing and checking data");
        refreshAndWaitForReplication();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2000L);
        }

        // now start shutting nodes down
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("4")
        );

        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("3")
        );

        logger.info("--> stopped two nodes, verifying data");
        refreshAndWaitForReplication();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2000L);
        }

        // closing the 3rd node
        internalCluster().stopRandomDataNode();
        // make sure the cluster state is green, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForGreenStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("2")
        );

        internalCluster().stopRandomDataNode();

        // make sure the cluster state is yellow, and all has been recovered
        assertTimeout(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(healthTimeout)
                .setWaitForYellowStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForNodes("1")
        );

        logger.info("--> one node left, verifying data");
        refreshAndWaitForReplication();
        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2000L);
        }
    }
}
