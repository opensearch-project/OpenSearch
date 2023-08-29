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

package org.opensearch.indices.settings;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.EnumSet;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class UpdateNumberOfReplicasIT extends OpenSearchIntegTestCase {
    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    public void testSimpleUpdateNumberOfReplicas() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test", 2));
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .execute()
            .actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());

        NumShards numShards = getNumShards("test");

        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(numShards.numReplicas));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.totalNumShards));

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(jsonBuilder().startObject().field("value", "test" + i).endObject())
                .get();
        }

        refresh();

        for (int i = 0; i < 10; i++) {
            SearchResponse countResponse = client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get();
            assertHitCount(countResponse, 10L);
        }

        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        logger.info("Increasing the number of replicas from 1 to 2");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", 2))
                .execute()
                .actionGet()
        );
        logger.info("Running Cluster Health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .execute()
            .actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        // only 2 copies allocated (1 replica) across 2 nodes
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        final long afterReplicaIncreaseSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterReplicaIncreaseSettingsVersion, equalTo(1 + settingsVersion));

        logger.info("starting another node to new replicas will be allocated to it");
        allowNodes("test", 3);

        final long afterStartingAnotherNodeVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();

        logger.info("Running Cluster Health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNodes(">=3")
            .execute()
            .actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        // all 3 copies allocated across 3 nodes
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        for (int i = 0; i < 10; i++) {
            SearchResponse countResponse = client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get();
            assertHitCount(countResponse, 10L);
        }

        logger.info("Decreasing number of replicas from 2 to 0");
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", 0))
                .get()
        );

        logger.info("Running Cluster Health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNoRelocatingShards(true)
            .setWaitForNodes(">=3")
            .execute()
            .actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        // a single copy is allocated (replica set to 0)
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), 10);
        }

        final long afterReplicaDecreaseSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterReplicaDecreaseSettingsVersion, equalTo(1 + afterStartingAnotherNodeVersion));
    }

    public void testAutoExpandNumberOfReplicas0ToData() throws IOException {
        internalCluster().ensureAtMostNumDataNodes(2);
        logger.info("--> creating index test with auto expand replicas");
        assertAcked(prepareCreate("test", 2, Settings.builder().put("auto_expand_replicas", "0-all")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(ActiveShardCount.ALL));

            clusterHealth = client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus()
                .setWaitForActiveShards(numShards.numPrimaries * 2)
                .execute()
                .actionGet();
            logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
            assertThat(clusterHealth.isTimedOut(), equalTo(false));
            assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
            assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
            assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));
        }

        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();

        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 3)
            .setWaitForNodes(">=3")
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        final long afterAddingOneNodeSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterAddingOneNodeSettingsVersion, equalTo(1 + settingsVersion));

        logger.info("--> closing one node");
        internalCluster().ensureAtMostNumDataNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .setWaitForNodes(">=2")
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        final long afterClosingOneNodeSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterClosingOneNodeSettingsVersion, equalTo(1 + afterAddingOneNodeSettingsVersion));

        logger.info("--> closing another node");
        internalCluster().ensureAtMostNumDataNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(">=1")
            .setWaitForActiveShards(numShards.numPrimaries)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(0));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));

        final long afterClosingAnotherNodeSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterClosingAnotherNodeSettingsVersion, equalTo(1 + afterClosingOneNodeSettingsVersion));
    }

    public void testAutoExpandNumberReplicas1ToData() throws IOException {
        logger.info("--> creating index test with auto expand replicas");
        internalCluster().ensureAtMostNumDataNodes(2);
        assertAcked(prepareCreate("test", 2, Settings.builder().put("auto_expand_replicas", "1-all")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(ActiveShardCount.ALL));

            clusterHealth = client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForGreenStatus()
                .setWaitForActiveShards(numShards.numPrimaries * 2)
                .execute()
                .actionGet();
            logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
            assertThat(clusterHealth.isTimedOut(), equalTo(false));
            assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
            assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
            assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));
        }

        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        logger.info("--> add another node, should increase the number of replicas");
        allowNodes("test", 3);

        logger.info("--> running cluster health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 3)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        final long afterAddingOneNodeSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterAddingOneNodeSettingsVersion, equalTo(1 + settingsVersion));

        logger.info("--> closing one node");
        internalCluster().ensureAtMostNumDataNodes(2);
        allowNodes("test", 2);

        logger.info("--> running cluster health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForNodes(">=2")
            .setWaitForActiveShards(numShards.numPrimaries * 2)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 2));

        final long afterClosingOneNodeSettingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        assertThat(afterClosingOneNodeSettingsVersion, equalTo(1 + afterAddingOneNodeSettingsVersion));

        logger.info("--> closing another node");
        internalCluster().ensureAtMostNumDataNodes(1);
        allowNodes("test", 1);

        logger.info("--> running cluster health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus()
            .setWaitForNodes(">=1")
            .setWaitForActiveShards(numShards.numPrimaries)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(1));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries));
    }

    public void testAutoExpandNumberReplicas2() {
        logger.info("--> creating index test with auto expand replicas set to 0-2");
        assertAcked(prepareCreate("test", 3, Settings.builder().put("auto_expand_replicas", "0-2")));

        NumShards numShards = getNumShards("test");

        logger.info("--> running cluster health");
        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 3)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(2));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 3));

        logger.info("--> add two more nodes");
        allowNodes("test", 4);
        allowNodes("test", 5);

        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        logger.info("--> update the auto expand replicas to 0-3");
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("auto_expand_replicas", "0-3"))
            .execute()
            .actionGet();

        logger.info("--> running cluster health");
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForGreenStatus()
            .setWaitForActiveShards(numShards.numPrimaries * 4)
            .execute()
            .actionGet();
        logger.info("--> done cluster health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getIndices().get("test").getActivePrimaryShards(), equalTo(numShards.numPrimaries));
        assertThat(clusterHealth.getIndices().get("test").getNumberOfReplicas(), equalTo(3));
        assertThat(clusterHealth.getIndices().get("test").getActiveShards(), equalTo(numShards.numPrimaries * 4));

        /*
         * The settings version increases twice, the first time from the settings update increasing auto expand replicas, and the second
         * time from the number of replicas changed by the allocation service.
         */
        assertThat(
            client().admin().cluster().prepareState().get().getState().metadata().index("test").getSettingsVersion(),
            equalTo(1 + 1 + settingsVersion)
        );
    }

    public void testUpdateWithInvalidNumberOfReplicas() {
        createIndex("test");
        final long settingsVersion = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index("test")
            .getSettingsVersion();
        final int value = randomIntBetween(-10, -1);
        try {
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, value))
                .execute()
                .actionGet();
            fail("should have thrown an exception about the replica shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
            assertThat(
                client().admin().cluster().prepareState().get().getState().metadata().index("test").getSettingsVersion(),
                equalTo(settingsVersion)
            );
        }
    }

    public void testUpdateNumberOfReplicasAllowNoIndices() {
        createIndex("test-index", Settings.builder().put("index.number_of_replicas", 0).build());
        final IndicesOptions options = new IndicesOptions(
            EnumSet.of(IndicesOptions.Option.ALLOW_NO_INDICES),
            EnumSet.of(IndicesOptions.WildcardStates.OPEN)
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("non-existent-*")
                .setSettings(Settings.builder().put("index.number_of_replicas", 1))
                .setIndicesOptions(options)
                .get()
        );
        final int numberOfReplicas = Integer.parseInt(
            client().admin().indices().prepareGetSettings("test-index").get().getSetting("test-index", "index.number_of_replicas")
        );
        assertThat(numberOfReplicas, equalTo(0));
    }

    public void testAwarenessReplicaBalance() {
        createIndex("aware-replica", Settings.builder().put("index.number_of_replicas", 0).build());
        createIndex(".system-index", Settings.builder().put("index.number_of_replicas", 0).build());
        manageReplicaBalanceSetting(true);
        int updated = 0;

        try {
            // replica count of 1 is ideal
            client().admin()
                .indices()
                .prepareUpdateSettings("aware-replica")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                .execute()
                .actionGet();
            updated++;

            // Since auto expand replica setting take precedence, this should pass
            client().admin()
                .indices()
                .prepareUpdateSettings("aware-replica")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                )
                .execute()
                .actionGet();
            updated++;

            // system index - should be able to update
            client().admin()
                .indices()
                .prepareUpdateSettings(".system-index")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2))
                .execute()
                .actionGet();
            updated++;

            client().admin()
                .indices()
                .prepareUpdateSettings("aware-replica")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2))
                .execute()
                .actionGet();
            fail("should have thrown an exception about the replica count");

        } catch (IllegalArgumentException e) {
            assertEquals(
                "Validation Failed: 1: expected total copies needs to be a multiple of total awareness attributes [2];",
                e.getMessage()
            );
            assertEquals(3, updated);
        } finally {
            manageReplicaBalanceSetting(false);
        }
    }

    public void testAwarenessReplicaBalanceWithUseZoneForDefaultReplicaCount() {
        createIndex("aware-replica", Settings.builder().put("index.number_of_replicas", 0).build());
        createIndex(".system-index", Settings.builder().put("index.number_of_replicas", 0).build());
        DeleteIndexTemplateRequestBuilder deleteTemplate = client().admin().indices().prepareDeleteTemplate("random_index_template");
        assertAcked(deleteTemplate.execute().actionGet());
        manageReplicaSettingForDefaultReplica(true);
        int updated = 0;

        try {

            // replica count should not be changed and we should see the original replica count
            client().admin()
                .indices()
                .prepareUpdateSettings("aware-replica")
                .setSettings(Settings.builder().put("refresh_interval", "1s"))
                .execute()
                .actionGet();
            updated++;

            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final IndexMetadata newIndex = state.metadata().index("aware-replica");
            assertThat(newIndex.getNumberOfReplicas(), equalTo(0));

            // replica count of 2 is ideal
            client().admin()
                .indices()
                .prepareUpdateSettings("aware-replica")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2))
                .execute()
                .actionGet();
            updated++;

            // Since auto expand replica setting take precedence, this should pass
            client().admin()
                .indices()
                .prepareUpdateSettings("aware-replica")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-2")
                )
                .execute()
                .actionGet();
            updated++;

            // system index - should be able to update
            client().admin()
                .indices()
                .prepareUpdateSettings(".system-index")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 3))
                .execute()
                .actionGet();
            updated++;

            client().admin()
                .indices()
                .prepareUpdateSettings("aware-replica")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                .execute()
                .actionGet();
            fail("should have thrown an exception about the replica count");

        } catch (IllegalArgumentException e) {
            assertEquals(
                "Validation Failed: 1: expected total copies needs to be a multiple of total awareness attributes [3];",
                e.getMessage()
            );
            assertEquals(4, updated);
        } finally {
            manageReplicaSettingForDefaultReplica(false);
            randomIndexTemplate();
        }
    }

}
