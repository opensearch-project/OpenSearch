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
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
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
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
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

    public void testDerivedSourceRollingRestart() throws Exception {
        String mapping = """
            {
              "properties": {
                "text_field": {
                  "type": "text",
                  "store": true
                },
                "keyword_field": {
                  "type": "keyword"
                },
                "numeric_field": {
                  "type": "long"
                },
                "date_field": {
                  "type": "date"
                }
              }
            }""";

        // Start initial node
        internalCluster().startNode();
        assertAcked(
            prepareCreate(
                "test",
                Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 0)
                    .put("index.derived_source.enabled", true)
            ).setMapping(mapping)
        );

        // Index test data
        int docCount = randomIntBetween(1000, 2000);
        bulkIndexDocuments(docCount);

        ensureGreen();

        // Start rolling restart with verification
        logger.info("--> starting rolling restart with {} initial docs", docCount);
        rollingRestartWithVerification(docCount);
    }

    public void testDerivedSourceWithMultiFieldsRollingRestart() throws Exception {
        String mapping = """
            {
              "properties": {
                "text_field": {
                  "type": "text",
                  "store": true
                },
                "multi_field": {
                  "properties": {
                    "keyword_field": {
                      "type": "keyword"
                    },
                    "long_field": {
                      "type": "long"
                    },
                    "boolean_field": {
                       "type": "boolean"
                    }
                  }
                }
              }
            }""";

        // Start initial cluster
        internalCluster().startNode();
        assertAcked(
            prepareCreate(
                "test",
                Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 0)
                    .put("index.derived_source.enabled", true)
                    .put("index.refresh_interval", "1s") // Ensure regular refreshes
            ).setMapping(mapping)
        );

        // Index initial documents
        int docCount = randomIntBetween(500, 1000);
        bulkIndexMultiFieldDocuments(docCount, 0);

        // Ensure documents are visible
        flush("test");
        refresh("test");

        // Verify initial document count
        assertHitCount(client().prepareSearch("test").setSize(0).get(), docCount);

        // Add replicas before starting new nodes
        assertAcked(
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.number_of_replicas", 1))
        );

        // Add nodes and additional documents
        int totalDocs = docCount;
        for (int i = 0; i < 1; i++) {
            internalCluster().startNode();

            // Add more documents
            int additionalDocs = randomIntBetween(100, 200);
            bulkIndexMultiFieldDocuments(additionalDocs, totalDocs);
            totalDocs += additionalDocs;

            // Ensure all documents are visible
            flush("test");
            refresh("test");

            // Verify document count and contents
            int finalTotalDocs = totalDocs;
            assertBusy(() -> { verifyDerivedSourceWithMultiField(finalTotalDocs); }, 30, TimeUnit.SECONDS);
        }

        ensureGreen("test");

        // Rolling restart
        for (String node : internalCluster().getNodeNames()) {
            internalCluster().restartNode(node);
            ensureGreen("test");

            // Verify after each node restart
            int finalTotalDocs1 = totalDocs;
            assertBusy(() -> { verifyDerivedSourceWithMultiField(finalTotalDocs1); }, 30, TimeUnit.SECONDS);
        }
    }

    public void testDerivedSourceWithConcurrentUpdatesRollingRestart() throws Exception {
        String mapping = """
            {
              "properties": {
                "text_field": {
                  "type": "text",
                  "store": true
                },
                "counter": {
                  "type": "long"
                },
                "last_updated": {
                  "type": "date"
                },
                "version": {
                  "type": "long"
                }
              }
            }""";

        // Start initial node
        internalCluster().startNode();
        assertAcked(
            prepareCreate(
                "test",
                Settings.builder()
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .put("index.derived_source.enabled", true)
                    .put("index.refresh_interval", "1s")
            ).setMapping(mapping)
        );

        // Initial indexing
        int docCount = randomIntBetween(100, 200); // Reduced count for stability
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        for (int i = 0; i < docCount; i++) {
            bulkRequest.add(
                client().prepareIndex("test")
                    .setId(String.valueOf(i))
                    .setSource("text_field", "text value " + i, "counter", 0, "last_updated", System.currentTimeMillis(), "version", 0)
            );

            if (i % 100 == 0) {
                BulkResponse response = bulkRequest.execute().actionGet();
                assertFalse(response.hasFailures());
                bulkRequest = client().prepareBulk();
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse response = bulkRequest.execute().actionGet();
            assertFalse(response.hasFailures());
        }

        refresh("test");
        flush("test");
        ensureGreen();

        // Verify initial document count
        assertHitCount(client().prepareSearch("test").setSize(0).get(), docCount);

        // Start concurrent updates during rolling restart
        logger.info("--> starting rolling restart with concurrent updates");

        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger successfulUpdates = new AtomicInteger(0);
        final CountDownLatch updateDocLatch = new CountDownLatch(docCount / 3);
        final Thread updateThread = new Thread(() -> {
            while (stop.get() == false) {
                try {
                    // Update documents sequentially to avoid conflicts
                    for (int i = 0; i < docCount && !stop.get(); i++) {
                        client().prepareUpdate("test", String.valueOf(i))
                            .setRetryOnConflict(3)
                            .setDoc("counter", successfulUpdates.get() + 1, "last_updated", System.currentTimeMillis(), "version", 1)
                            .execute()
                            .actionGet(TimeValue.timeValueSeconds(5));
                        successfulUpdates.incrementAndGet();
                        updateDocLatch.countDown();
                        Thread.sleep(50); // Larger delay between updates
                    }
                } catch (Exception e) {
                    logger.warn("Error in background update thread", e);
                }
            }
        });

        try {
            // Add replicas
            assertAcked(
                client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.number_of_replicas", 1))
            );

            // Start additional nodes
            internalCluster().startNode();
            ensureGreen("test");

            // Start updates after cluster is stable
            updateThread.start();
            // Wait for fix number of updates to go through
            updateDocLatch.await();

            // Rolling restart of all nodes
            for (String node : internalCluster().getNodeNames()) {
                // Stop updates temporarily during node restart
                stop.set(true);
                Thread.sleep(1000); // Wait for in-flight operations to complete

                internalCluster().restartNode(node);
                ensureGreen(TimeValue.timeValueSeconds(60));

                // Verify data consistency
                refresh("test");
                verifyDerivedSourceWithUpdates(docCount);

                // Resume updates
                stop.set(false);
            }

        } finally {
            // Clean shutdown
            stop.set(true);
            updateThread.join(TimeValue.timeValueSeconds(30).millis());
            if (updateThread.isAlive()) {
                updateThread.interrupt();
                updateThread.join(TimeValue.timeValueSeconds(5).millis());
            }
        }

        logger.info("--> performed {} successful updates during rolling restart", successfulUpdates.get());
        refresh("test");
        flush("test");
        verifyDerivedSourceWithUpdates(docCount);
    }

    private void verifyDerivedSourceWithUpdates(int expectedDocs) throws Exception {
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch("test")
                .setSize(expectedDocs)
                .addSort("last_updated", SortOrder.DESC) // Sort by version to ensure we see latest updates
                .get();
            assertHitCount(response, expectedDocs);

            for (SearchHit hit : response.getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();
                String id = hit.getId();

                // Verify all required fields are present
                assertEquals("text value " + id, source.get("text_field"));
                assertNotNull("counter missing for doc " + id, source.get("counter"));
                assertFalse(((String) source.get("last_updated")).isEmpty());
                Integer counter = (Integer) source.get("counter");
                assertEquals(counter == 0 ? 0 : 1, source.get("version"));

                // Verify text_field maintains original value
                assertEquals("text value " + id, source.get("text_field"));
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void bulkIndexDocuments(int docCount) throws Exception {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        for (int i = 0; i < docCount; i++) {
            bulkRequest.add(
                client().prepareIndex("test")
                    .setId(String.valueOf(i))
                    .setSource(
                        "text_field",
                        "text value " + i,
                        "keyword_field",
                        "key_" + i,
                        "numeric_field",
                        i,
                        "date_field",
                        System.currentTimeMillis()
                    )
            );

            if (i % 100 == 0) {
                bulkRequest.execute().actionGet();
                bulkRequest = client().prepareBulk();
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            bulkRequest.execute().actionGet();
        }
        refresh();
    }

    private void bulkIndexMultiFieldDocuments(int docCount, int startingId) throws Exception {
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        for (int i = 0; i < docCount; i++) {
            int id = startingId + i;
            Map<String, Object> multiFieldObj = new HashMap<>();
            multiFieldObj.put("keyword_field", "keyword_" + id);
            multiFieldObj.put("long_field", id);
            multiFieldObj.put("boolean_field", id % 2 == 0);

            bulkRequest.add(
                client().prepareIndex("test")
                    .setId(String.valueOf(id))
                    .setSource("text_field", "text value " + id, "multi_field", multiFieldObj)
            );

            if (i % 100 == 0) {
                BulkResponse response = bulkRequest.execute().actionGet();
                assertFalse(response.hasFailures());
                bulkRequest = client().prepareBulk();
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse response = bulkRequest.execute().actionGet();
            assertFalse(response.hasFailures());
        }
    }

    private void verifyDerivedSourceWithMultiField(int expectedDocs) {
        // Search with preference to primary to ensure consistency
        SearchResponse response = client().prepareSearch("test")
            .setSize(expectedDocs)
            .setPreference("_primary")
            .addSort("multi_field.long_field", SortOrder.ASC)
            .get();

        assertHitCount(response, expectedDocs);

        int previousId = -1;
        for (SearchHit hit : response.getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();
            Map<String, Object> multiField = (Map<String, Object>) source.get("multi_field");

            int currentId = ((Number) multiField.get("long_field")).intValue();
            assertTrue("Documents should be in order", currentId > previousId);
            previousId = currentId;

            assertEquals("text value " + currentId, source.get("text_field"));
            assertEquals("keyword_" + currentId, multiField.get("keyword_field"));
            assertEquals(currentId % 2 == 0, multiField.get("boolean_field"));
        }
    }

    private void rollingRestartWithVerification(int initialDocCount) throws Exception {
        final String healthTimeout = "1m";
        int currentNodes = 1;

        // Add replicas
        assertAcked(
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 2))
        );

        // Add nodes
        for (int i = 0; i < 2; i++) {
            internalCluster().startNode();
            currentNodes++;
            assertTimeout(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(healthTimeout)
                    .setWaitForNoRelocatingShards(true)
                    .setWaitForNodes(String.valueOf(currentNodes))
            );
            verifyDerivedSource(initialDocCount);
        }

        ensureGreen();

        // Remove nodes
        for (int i = 0; i < 2; i++) {
            internalCluster().stopRandomDataNode();
            currentNodes--;
            assertTimeout(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(healthTimeout)
                    .setWaitForNodes(String.valueOf(currentNodes))
                    .setWaitForYellowStatus()
            );
            verifyDerivedSource(initialDocCount);
        }
    }

    private void verifyDerivedSource(int expectedDocs) throws Exception {
        refresh();
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch("test").setSize(expectedDocs).addSort("numeric_field", SortOrder.ASC).get();
            assertHitCount(response, expectedDocs);

            for (SearchHit hit : response.getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();
                String id = hit.getId();
                int docId = Integer.parseInt(id);

                assertEquals("text value " + docId, source.get("text_field"));
                assertEquals("key_" + docId, source.get("keyword_field"));
                assertEquals(docId, source.get("numeric_field"));
                assertNotNull(source.get("date_field"));
            }
        });
    }
}
