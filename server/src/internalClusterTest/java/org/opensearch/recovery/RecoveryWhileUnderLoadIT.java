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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.translog.Translog;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;

public class RecoveryWhileUnderLoadIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public RecoveryWhileUnderLoadIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return replicationSettings;
    }

    private final Logger logger = LogManager.getLogger(RecoveryWhileUnderLoadIT.class);

    public static final class RetentionLeaseSyncIntervalSettingPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING);
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(RetentionLeaseSyncIntervalSettingPlugin.class))
            .collect(Collectors.toList());
    }

    public void testRecoverWhileUnderLoadAllocateReplicasTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate(
                "test",
                1,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            )
        );

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            client().admin().indices().prepareFlush().execute().actionGet();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);

            logger.info("--> allow 2 nodes for index [test] ...");
            // now start another node, while we index
            allowNodes("test", 2);

            logger.info("--> waiting for GREEN health status ...");
            // make sure the cluster state is green, and all has been recovered
            assertNoTimeout(
                client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus()
            );

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            assertAfterRefreshAndWaitForReplication();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, 10, indexer.getIds());
        }
    }

    public void testRecoverWhileUnderLoadAllocateReplicasRelocatePrimariesTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate(
                "test",
                1,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            )
        );

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            client().admin().indices().prepareFlush().execute().actionGet();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 4 nodes for index [test] ...");
            allowNodes("test", 4);

            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(
                client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus()
            );

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            assertAfterRefreshAndWaitForReplication();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, 10, indexer.getIds());
        }
    }

    public void testRecoverWhileUnderLoadWithReducedAllowedNodes() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate(
                "test",
                2,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            )
        );

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            client().admin().indices().prepareFlush().execute().actionGet();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            // now start more nodes, while we index
            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 4 nodes for index [test] ...");
            allowNodes("test", 4);

            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout("5m")
                    .setWaitForGreenStatus()
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();

            logger.info("--> {} docs indexed", totalNumDocs);
            // now, shutdown nodes
            logger.info("--> allow 3 nodes for index [test] ...");
            allowNodes("test", 3);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout("5m")
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> allow 2 nodes for index [test] ...");
            allowNodes("test", 2);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout("5m")
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> allow 1 nodes for index [test] ...");
            allowNodes("test", 1);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout("5m")
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            assertNoTimeout(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout("5m")
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> refreshing the index");
            assertAfterRefreshAndWaitForReplication();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, 10, indexer.getIds());
        }
    }

    public void testRecoverWhileRelocating() throws Exception {
        final int numShards = between(2, 5);
        final int numReplicas = 0;
        logger.info("--> creating test index ...");
        int allowNodes = 2;
        assertAcked(
            prepareCreate(
                "test",
                3,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, numReplicas)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), randomFrom("100ms", "1s", "5s", "30s", "60s"))
            )
        );

        final int numDocs = scaledRandomIntBetween(200, 9999);

        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), numDocs)) {

            for (int i = 0; i < numDocs; i += scaledRandomIntBetween(100, Math.min(1000, numDocs))) {
                indexer.assertNoFailures();
                logger.info("--> waiting for {} docs to be indexed ...", i);
                waitForDocs(i, indexer);
                logger.info("--> {} docs indexed", i);
                allowNodes = 2 / allowNodes;
                allowNodes("test", allowNodes);
                logger.info("--> waiting for GREEN health status ...");
                ensureGreen(TimeValue.timeValueMinutes(5));
            }

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();

            logger.info("--> indexing threads stopped");
            logger.info("--> bump up number of replicas to 1 and allow all nodes to hold the index");
            allowNodes("test", 3);
            assertAcked(
                client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("number_of_replicas", 1)).get()
            );
            ensureGreen(TimeValue.timeValueMinutes(5));

            logger.info("--> refreshing the index");
            assertAfterRefreshAndWaitForReplication();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numShards, 10, indexer.getIds());
        }
    }

    private void iterateAssertCount(final int numberOfShards, final int iterations, final Set<String> ids) throws Exception {
        final long numberOfDocs = ids.size();
        SearchResponse[] iterationResults = new SearchResponse[iterations];
        boolean error = false;
        for (int i = 0; i < iterations; i++) {
            SearchResponse searchResponse = client().prepareSearch()
                .setSize((int) numberOfDocs)
                .setQuery(matchAllQuery())
                .setTrackTotalHits(true)
                .addSort("id", SortOrder.ASC)
                .get();
            logSearchResponse(numberOfShards, numberOfDocs, i, searchResponse);
            iterationResults[i] = searchResponse;
            if (searchResponse.getHits().getTotalHits().value() != numberOfDocs) {
                error = true;
            }
        }

        if (error) {
            // Printing out shards and their doc count
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().get();
            for (ShardStats shardStats : indicesStatsResponse.getShards()) {
                DocsStats docsStats = shardStats.getStats().docs;
                logger.info(
                    "shard [{}] - count {}, primary {}",
                    shardStats.getShardRouting().id(),
                    docsStats.getCount(),
                    shardStats.getShardRouting().primary()
                );
            }

            ClusterService clusterService = clusterService();
            final ClusterState state = clusterService.state();
            for (int shard = 0; shard < numberOfShards; shard++) {
                for (String id : ids) {
                    ShardId docShard = clusterService.operationRouting().shardId(state, "test", id, null);
                    if (docShard.id() == shard) {
                        for (ShardRouting shardRouting : state.routingTable().shardRoutingTable("test", shard)) {
                            GetResponse response = client().prepareGet("test", id)
                                .setPreference("_only_nodes:" + shardRouting.currentNodeId())
                                .get();
                            if (response.isExists()) {
                                logger.info("missing id [{}] on shard {}", id, shardRouting);
                            }
                        }
                    }
                }
            }

            // if there was an error we try to wait and see if at some point it'll get fixed
            logger.info("--> trying to wait");
            assertBusy(() -> {
                boolean errorOccurred = false;
                for (int i = 0; i < iterations; i++) {
                    SearchResponse searchResponse = client().prepareSearch()
                        .setTrackTotalHits(true)
                        .setSize(0)
                        .setQuery(matchAllQuery())
                        .get();
                    if (searchResponse.getHits().getTotalHits().value() != numberOfDocs) {
                        errorOccurred = true;
                    }
                }
                assertFalse("An error occurred while waiting", errorOccurred);
            }, 5, TimeUnit.MINUTES);
            assertEquals(numberOfDocs, ids.size());
        }

        // lets now make the test fail if it was supposed to fail
        for (int i = 0; i < iterations; i++) {
            assertHitCount(iterationResults[i], numberOfDocs);
        }
    }

    private void logSearchResponse(int numberOfShards, long numberOfDocs, int iteration, SearchResponse searchResponse) {
        logger.info(
            "iteration [{}] - successful shards: {} (expected {})",
            iteration,
            searchResponse.getSuccessfulShards(),
            numberOfShards
        );
        logger.info("iteration [{}] - failed shards: {} (expected 0)", iteration, searchResponse.getFailedShards());
        if (CollectionUtils.isEmpty(searchResponse.getShardFailures()) == false) {
            logger.info("iteration [{}] - shard failures: {}", iteration, Arrays.toString(searchResponse.getShardFailures()));
        }
        logger.info(
            "iteration [{}] - returned documents: {} (expected {})",
            iteration,
            searchResponse.getHits().getTotalHits().value(),
            numberOfDocs
        );
    }

    private void assertAfterRefreshAndWaitForReplication() throws Exception {
        assertBusy(() -> {
            RefreshResponse actionGet = client().admin().indices().prepareRefresh().get();
            assertAllSuccessful(actionGet);
        }, 5, TimeUnit.MINUTES);
        waitForReplication();
    }

    public void testRecoveryWithDerivedSourceEnabled() throws Exception {
        logger.info("--> creating test index with derived source enabled...");
        int numberOfShards = numberOfShards();
        String mapping = """
            {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "age": {
                  "type": "integer"
                }
              }
            }""";

        assertAcked(
            prepareCreate(
                "test",
                1,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            ).setMapping(mapping)
        );

        final int totalNumDocs = scaledRandomIntBetween(200, 1000);
        for (int i = 0; i < totalNumDocs; i++) {
            if (i % 2 == 0) {
                client().prepareIndex("test").setId(String.valueOf(i)).setSource("name", "test" + i, "age", i).get();
            } else {
                client().prepareIndex("test").setId(String.valueOf(i)).setSource("age", i, "name", "test" + i).get();
            }

            if (i % 100 == 0) {
                // Occasionally flush to create new segments
                client().admin().indices().prepareFlush("test").setForce(true).get();
            }
        }

        logger.info("--> allow 2 nodes for index [test] with replica ...");
        allowNodes("test", 2);

        logger.info("--> waiting for GREEN health status ...");
        ensureGreen(TimeValue.timeValueMinutes(2));

        // Verify documents on replica
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch("test").setPreference("_replica").setSize(totalNumDocs).get();
            assertHitCount(searchResponse, totalNumDocs);

            // Verify derived source reconstruction
            for (SearchHit hit : searchResponse.getHits()) {
                assertNotNull(hit.getSourceAsMap());
                assertEquals(2, hit.getSourceAsMap().size());
                assertNotNull(hit.getSourceAsMap().get("name"));
                assertNotNull(hit.getSourceAsMap().get("age"));
            }
        });
    }

    public void testReplicaRecoveryWithDerivedSourceBeforeRefresh() throws Exception {
        logger.info("--> creating test index with derived source enabled...");
        String mapping = """
            {
              "properties": {
                "timestamp": {
                  "type": "date"
                },
                "ip": {
                  "type": "ip"
                }
              }
            }""";

        assertAcked(
            prepareCreate(
                "test",
                3,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                    .put("index.refresh_interval", -1)
            ).setMapping(mapping)
        );

        // Index documents without refresh
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            if (i % 2 == 0) {
                client().prepareIndex("test")
                    .setId(String.valueOf(i))
                    .setSource("timestamp", "2023-01-01T01:20:30." + String.valueOf(i % 10).repeat(3) + "Z", "ip", "192.168.1." + i)
                    .get();
            } else {
                client().prepareIndex("test")
                    .setId(String.valueOf(i))
                    .setSource("ip", "192.168.1." + i, "timestamp", "2023-01-01T01:20:30." + String.valueOf(i % 10).repeat(3) + "Z")
                    .get();
            }
        }

        // Add replica before refresh
        assertAcked(
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, 2))
        );

        ensureGreen(TimeValue.timeValueMinutes(2));

        // Verify documents on replica
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch("test").setPreference("_replica").setSize(docCount).get();
            assertHitCount(searchResponse, docCount);

            // Verify source reconstruction
            for (SearchHit hit : searchResponse.getHits()) {
                assertNotNull(hit.getSourceAsMap());
                assertEquals(2, hit.getSourceAsMap().size());
                String id = hit.getId();
                assertEquals(
                    "2023-01-01T01:20:30." + String.valueOf(Integer.valueOf(id) % 10).repeat(3) + "Z",
                    hit.getSourceAsMap().get("timestamp")
                );
                assertEquals("192.168.1." + id, hit.getSourceAsMap().get("ip"));
            }
        });
    }

    public void testReplicaRecoveryWithDerivedSourceFromTranslog() throws Exception {
        logger.info("--> creating test index with derived source enabled...");
        String mapping = """
            {
              "properties": {
                "coordinates": {
                  "type": "geo_point"
                },
                "value": {
                  "type": "text",
                  "store": true
                }
              }
            }""";

        // Create index with replica
        assertAcked(
            prepareCreate(
                "test",
                2,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            ).setMapping(mapping)
        );

        ensureGreen();

        // Index documents with immediate visibility
        int docCount = randomIntBetween(100, 200);
        for (int i = 0; i < docCount; i++) {
            if (i % 2 == 0) {
                client().prepareIndex("test")
                    .setId(String.valueOf(i))
                    .setSource("coordinates", Geohash.stringEncode(40.0 + i, 75.0 + i) + i, "value", "fox_" + i + " in the field")
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get();
            } else {
                client().prepareIndex("test")
                    .setId(String.valueOf(i))
                    .setSource("value", "fox_" + i + " in the field", "coordinates", Geohash.stringEncode(40.0 + i, 75.0 + i) + i)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .get();
            }
        }

        // Force flush to ensure documents are in segments
        client().admin().indices().prepareFlush("test").setForce(true).get();

        // Kill replica node and index more documents
        final String replicaNode = ensureReplicaNode("test");
        if (replicaNode != null) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));
        }

        int additionalDocs = randomIntBetween(50, 100);
        for (int i = docCount; i < docCount + additionalDocs; i++) {
            client().prepareIndex("test")
                .setId(String.valueOf(i))
                .setSource("coordinates", Geohash.stringEncode(40.0 + i, 75.0 + i) + i, "value", "fox_" + i + " in the field")
                .get();
        }

        // Restart replica node and verify recovery
        internalCluster().startNode();
        ensureGreen(TimeValue.timeValueMinutes(2));

        // Verify all documents on replica including those recovered from translog
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch("test")
                .setPreference("_replica")
                .setSize(docCount + additionalDocs)
                .get();
            assertHitCount(searchResponse, docCount + additionalDocs);

            // Verify source reconstruction for all documents
            for (SearchHit hit : searchResponse.getHits()) {
                assertNotNull(hit.getSourceAsMap());
                assertEquals(2, hit.getSourceAsMap().size());
                String id = hit.getId();
                assertNotNull(hit.getSourceAsMap().get("coordinates"));
                assertEquals("fox_" + id + " in the field", hit.getSourceAsMap().get("value"));
            }
        });
    }

    public void testRecoverWhileUnderLoadWithDerivedSource() throws Exception {
        logger.info("--> creating test index with derived source enabled...");
        String mapping = """
            {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "value": {
                  "type": "integer"
                },
                "timestamp": {
                  "type": "date"
                }
              }
            }""";

        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate(
                "test",
                1,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            ).setMapping(mapping)
        );

        final int totalNumDocs = scaledRandomIntBetween(2000, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;

        // Custom indexer for derived source documents
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", null, client(), extraDocs) {
            @Override
            protected XContentBuilder generateSource(long id, Random random) throws IOException {
                return jsonBuilder().startObject()
                    .field("name", "name_" + id)
                    .field("value", id)
                    .field("timestamp", System.currentTimeMillis())
                    .endObject();
            }
        }) {
            indexer.setUseAutoGeneratedIDs(true);
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            client().admin().indices().prepareFlush().execute().actionGet();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();

            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 2 nodes for index [test] ...");
            allowNodes("test", 2);

            logger.info("--> waiting for GREEN health status ...");
            // make sure the cluster state is green, and all has been recovered
            assertNoTimeout(
                client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus()
            );

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();

            logger.info("--> refreshing the index");
            client().admin().indices().prepareRefresh().get();

            logger.info("--> verifying indexed content");

            // Verify docs on primary
            SearchResponse primaryResponse = client().prepareSearch("test").setPreference("_primary").setTrackTotalHits(true).get();
            assertHitCount(primaryResponse, totalNumDocs);

            // Verify docs and derived source on replica
            assertBusy(() -> {
                SearchResponse replicaResponse = client().prepareSearch("test")
                    .setPreference("_replica")
                    .setTrackTotalHits(true)
                    .setSize(totalNumDocs)
                    .addSort("value", SortOrder.ASC)
                    .get();

                assertHitCount(replicaResponse, totalNumDocs);

                // Verify source reconstruction on replica
                for (SearchHit hit : replicaResponse.getHits()) {
                    assertNotNull(hit.getSourceAsMap());
                    assertEquals(3, hit.getSourceAsMap().size());
                    int id = (Integer) hit.getSourceAsMap().get("value");
                    assertEquals("name_" + id, hit.getSourceAsMap().get("name"));
                    assertNotNull(hit.getSourceAsMap().get("timestamp"));
                }
            }, 30, TimeUnit.SECONDS);

            // Additional source verification with random sampling
            assertRandomDocsSource(50);
        }
    }

    public void testRecoverWithRelocationAndDerivedSource() throws Exception {
        final int numShards = between(3, 5);
        logger.info("--> creating test index with derived source enabled...");

        String mapping = """
            {
              "properties": {
                "name": {
                  "type": "keyword"
                },
                "value": {
                  "type": "integer"
                },
                "timestamp": {
                  "type": "date"
                }
              }
            }""";

        assertAcked(
            prepareCreate(
                "test",
                1,
                Settings.builder()
                    .put(SETTING_NUMBER_OF_SHARDS, numShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                    .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
            ).setMapping(mapping)
        );
        int numNodes = 1;
        final int numDocs = scaledRandomIntBetween(1500, 2000);

        try (BackgroundIndexer indexer = new BackgroundIndexer("test", null, client(), numDocs) {
            @Override
            protected XContentBuilder generateSource(long id, Random random) throws IOException {
                return jsonBuilder().startObject()
                    .field("name", "name_" + id)
                    .field("value", id)
                    .field("timestamp", System.currentTimeMillis())
                    .endObject();
            }
        }) {

            indexer.setUseAutoGeneratedIDs(true);
            for (int i = 0; i < numDocs; i += scaledRandomIntBetween(500, Math.min(1000, numDocs))) {
                indexer.assertNoFailures();
                logger.info("--> waiting for {} docs to be indexed ...", i);
                waitForDocs(i, indexer);
                internalCluster().startDataOnlyNode();
                numNodes++;

                logger.info("--> waiting for GREEN health status ...");
                ensureGreen(TimeValue.timeValueMinutes(2));
            }

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();

            // Add replicas after stopping indexing
            logger.info("--> adding replicas ...");
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, numNodes - 1))
                    .get()
            );
            ensureGreen(TimeValue.timeValueMinutes(2));

            logger.info("--> refreshing the index");
            client().admin().indices().prepareRefresh().get();

            // Verify final doc count and derived source reconstruction
            SearchResponse primaryResponse = client().prepareSearch("test").setPreference("_primary").setTrackTotalHits(true).get();
            assertHitCount(primaryResponse, numDocs);

            assertBusy(() -> {
                SearchResponse replicaResponse = client().prepareSearch("test")
                    .setPreference("_replica")
                    .setTrackTotalHits(true)
                    .setSize(numDocs)
                    .addSort("value", SortOrder.ASC)
                    .get();

                assertHitCount(replicaResponse, numDocs);

                // Verify source reconstruction on replica
                for (SearchHit hit : replicaResponse.getHits()) {
                    assertNotNull(hit.getSourceAsMap());
                    assertEquals(3, hit.getSourceAsMap().size());
                    int id = (Integer) hit.getSourceAsMap().get("value");
                    assertEquals("name_" + id, hit.getSourceAsMap().get("name"));
                    assertNotNull(hit.getSourceAsMap().get("timestamp"));
                }
            }, 30, TimeUnit.SECONDS);

            assertRandomDocsSource(100);
        }
    }

    private void assertRandomDocsSource(int sampleSize) {
        // Random sampling of documents for detailed source verification
        for (int i = 0; i < sampleSize; i++) {
            String id = String.valueOf(randomIntBetween(0, 100));
            GetResponse getResponse = client().prepareGet("test", id).setPreference("_replica").get();

            if (getResponse.isExists()) {
                Map<String, Object> source = getResponse.getSourceAsMap();
                assertNotNull(source);
                assertEquals(3, source.size());
                assertEquals("name_" + id, source.get("name"));
                assertEquals(Integer.parseInt(id), source.get("value"));
                assertNotNull(source.get("timestamp"));
            }
        }
    }

    private String ensureReplicaNode(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        Index idx = state.metadata().index(index).getIndex();
        String replicaNode = state.routingTable().index(idx).shard(0).replicaShards().get(0).currentNodeId();
        String clusterManagerNode = internalCluster().getClusterManagerName();
        if (!replicaNode.equals(clusterManagerNode)) {
            state.nodes().get(replicaNode).getName();
        }
        return null;
    }
}
