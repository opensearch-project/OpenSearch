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

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.tests.util.English;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLease;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.MockIndexEventListener;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.test.transport.StubbableTransport;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RelocationIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public RelocationIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return replicationSettings;
    }

    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockTransportService.TestPlugin.class, MockIndexEventListener.TestPlugin.class);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        assertActiveCopiesEstablishedPeerRecoveryRetentionLeases();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            // sync global checkpoint quickly so we can verify seq_no_stats aligned between all copies after tests.
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
    }

    public void testSimpleRelocationNoIndexing() {
        logger.info("--> starting [node1] ...");
        final String node_1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush so we have an actual index");
        client().admin().indices().prepareFlush().execute().actionGet();
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        logger.info("--> verifying count");
        refreshAndWaitForReplication();
        assertThat(client().prepareSearch("test").setSize(0).execute().actionGet().getHits().getTotalHits().value, equalTo(20L));

        logger.info("--> start another node");
        final String node_2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_2)).execute().actionGet();

        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count again...");
        refreshAndWaitForReplication();
        assertThat(client().prepareSearch("test").setSize(0).execute().actionGet().getHits().getTotalHits().value, equalTo(20L));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/2063")
    public void testRelocationWhileIndexingRandom() throws Exception {
        int numberOfRelocations = scaledRandomIntBetween(1, rarely() ? 10 : 4);
        int numberOfReplicas = randomBoolean() ? 0 : 1;
        int numberOfNodes = numberOfReplicas == 0 ? 2 : 3;

        logger.info(
            "testRelocationWhileIndexingRandom(numRelocations={}, numberOfReplicas={}, numberOfNodes={})",
            numberOfRelocations,
            numberOfReplicas,
            numberOfNodes
        );

        String[] nodes = new String[numberOfNodes];
        logger.info("--> starting [node1] ...");
        nodes[0] = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate("test", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", numberOfReplicas)).get();

        for (int i = 2; i <= numberOfNodes; i++) {
            logger.info("--> starting [node{}] ...", i);
            nodes[i - 1] = internalCluster().startNode();
            if (i != numberOfNodes) {
                ClusterHealthResponse healthResponse = client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(Integer.toString(i))
                    .setWaitForGreenStatus()
                    .execute()
                    .actionGet();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            }
        }

        int numDocs = scaledRandomIntBetween(200, 2500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", MapperService.SINGLE_MAPPING_NAME, client(), numDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);

            logger.info("--> starting relocations...");
            int nodeShiftBased = numberOfReplicas; // if we have replicas shift those
            for (int i = 0; i < numberOfRelocations; i++) {
                int fromNode = (i % 2);
                int toNode = fromNode == 0 ? 1 : 0;
                fromNode += nodeShiftBased;
                toNode += nodeShiftBased;
                numDocs = scaledRandomIntBetween(200, 1000);
                logger.debug("--> Allow indexer to index [{}] documents", numDocs);
                indexer.continueIndexing(numDocs);
                logger.info("--> START relocate the shard from {} to {}", nodes[fromNode], nodes[toNode]);
                client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, nodes[fromNode], nodes[toNode])).get();
                if (rarely()) {
                    logger.debug("--> flushing");
                    client().admin().indices().prepareFlush().get();
                }
                ClusterHealthResponse clusterHealthResponse = client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNoRelocatingShards(true)
                    .setTimeout(ACCEPTABLE_RELOCATION_TIME)
                    .execute()
                    .actionGet();
                assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
                indexer.pauseIndexing();
                logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);
            }
            logger.info("--> done relocations");
            logger.info("--> waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            refreshAndWaitForReplication("test");
            logger.info("--> searching the index");
            boolean ranOnce = false;
            for (int i = 0; i < 10; i++) {
                logger.info("--> START search test round {}", i + 1);
                SearchHits hits = client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .setSize((int) indexer.totalIndexedDocs())
                    .storedFields()
                    .execute()
                    .actionGet()
                    .getHits();
                ranOnce = true;
                if (hits.getTotalHits().value != indexer.totalIndexedDocs()) {
                    int[] hitIds = new int[(int) indexer.totalIndexedDocs()];
                    for (int hit = 0; hit < indexer.totalIndexedDocs(); hit++) {
                        hitIds[hit] = hit + 1;
                    }
                    Set<Integer> set = Arrays.stream(hitIds).boxed().collect(Collectors.toSet());
                    for (SearchHit hit : hits.getHits()) {
                        int id = Integer.parseInt(hit.getId());
                        if (set.remove(id) == false) {
                            logger.error("Extra id [{}]", id);
                        }
                    }
                    set.forEach(value -> logger.error("Missing id [{}]", value));
                }
                assertThat(hits.getTotalHits().value, equalTo(indexer.totalIndexedDocs()));
                logger.info("--> DONE search test round {}", i + 1);

            }
            if (ranOnce == false) {
                fail();
            }
        }
    }

    public void testRelocationWhileRefreshing() throws Exception {
        int numberOfRelocations = scaledRandomIntBetween(1, rarely() ? 10 : 4);
        int numberOfReplicas = randomBoolean() ? 0 : 1;
        int numberOfNodes = numberOfReplicas == 0 ? 2 : 3;

        logger.info(
            "testRelocationWhileIndexingRandom(numRelocations={}, numberOfReplicas={}, numberOfNodes={})",
            numberOfRelocations,
            numberOfReplicas,
            numberOfNodes
        );

        String[] nodes = new String[numberOfNodes];
        logger.info("--> starting [node_0] ...");
        nodes[0] = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            "test",
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", numberOfReplicas)
                // we want to control refreshes
                .put("index.refresh_interval", -1)
        ).get();

        for (int i = 1; i < numberOfNodes; i++) {
            logger.info("--> starting [node_{}] ...", i);
            nodes[i] = internalCluster().startNode();
            if (i != numberOfNodes - 1) {
                ClusterHealthResponse healthResponse = client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(Integer.toString(i + 1))
                    .setWaitForGreenStatus()
                    .execute()
                    .actionGet();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            }
        }

        final Semaphore postRecoveryShards = new Semaphore(0);
        final IndexEventListener listener = new IndexEventListener() {
            @Override
            public void indexShardStateChanged(
                IndexShard indexShard,
                @Nullable IndexShardState previousState,
                IndexShardState currentState,
                @Nullable String reason
            ) {
                if (currentState == IndexShardState.POST_RECOVERY) {
                    postRecoveryShards.release();
                }
            }
        };
        for (MockIndexEventListener.TestEventListener eventListener : internalCluster().getInstances(
            MockIndexEventListener.TestEventListener.class
        )) {
            eventListener.setNewDelegate(listener);
        }

        logger.info("--> starting relocations...");
        int nodeShiftBased = numberOfReplicas; // if we have replicas shift those
        for (int i = 0; i < numberOfRelocations; i++) {
            int fromNode = (i % 2);
            int toNode = fromNode == 0 ? 1 : 0;
            fromNode += nodeShiftBased;
            toNode += nodeShiftBased;

            List<IndexRequestBuilder> builders1 = new ArrayList<>();
            for (int numDocs = randomIntBetween(10, 30); numDocs > 0; numDocs--) {
                builders1.add(client().prepareIndex("test").setSource("{}", MediaTypeRegistry.JSON));
            }

            List<IndexRequestBuilder> builders2 = new ArrayList<>();
            for (int numDocs = randomIntBetween(10, 30); numDocs > 0; numDocs--) {
                builders2.add(client().prepareIndex("test").setSource("{}", MediaTypeRegistry.JSON));
            }

            logger.info("--> START relocate the shard from {} to {}", nodes[fromNode], nodes[toNode]);

            client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, nodes[fromNode], nodes[toNode])).get();

            logger.debug("--> index [{}] documents", builders1.size());
            indexRandom(false, true, builders1);
            // wait for shard to reach post recovery
            postRecoveryShards.acquire(1);

            logger.debug("--> index [{}] documents", builders2.size());
            indexRandom(true, true, builders2);

            // verify cluster was finished.
            assertFalse(
                client().admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForNoRelocatingShards(true)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout("30s")
                    .get()
                    .isTimedOut()
            );
            logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);

            logger.debug("--> verifying all searches return the same number of docs");
            long expectedCount = -1;
            for (Client client : clients()) {
                SearchResponse response = client.prepareSearch("test").setPreference("_local").setSize(0).get();
                assertNoFailures(response);
                if (expectedCount < 0) {
                    expectedCount = response.getHits().getTotalHits().value;
                } else {
                    assertEquals(expectedCount, response.getHits().getTotalHits().value);
                }
            }

        }

    }

    public void testCancellationCleansTempFiles() throws Exception {
        final String indexName = "test";

        final String p_node = internalCluster().startNode();

        prepareCreate(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        ).get();

        internalCluster().startNode();
        internalCluster().startNode();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName).setSource("{}", MediaTypeRegistry.JSON));
        }
        indexRandom(true, requests);
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("3").setWaitForGreenStatus().get().isTimedOut());
        flush();

        int allowedFailures = randomIntBetween(3, 5); // the default of the `index.allocation.max_retries` is 5.
        logger.info("--> blocking recoveries from primary (allowed failures: [{}])", allowedFailures);
        CountDownLatch corruptionCount = new CountDownLatch(allowedFailures);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, p_node);
        MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, p_node);
        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (!node.equals(clusterService.localNode())) {
                mockTransportService.addSendBehavior(
                    internalCluster().getInstance(TransportService.class, node.getName()),
                    new RecoveryCorruption(corruptionCount)
                );
            }
        }

        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();

        corruptionCount.await();

        logger.info("--> stopping replica assignment");
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                )
        );

        logger.info("--> wait for all replica shards to be removed, on all nodes");
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                if (node.equals(p_node)) {
                    continue;
                }
                ClusterState state = client(node).admin().cluster().prepareState().setLocal(true).get().getState();
                assertThat(
                    node + " indicates assigned replicas",
                    state.getRoutingTable().index(indexName).shardsWithState(ShardRoutingState.UNASSIGNED).size(),
                    equalTo(1)
                );
            }
        });

        logger.info("--> verifying no temporary recoveries are left");
        for (String node : internalCluster().getNodeNames()) {
            NodeEnvironment nodeEnvironment = internalCluster().getInstance(NodeEnvironment.class, node);
            for (final Path shardLoc : nodeEnvironment.availableShardPaths(new ShardId(indexName, "_na_", 0))) {
                if (Files.exists(shardLoc)) {
                    assertBusy(() -> {
                        try {
                            Files.walkFileTree(shardLoc, new SimpleFileVisitor<Path>() {
                                @Override
                                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                    assertThat(
                                        "found a temporary recovery file: " + file,
                                        file.getFileName().toString(),
                                        not(startsWith("recovery."))
                                    );
                                    return FileVisitResult.CONTINUE;
                                }
                            });
                        } catch (IOException e) {
                            throw new AssertionError("failed to walk file tree starting at [" + shardLoc + "]", e);
                        }
                    });
                }
            }
        }
    }

    public void testIndexSearchAndRelocateConcurrently() throws Exception {
        int halfNodes = randomIntBetween(1, 3);
        Settings[] nodeSettings = Stream.concat(
            Stream.generate(() -> Settings.builder().put("node.attr.color", "blue").build()).limit(halfNodes),
            Stream.generate(() -> Settings.builder().put("node.attr.color", "red").build()).limit(halfNodes)
        ).toArray(Settings[]::new);
        List<String> nodes = internalCluster().startNodes(nodeSettings);
        String[] blueNodes = nodes.subList(0, halfNodes).stream().toArray(String[]::new);
        String[] redNodes = nodes.subList(halfNodes, nodes.size()).stream().toArray(String[]::new);
        logger.info("blue nodes: {}", (Object) blueNodes);
        logger.info("red nodes: {}", (Object) redNodes);
        ensureStableCluster(halfNodes * 2);

        final Settings.Builder settings = Settings.builder()
            .put("index.routing.allocation.exclude.color", "blue")
            .put(indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(halfNodes - 1));
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), randomIntBetween(1, 10) + "s");
        }
        assertAcked(prepareCreate("test", settings));
        assertAllShardsOnNodes("test", redNodes);
        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread[] searchThreads = randomBoolean() ? new Thread[0] : new Thread[randomIntBetween(1, 4)];
        for (int i = 0; i < searchThreads.length; i++) {
            searchThreads[i] = new Thread(() -> {
                while (stopped.get() == false) {
                    assertNoFailures(client().prepareSearch("test").setRequestCache(false).get());
                }
            });
            searchThreads[i].start();
        }
        int numDocs = randomIntBetween(100, 150);
        ArrayList<String> ids = new ArrayList<>();
        logger.info(" --> indexing [{}] docs", numDocs);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String id = randomRealisticUnicodeOfLength(10) + String.valueOf(i);
            ids.add(id);
            docs[i] = client().prepareIndex("test").setId(id).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);
        SearchResponse countResponse = client().prepareSearch("test").get();
        assertHitCount(countResponse, numDocs);

        logger.info(" --> moving index to new nodes");
        Settings build = Settings.builder()
            .put("index.routing.allocation.exclude.color", "red")
            .put("index.routing.allocation.include.color", "blue")
            .build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).execute().actionGet();

        // index while relocating
        logger.info(" --> indexing [{}] more docs", numDocs);
        for (int i = 0; i < numDocs; i++) {
            String id = randomRealisticUnicodeOfLength(10) + String.valueOf(numDocs + i);
            ids.add(id);
            docs[i] = client().prepareIndex("test").setId(id).setSource("field1", English.intToEnglish(numDocs + i));
        }
        indexRandom(true, docs);

        logger.info(" --> waiting for relocation to complete");
        ensureGreen(TimeValue.timeValueSeconds(60), "test"); // move all shards to the new nodes (it waits on relocation)

        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            logger.info(" --> checking iteration {}", i);
            SearchResponse afterRelocation = client().prepareSearch().setSize(ids.size()).get();
            assertNoFailures(afterRelocation);
            assertSearchHits(afterRelocation, ids.toArray(new String[0]));
        }
        stopped.set(true);
        for (Thread searchThread : searchThreads) {
            searchThread.join();
        }
    }

    public void testRelocateWhileWaitingForRefresh() {
        logger.info("--> starting [node1] ...");
        final String node1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            "test",
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                // we want to control refreshes
                .put("index.refresh_interval", -1)
        ).get();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush so we have an actual index");
        client().admin().indices().prepareFlush().execute().actionGet();
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .setSource("field", "value" + i)
                .execute();
        }

        logger.info("--> start another node");
        final String node2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node1, node2)).execute().actionGet();

        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count");
        refreshAndWaitForReplication();
        assertThat(client().prepareSearch("test").setSize(0).execute().actionGet().getHits().getTotalHits().value, equalTo(20L));
    }

    public void testRelocateWhileContinuouslyIndexingAndWaitingForRefresh() throws Exception {
        logger.info("--> starting [node1] ...");
        final String node1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            "test",
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).put("index.refresh_interval", -1) // we
                                                                                                                                     // want
                                                                                                                                     // to
                                                                                                                                     // control
                                                                                                                                     // refreshes
        ).get();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush so we have an actual index");
        client().admin().indices().prepareFlush().execute().actionGet();
        logger.info("--> index more docs so we have something in the translog");
        final List<ActionFuture<IndexResponse>> pendingIndexResponses = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            pendingIndexResponses.add(
                client().prepareIndex("test")
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }

        logger.info("--> start another node");
        final String node2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        ActionFuture<ClusterRerouteResponse> relocationListener = client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand("test", 0, node1, node2))
            .execute();
        logger.info("--> index 100 docs while relocating");
        for (int i = 20; i < 120; i++) {
            pendingIndexResponses.add(
                client().prepareIndex("test")
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }
        relocationListener.actionGet();
        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count");
        assertBusy(() -> {
            refreshAndWaitForReplication();
            assertTrue(pendingIndexResponses.stream().allMatch(ActionFuture::isDone));
        }, 1, TimeUnit.MINUTES);

        assertThat(client().prepareSearch("test").setSize(0).execute().actionGet().getHits().getTotalHits().value, equalTo(120L));
    }

    public void testRelocationEstablishedPeerRecoveryRetentionLeases() throws Exception {
        int halfNodes = randomIntBetween(1, 3);
        String indexName = "test";
        Settings[] nodeSettings = Stream.concat(
            Stream.generate(() -> Settings.builder().put("node.attr.color", "blue").build()).limit(halfNodes),
            Stream.generate(() -> Settings.builder().put("node.attr.color", "red").build()).limit(halfNodes)
        ).toArray(Settings[]::new);
        List<String> nodes = internalCluster().startNodes(nodeSettings);
        String[] blueNodes = nodes.subList(0, halfNodes).toArray(new String[0]);
        String[] redNodes = nodes.subList(halfNodes, nodes.size()).toArray(new String[0]);
        ensureStableCluster(halfNodes * 2);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, halfNodes - 1))
                        .put("index.routing.allocation.include.color", "blue")
                )
        );
        ensureGreen("test");
        assertBusy(() -> assertAllShardsOnNodes(indexName, blueNodes));
        assertActiveCopiesEstablishedPeerRecoveryRetentionLeases();
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.include.color", "red"))
            .get();
        assertBusy(() -> assertAllShardsOnNodes(indexName, redNodes));
        ensureGreen("test");
        assertActiveCopiesEstablishedPeerRecoveryRetentionLeases();
    }

    private void assertActiveCopiesEstablishedPeerRecoveryRetentionLeases() throws Exception {
        assertBusy(() -> {
            for (final String it : client().admin().cluster().prepareState().get().getState().metadata().indices().keySet()) {
                Map<ShardId, List<ShardStats>> byShardId = Stream.of(client().admin().indices().prepareStats(it).get().getShards())
                    .collect(Collectors.groupingBy(l -> l.getShardRouting().shardId()));
                for (List<ShardStats> shardStats : byShardId.values()) {
                    Set<String> expectedLeaseIds = shardStats.stream()
                        .map(s -> ReplicationTracker.getPeerRecoveryRetentionLeaseId(s.getShardRouting()))
                        .collect(Collectors.toSet());
                    for (ShardStats shardStat : shardStats) {
                        Set<String> actualLeaseIds = shardStat.getRetentionLeaseStats()
                            .retentionLeases()
                            .leases()
                            .stream()
                            .map(RetentionLease::id)
                            .collect(Collectors.toSet());
                        assertThat(expectedLeaseIds, everyItem(in(actualLeaseIds)));
                    }
                }
            }
        });
    }

    class RecoveryCorruption implements StubbableTransport.SendRequestBehavior {

        private final CountDownLatch corruptionCount;

        RecoveryCorruption(CountDownLatch corruptionCount) {
            this.corruptionCount = corruptionCount;
        }

        @Override
        public void sendRequest(
            Transport.Connection connection,
            long requestId,
            String action,
            TransportRequest request,
            TransportRequestOptions options
        ) throws IOException {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                FileChunkRequest chunkRequest = (FileChunkRequest) request;
                if (chunkRequest.name().startsWith(IndexFileNames.SEGMENTS)) {
                    // corrupting the segments_N files in order to make sure future recovery re-send files
                    logger.debug("corrupting [{}] to {}. file name: [{}]", action, connection.getNode(), chunkRequest.name());
                    assert chunkRequest.content().toBytesRef().bytes == chunkRequest.content().toBytesRef().bytes
                        : "no internal reference!!";
                    byte[] array = chunkRequest.content().toBytesRef().bytes;
                    array[0] = (byte) ~array[0]; // flip one byte in the content
                    corruptionCount.countDown();
                }
                connection.sendRequest(requestId, action, request, options);
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        }
    }
}
