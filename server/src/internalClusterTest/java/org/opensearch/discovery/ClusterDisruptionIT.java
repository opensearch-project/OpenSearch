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

package org.opensearch.discovery;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.OpenSearchException;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.coordination.ClusterBootstrapService;
import org.opensearch.cluster.coordination.LagDetector;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.VersionType;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.disruption.NetworkDisruption.Bridge;
import org.opensearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.opensearch.test.disruption.ServiceDisruptionScheme;
import org.opensearch.test.junit.annotations.TestIssueLogging;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

/**
 * Tests various cluster operations (e.g., indexing) during disruptions.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterDisruptionIT extends AbstractDisruptionTestCase {

    private enum ConflictMode {
        none,
        external,
        create;

        static ConflictMode randomMode() {
            ConflictMode[] values = values();
            return values[randomInt(values.length - 1)];
        }
    }

    /**
     * Test that we do not loose document whose indexing request was successful, under a randomly selected disruption scheme
     * We also collect &amp; report the type of indexing failures that occur.
     * <p>
     * This test is a superset of tests run in the Jepsen test suite, with the exception of versioned updates
     */
    @TestIssueLogging(value = "_root:DEBUG,org.opensearch.action.bulk:TRACE,org.opensearch.action.get:TRACE,"
        + "org.opensearch.discovery:TRACE,org.opensearch.action.support.replication:TRACE,"
        + "org.opensearch.cluster.service:TRACE,org.opensearch.indices.recovery:TRACE,"
        + "org.opensearch.indices.cluster:TRACE,org.opensearch.index.shard:TRACE", issueUrl = "https://github.com/elastic/elasticsearch/issues/41068")
    public void testAckedIndexing() throws Exception {

        final int seconds = !(TEST_NIGHTLY && rarely()) ? 1 : 5;
        final String timeout = seconds + "s";

        final List<String> nodes = startCluster(rarely() ? 5 : 3);

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
            )
        );
        ensureGreen();

        ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();
        logger.info("disruption scheme [{}] added", disruptionScheme);

        final ConcurrentHashMap<String, String> ackedDocs = new ConcurrentHashMap<>(); // id -> node sent.

        final AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> indexers = new ArrayList<>(nodes.size());
        List<Semaphore> semaphores = new ArrayList<>(nodes.size());
        final AtomicInteger idGenerator = new AtomicInteger(0);
        final AtomicReference<CountDownLatch> countDownLatchRef = new AtomicReference<>();
        final List<Exception> exceptedExceptions = new CopyOnWriteArrayList<>();

        final ConflictMode conflictMode = ConflictMode.randomMode();
        final List<String> fieldNames = IntStream.rangeClosed(0, randomInt(10)).mapToObj(n -> "f" + n).collect(Collectors.toList());

        logger.info("starting indexers using conflict mode " + conflictMode);
        try {
            for (final String node : nodes) {
                final Semaphore semaphore = new Semaphore(0);
                semaphores.add(semaphore);
                final Client client = client(node);
                final String name = "indexer_" + indexers.size();
                final int numPrimaries = getNumShards("test").numPrimaries;
                Thread thread = new Thread(() -> {
                    while (!stop.get()) {
                        String id = null;
                        try {
                            if (!semaphore.tryAcquire(10, TimeUnit.SECONDS)) {
                                continue;
                            }
                            logger.info("[{}] Acquired semaphore and it has {} permits left", name, semaphore.availablePermits());
                            try {
                                id = Integer.toString(idGenerator.incrementAndGet());
                                int shard = Math.floorMod(Murmur3HashFunction.hash(id), numPrimaries);
                                logger.trace("[{}] indexing id [{}] through node [{}] targeting shard [{}]", name, id, node, shard);
                                IndexRequestBuilder indexRequestBuilder = client.prepareIndex("test")
                                    .setId(id)
                                    .setSource(
                                        Collections.singletonMap(randomFrom(fieldNames), randomNonNegativeLong()),
                                        MediaTypeRegistry.JSON
                                    )
                                    .setTimeout(timeout);

                                if (conflictMode == ConflictMode.external) {
                                    indexRequestBuilder.setVersion(randomIntBetween(1, 10)).setVersionType(VersionType.EXTERNAL);
                                } else if (conflictMode == ConflictMode.create) {
                                    indexRequestBuilder.setCreate(true);
                                }

                                IndexResponse response = indexRequestBuilder.get(timeout);
                                assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                                ackedDocs.put(id, node);
                                logger.trace("[{}] indexed id [{}] through node [{}], response [{}]", name, id, node, response);
                            } catch (OpenSearchException e) {
                                exceptedExceptions.add(e);
                                final String docId = id;
                                logger.trace(() -> new ParameterizedMessage("[{}] failed id [{}] through node [{}]", name, docId, node), e);
                            } finally {
                                countDownLatchRef.get().countDown();
                                logger.trace("[{}] decreased counter : {}", name, countDownLatchRef.get().getCount());
                            }
                        } catch (InterruptedException e) {
                            // fine - semaphore interrupt
                        } catch (AssertionError | Exception e) {
                            logger.info(() -> new ParameterizedMessage("unexpected exception in background thread of [{}]", node), e);
                        }
                    }
                });

                thread.setName(name);
                thread.start();
                indexers.add(thread);
            }

            int docsPerIndexer = randomInt(3);
            logger.info("indexing {} docs per indexer before partition", docsPerIndexer);
            countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
            for (Semaphore semaphore : semaphores) {
                semaphore.release(docsPerIndexer);
            }
            assertTrue(countDownLatchRef.get().await(1, TimeUnit.MINUTES));

            for (int iter = 1 + randomInt(2); iter > 0; iter--) {
                logger.info("starting disruptions & indexing (iteration [{}])", iter);
                disruptionScheme.startDisrupting();

                docsPerIndexer = 1 + randomInt(5);
                logger.info("indexing {} docs per indexer during partition", docsPerIndexer);
                countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
                Collections.shuffle(semaphores, random());
                for (Semaphore semaphore : semaphores) {
                    assertThat(semaphore.availablePermits(), equalTo(0));
                    semaphore.release(docsPerIndexer);
                }
                logger.info("waiting for indexing requests to complete");
                assertTrue(countDownLatchRef.get().await(docsPerIndexer * seconds * 1000 + 2000, TimeUnit.MILLISECONDS));

                logger.info("stopping disruption");
                disruptionScheme.stopDisrupting();
                for (String node : internalCluster().getNodeNames()) {
                    ensureStableCluster(
                        nodes.size(),
                        TimeValue.timeValueMillis(disruptionScheme.expectedTimeToHeal().millis() + DISRUPTION_HEALING_OVERHEAD.millis()),
                        true,
                        node
                    );
                }
                // in case of a bridge partition, shard allocation can fail "index.allocation.max_retries" times if the cluster-manager
                // is the super-connected node and recovery source and target are on opposite sides of the bridge
                if (disruptionScheme instanceof NetworkDisruption
                    && ((NetworkDisruption) disruptionScheme).getDisruptedLinks() instanceof Bridge) {
                    assertBusy(() -> assertAcked(client().admin().cluster().prepareReroute().setRetryFailed(true)));
                }
                ensureGreen("test");

                logger.info("validating successful docs");
                assertBusy(() -> {
                    for (String node : nodes) {
                        try {
                            logger.debug("validating through node [{}] ([{}] acked docs)", node, ackedDocs.size());
                            for (String id : ackedDocs.keySet()) {
                                assertTrue(
                                    "doc [" + id + "] indexed via node [" + ackedDocs.get(id) + "] not found",
                                    client(node).prepareGet("test", id).setPreference("_local").get().isExists()
                                );
                            }
                        } catch (AssertionError | NoShardAvailableActionException e) {
                            throw new AssertionError(e.getMessage() + " (checked via node [" + node + "]", e);
                        }
                    }
                }, 30, TimeUnit.SECONDS);

                logger.info("done validating (iteration [{}])", iter);
            }
        } finally {
            logger.info("shutting down indexers");
            stop.set(true);
            for (Thread indexer : indexers) {
                indexer.interrupt();
                indexer.join(60000);
            }
            if (exceptedExceptions.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (Exception e : exceptedExceptions) {
                    sb.append("\n").append(e.getMessage());
                }
                logger.debug("Indexing exceptions during disruption: {}", sb);
            }
        }
    }

    /**
     * Test that a document which is indexed on the majority side of a partition, is available from the minority side,
     * once the partition is healed
     */
    public void testRejoinDocumentExistsInAllShardCopies() throws Exception {
        List<String> nodes = startCluster(3);

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            ).get()
        );
        ensureGreen("test");

        nodes = new ArrayList<>(nodes);
        Collections.shuffle(nodes, random());
        String isolatedNode = nodes.get(0);
        String notIsolatedNode = nodes.get(1);

        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption scheme = addRandomDisruptionType(partitions);
        scheme.startDisrupting();
        ensureStableCluster(2, notIsolatedNode);
        assertFalse(client(notIsolatedNode).admin().cluster().prepareHealth("test").setWaitForYellowStatus().get().isTimedOut());

        IndexResponse indexResponse = internalCluster().client(notIsolatedNode).prepareIndex("test").setSource("field", "value").get();
        assertThat(indexResponse.getVersion(), equalTo(1L));

        logger.info("Verifying if document exists via node[{}]", notIsolatedNode);
        GetResponse getResponse = internalCluster().client(notIsolatedNode)
            .prepareGet("test", indexResponse.getId())
            .setPreference("_local")
            .get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getVersion(), equalTo(1L));
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        scheme.stopDisrupting();

        ensureStableCluster(3);
        ensureGreen("test");

        for (String node : nodes) {
            logger.info("Verifying if document exists after isolating node[{}] via node[{}]", isolatedNode, node);
            getResponse = internalCluster().client(node).prepareGet("test", indexResponse.getId()).setPreference("_local").get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getVersion(), equalTo(1L));
            assertThat(getResponse.getId(), equalTo(indexResponse.getId()));
        }
    }

    // simulate handling of sending shard failure during an isolation
    public void testSendingShardFailure() throws Exception {
        List<String> nodes = startCluster(3);
        String clusterManagerNode = internalCluster().getClusterManagerName();
        List<String> nonClusterManagerNodes = nodes.stream().filter(node -> !node.equals(clusterManagerNode)).collect(Collectors.toList());
        String nonClusterManagerNode = randomFrom(nonClusterManagerNodes);
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            )
        );
        ensureGreen();
        String nonClusterManagerNodeId = internalCluster().clusterService(nonClusterManagerNode).localNode().getId();

        // fail a random shard
        ShardRouting failedShard = randomFrom(
            clusterService().state().getRoutingNodes().node(nonClusterManagerNodeId).shardsWithState(ShardRoutingState.STARTED)
        );
        ShardStateAction service = internalCluster().getInstance(ShardStateAction.class, nonClusterManagerNode);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean();

        String isolatedNode = randomBoolean() ? clusterManagerNode : nonClusterManagerNode;
        TwoPartitions partitions = isolateNode(isolatedNode);
        // we cannot use the NetworkUnresponsive disruption type here as it will swallow the "shard failed" request, calling neither
        // onSuccess nor onFailure on the provided listener.
        NetworkDisruption networkDisruption = new NetworkDisruption(partitions, NetworkDisruption.DISCONNECT);
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        service.localShardFailed(
            failedShard,
            "simulated",
            new CorruptIndexException("simulated", (String) null),
            new ActionListener<Void>() {
                @Override
                public void onResponse(final Void aVoid) {
                    success.set(true);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    success.set(false);
                    latch.countDown();
                    assert false;
                }
            }
        );

        if (isolatedNode.equals(nonClusterManagerNode)) {
            assertNoClusterManager(nonClusterManagerNode);
        } else {
            ensureStableCluster(2, nonClusterManagerNode);
        }

        // heal the partition
        networkDisruption.removeAndEnsureHealthy(internalCluster());

        // the cluster should stabilize
        ensureStableCluster(3);

        latch.await();

        // the listener should be notified
        assertTrue(success.get());

        // the failed shard should be gone
        List<ShardRouting> shards = clusterService().state().getRoutingTable().allShards("test");
        for (ShardRouting shard : shards) {
            assertThat(shard.allocationId(), not(equalTo(failedShard.allocationId())));
        }
    }

    public void testCannotJoinIfClusterManagerLostDataFolder() throws Exception {
        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();

        internalCluster().restartNode(clusterManagerNode, new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }

            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .put(ClusterBootstrapService.INITIAL_CLUSTER_MANAGER_NODES_SETTING.getKey(), nodeName)
                    /*
                     * the data node might join while the cluster-manager is still not fully established as cluster-manager just yet and bypasses the join
                     * validation that is done before adding the node to the cluster. Only the join validation when handling the publish
                     * request takes place, but at this point the cluster state has been successfully committed, and will subsequently be
                     * exposed to the applier. The health check below therefore sees the cluster state with the 2 nodes and thinks all is
                     * good, even though the data node never accepted this state. What's worse is that it takes 90 seconds for the data
                     * node to be kicked out of the cluster (lag detection). We speed this up here.
                     */
                    .put(LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.getKey(), "10s")
                    .build();
            }

            @Override
            public boolean validateClusterForming() {
                return false;
            }
        });

        assertBusy(() -> {
            assertFalse(internalCluster().client(clusterManagerNode).admin().cluster().prepareHealth().get().isTimedOut());
            assertTrue(
                internalCluster().client(clusterManagerNode)
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForNodes("2")
                    .setTimeout("2s")
                    .get()
                    .isTimedOut()
            );
        }, 30, TimeUnit.SECONDS);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode)); // otherwise we will fail during clean-up
    }

    /**
     * Tests that indices are properly deleted even if there is a cluster-manager transition in between.
     * Test for <a href="https://github.com/elastic/elasticsearch/issues/11665">Elasticsearch issue #11665</a>
     */
    public void testIndicesDeleted() throws Exception {
        final String idxName = "test";
        final List<String> allClusterManagerEligibleNodes = internalCluster().startClusterManagerOnlyNodes(2);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        assertAcked(prepareCreate("test"));

        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        NetworkDisruption networkDisruption = new NetworkDisruption(
            new TwoPartitions(clusterManagerNode1, dataNode),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        // We know this will time out due to the partition, we check manually below to not proceed until
        // the delete has been applied to the cluster-manager node and the cluster-manager eligible node.
        internalCluster().client(clusterManagerNode1).admin().indices().prepareDelete(idxName).setTimeout("0s").get();
        // Don't restart the cluster-manager node until we know the index deletion has taken effect on cluster-manager and the
        // cluster-manager eligible node.
        assertBusy(() -> {
            for (String clusterManagerNode : allClusterManagerEligibleNodes) {
                final ClusterState clusterManagerState = internalCluster().clusterService(clusterManagerNode).state();
                assertTrue("index not deleted on " + clusterManagerNode, clusterManagerState.metadata().hasIndex(idxName) == false);
            }
        });
        internalCluster().restartNode(clusterManagerNode1, InternalTestCluster.EMPTY_CALLBACK);
        ensureYellow();
        assertFalse(client().admin().indices().prepareExists(idxName).get().isExists());
    }

    public void testRestartNodeWhileIndexing() throws Exception {
        startCluster(3);
        String index = "restart_while_indexing";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(1, 2))
                )
        );
        AtomicBoolean stopped = new AtomicBoolean();
        Thread[] threads = new Thread[between(1, 4)];
        AtomicInteger docID = new AtomicInteger();
        Set<String> ackedDocs = ConcurrentCollections.newConcurrentSet();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                while (stopped.get() == false && docID.get() < 5000) {
                    String id = Integer.toString(docID.incrementAndGet());
                    try {
                        IndexResponse response = client().prepareIndex(index)
                            .setId(id)
                            .setSource(
                                Collections.singletonMap("f" + randomIntBetween(1, 10), randomNonNegativeLong()),
                                MediaTypeRegistry.JSON
                            )
                            .get();
                        assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                        logger.info("--> index id={} seq_no={}", response.getId(), response.getSeqNo());
                        ackedDocs.add(response.getId());
                    } catch (OpenSearchException ignore) {
                        logger.info("--> fail to index id={}", id);
                    }
                }
            });
            threads[i].start();
        }
        ensureGreen(index);
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(100)), 1L, TimeUnit.MINUTES);
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback());
        ensureGreen(index);
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(200)), 1L, TimeUnit.MINUTES);
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
        ClusterState clusterState = internalCluster().clusterService().state();
        for (ShardRouting shardRouting : clusterState.routingTable().allShards(index)) {
            String nodeName = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
            Set<String> docs = IndexShardTestCase.getShardDocUIDs(shard);
            assertThat(
                "shard [" + shard.routingEntry() + "] docIds [" + docs + "] vs " + " acked docIds [" + ackedDocs + "]",
                ackedDocs,
                everyItem(is(in(docs)))
            );
        }
    }

}
