/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class ShardIndexingPressureIT extends OpenSearchIntegTestCase {

    public static final String INDEX_NAME = "test_index";

    private static final Settings unboundedWriteQueue = Settings.builder().put("thread_pool.write.queue_size", -1).build();

    public static final Settings settings = Settings.builder()
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
        .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(unboundedWriteQueue).put(settings).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    public void testShardIndexingPressureTrackingDuringBulkWrites() throws Exception {
        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );

        ensureGreen(INDEX_NAME);
        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final CountDownLatch replicationSendPointReached = new CountDownLatch(1);
        final CountDownLatch latchBlockingReplicationSend = new CountDownLatch(1);

        TransportService primaryService = internalCluster().getInstance(TransportService.class, primaryName);
        final MockTransportService primaryTransportService = (MockTransportService) primaryService;
        TransportService replicaService = internalCluster().getInstance(TransportService.class, replicaName);
        final MockTransportService replicaTransportService = (MockTransportService) replicaService;

        primaryTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportShardBulkAction.ACTION_NAME + "[r]")) {
                try {
                    replicationSendPointReached.countDown();
                    latchBlockingReplicationSend.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final ThreadPool replicaThreadPool = replicaTransportService.getThreadPool();
        final Releasable replicaRelease = blockReplicas(replicaThreadPool);

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        try {
            final ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            replicationSendPointReached.await();

            IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
            Index index = indexService.getIndexSettings().getIndex();
            ShardId shardId = new ShardId(index, 0);

            ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
                IndexingPressureService.class,
                coordinatingOnlyNode
            ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

            assertThat(
                primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes(),
                equalTo(bulkShardRequestSize)
            );
            assertThat(primaryShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes(), equalTo(bulkShardRequestSize));
            assertEquals(0, primaryShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());

            assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());

            assertEquals(
                bulkShardRequestSize,
                coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertEquals(
                bulkShardRequestSize,
                coordinatingShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes()
            );
            assertEquals(0, coordinatingShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());

            latchBlockingReplicationSend.countDown();

            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            final BulkRequest secondBulkRequest = new BulkRequest();
            secondBulkRequest.add(request);

            // Use the primary or the replica data node as the coordinating node this time
            boolean usePrimaryAsCoordinatingNode = randomBoolean();
            final ActionFuture<BulkResponse> secondFuture;
            if (usePrimaryAsCoordinatingNode) {
                secondFuture = client(primaryName).bulk(secondBulkRequest);
            } else {
                secondFuture = client(replicaName).bulk(secondBulkRequest);
            }

            final long secondBulkShardRequestSize = request.ramBytesUsed() + RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class)
                + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

            if (usePrimaryAsCoordinatingNode) {
                assertBusy(() -> {
                    assertThat(
                        primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes(),
                        equalTo(bulkShardRequestSize + secondBulkShardRequestSize)
                    );
                    assertEquals(
                        secondBulkShardRequestSize,
                        primaryShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes()
                    );
                    assertThat(
                        primaryShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes(),
                        equalTo(bulkShardRequestSize + secondBulkShardRequestSize)
                    );

                    assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                    assertEquals(0, replicaShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes());
                    assertEquals(0, replicaShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes());
                });
            } else {
                assertThat(
                    primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes(),
                    equalTo(bulkShardRequestSize)
                );

                assertEquals(
                    secondBulkShardRequestSize,
                    replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(
                    secondBulkShardRequestSize,
                    replicaShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes()
                );
                assertEquals(0, replicaShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes());
            }
            assertEquals(
                bulkShardRequestSize,
                coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
            );
            assertBusy(
                () -> assertThat(
                    replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes(),
                    equalTo(bulkShardRequestSize + secondBulkShardRequestSize)
                )
            );

            replicaRelease.close();

            successFuture.actionGet();
            secondFuture.actionGet();

            assertEquals(0, primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, primaryShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());

            assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());

            assertEquals(0, coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingShardTracker.getCoordinatingOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getPrimaryOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());

        } finally {
            if (replicationSendPointReached.getCount() > 0) {
                replicationSendPointReached.countDown();
            }
            replicaRelease.close();
            if (latchBlockingReplicationSend.getCount() > 0) {
                latchBlockingReplicationSend.countDown();
            }
            replicaRelease.close();
            primaryTransportService.clearAllRules();
        }
    }

    public void testWritesRejectedForSingleCoordinatingShardDueToNodeLevelLimitBreach() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        restartCluster(
            Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 1.5) + "B").build()
        );

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (Releasable replicaRelease = blockReplicas(replicaThreadPool)) {
            final ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);

            IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
            Index index = indexService.getIndexSettings().getIndex();
            ShardId shardId = new ShardId(index, 0);

            ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
                IndexingPressureService.class,
                coordinatingOnlyNode
            ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

            assertBusy(() -> {
                assertEquals(
                    bulkShardRequestSize,
                    primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(bulkShardRequestSize, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(
                    bulkShardRequestSize,
                    coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            });

            expectThrows(OpenSearchRejectedExecutionException.class, () -> {
                if (randomBoolean()) {
                    client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
                } else if (randomBoolean()) {
                    client(primaryName).bulk(bulkRequest).actionGet();
                } else {
                    client(replicaName).bulk(bulkRequest).actionGet();
                }
            });

            replicaRelease.close();

            successFuture.actionGet();

            assertEquals(0, primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
        }
    }

    public void testWritesRejectedFairnessWithMultipleCoordinatingShardsDueToNodeLevelLimitBreach() throws Exception {
        final BulkRequest largeBulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME + "large").id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            largeBulkRequest.add(request);
        }

        final long largeBulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        final BulkRequest smallBulkRequest = new BulkRequest();
        totalRequestSize = 0;
        for (int i = 0; i < 10; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME + "small").id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(10)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            smallBulkRequest.add(request);
        }

        final long smallBulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 10)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        restartCluster(
            Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (largeBulkShardRequestSize * 1.5) + "B").build()
        );

        assertAcked(
            prepareCreate(
                INDEX_NAME + "large",
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME + "large");

        assertAcked(
            prepareCreate(
                INDEX_NAME + "small",
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME + "small");

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME + "large");
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (Releasable replicaRelease = blockReplicas(replicaThreadPool)) {
            final ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(largeBulkRequest);

            ShardId shardId = null;
            for (IndexService indexService : internalCluster().getInstance(IndicesService.class, primaryName)) {
                if (indexService.getIndexSettings().getIndex().getName().equals(INDEX_NAME + "large")) {
                    shardId = new ShardId(indexService.getIndexSettings().getIndex(), 0);
                }
            }

            ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
                IndexingPressureService.class,
                coordinatingOnlyNode
            ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

            assertBusy(() -> {
                assertEquals(
                    largeBulkShardRequestSize,
                    primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(
                    largeBulkShardRequestSize,
                    replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes()
                );
                assertEquals(
                    largeBulkShardRequestSize,
                    coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            });

            // Large request on a shard with already large occupancy is rejected
            expectThrows(
                OpenSearchRejectedExecutionException.class,
                () -> { client(coordinatingOnlyNode).bulk(largeBulkRequest).actionGet(); }
            );

            replicaRelease.close();
            successFuture.actionGet();
            assertEquals(0, primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(1, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());

            // Try sending a small request now instead which should succeed one the new shard with less occupancy
            final ThreadPool replicaThreadPoolSmallRequest = internalCluster().getInstance(ThreadPool.class, replicaName);
            try (Releasable replicaReleaseSmallRequest = blockReplicas(replicaThreadPoolSmallRequest)) {
                final ActionFuture<BulkResponse> successFutureSmallRequest = client(coordinatingOnlyNode).bulk(smallBulkRequest);

                shardId = null;
                for (IndexService indexService : internalCluster().getInstance(IndicesService.class, primaryName)) {
                    if (indexService.getIndexSettings().getIndex().getName().equals(INDEX_NAME + "small")) {
                        shardId = new ShardId(indexService.getIndexSettings().getIndex(), 0);
                    }
                }

                ShardIndexingPressureTracker primaryShardTrackerSmall = internalCluster().getInstance(
                    IndexingPressureService.class,
                    primaryName
                ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);
                ShardIndexingPressureTracker replicaShardTrackerSmall = internalCluster().getInstance(
                    IndexingPressureService.class,
                    replicaName
                ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);
                ShardIndexingPressureTracker coordinatingShardTrackerSmall = internalCluster().getInstance(
                    IndexingPressureService.class,
                    coordinatingOnlyNode
                ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

                assertBusy(() -> {
                    assertEquals(
                        smallBulkShardRequestSize,
                        primaryShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                    );
                    assertEquals(0, primaryShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                    assertEquals(0, replicaShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                    assertEquals(
                        smallBulkShardRequestSize,
                        replicaShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes()
                    );
                    assertEquals(
                        smallBulkShardRequestSize,
                        coordinatingShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                    );
                    assertEquals(0, coordinatingShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                });

                replicaReleaseSmallRequest.close();
                successFutureSmallRequest.actionGet();
                assertEquals(0, primaryShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, primaryShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, replicaShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, replicaShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, coordinatingShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, coordinatingShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, coordinatingShardTrackerSmall.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
            }
        }
    }

    public void testWritesRejectedForSinglePrimaryShardDueToNodeLevelLimitBreach() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        restartCluster(
            Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 1.5) + "B").build()
        );

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (Releasable replicaRelease = blockReplicas(replicaThreadPool)) {
            final ActionFuture<BulkResponse> successFuture = client(primaryName).bulk(bulkRequest);

            IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
            Index index = indexService.getIndexSettings().getIndex();
            ShardId shardId = new ShardId(index, 0);

            ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
                IndexingPressureService.class,
                coordinatingOnlyNode
            ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

            assertBusy(() -> {
                assertEquals(
                    bulkShardRequestSize,
                    primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(bulkShardRequestSize, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            });

            BulkResponse responses = client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
            assertTrue(responses.hasFailures());
            assertThat(responses.getItems()[0].getFailure().getCause().getCause(), instanceOf(OpenSearchRejectedExecutionException.class));

            replicaRelease.close();
            successFuture.actionGet();

            assertEquals(0, primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(1, primaryShardTracker.getPrimaryOperationTracker().getRejectionTracker().getTotalRejections());
            assertEquals(0, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
        }
    }

    public void testWritesRejectedFairnessWithMultiplePrimaryShardsDueToNodeLevelLimitBreach() throws Exception {
        final BulkRequest largeBulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME + "large").id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            largeBulkRequest.add(request);
        }

        final long largeBulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        final BulkRequest smallBulkRequest = new BulkRequest();
        totalRequestSize = 0;
        for (int i = 0; i < 10; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME + "small").id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(10)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            smallBulkRequest.add(request);
        }

        final long smallBulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 10)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        restartCluster(
            Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (largeBulkShardRequestSize * 1.5) + "B").build()
        );

        assertAcked(
            prepareCreate(
                INDEX_NAME + "large",
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME + "large");

        assertAcked(
            prepareCreate(
                INDEX_NAME + "small",
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME + "small");

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME + "large");
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (Releasable replicaRelease = blockReplicas(replicaThreadPool)) {
            final ActionFuture<BulkResponse> successFuture = client(primaryName).bulk(largeBulkRequest);

            ShardId shardId = null;
            for (IndexService indexService : internalCluster().getInstance(IndicesService.class, primaryName)) {
                if (indexService.getIndexSettings().getIndex().getName().equals(INDEX_NAME + "large")) {
                    shardId = new ShardId(indexService.getIndexSettings().getIndex(), 0);
                }
            }

            ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
                IndexingPressureService.class,
                coordinatingOnlyNode
            ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

            assertBusy(() -> {
                assertEquals(
                    largeBulkShardRequestSize,
                    primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                );
                assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(
                    largeBulkShardRequestSize,
                    replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes()
                );
                assertEquals(0, coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            });

            BulkResponse responses = client(coordinatingOnlyNode).bulk(largeBulkRequest).actionGet();
            assertTrue(responses.hasFailures());
            assertThat(responses.getItems()[0].getFailure().getCause().getCause(), instanceOf(OpenSearchRejectedExecutionException.class));

            replicaRelease.close();
            successFuture.actionGet();
            assertEquals(0, primaryShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, replicaShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingShardTracker.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
            assertEquals(0, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
            assertEquals(1, primaryShardTracker.getPrimaryOperationTracker().getRejectionTracker().getTotalRejections());

            // Try sending a small request now instead which should succeed one the new shard with less occupancy
            final ThreadPool replicaThreadPoolSmallRequest = internalCluster().getInstance(ThreadPool.class, replicaName);
            try (Releasable replicaReleaseSmallRequest = blockReplicas(replicaThreadPoolSmallRequest)) {
                final ActionFuture<BulkResponse> successFutureSmallRequest = client(primaryName).bulk(smallBulkRequest);

                shardId = null;
                for (IndexService indexService : internalCluster().getInstance(IndicesService.class, primaryName)) {
                    if (indexService.getIndexSettings().getIndex().getName().equals(INDEX_NAME + "small")) {
                        shardId = new ShardId(indexService.getIndexSettings().getIndex(), 0);
                    }
                }

                ShardIndexingPressureTracker primaryShardTrackerSmall = internalCluster().getInstance(
                    IndexingPressureService.class,
                    primaryName
                ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);
                ShardIndexingPressureTracker replicaShardTrackerSmall = internalCluster().getInstance(
                    IndexingPressureService.class,
                    replicaName
                ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);
                ShardIndexingPressureTracker coordinatingShardTrackerSmall = internalCluster().getInstance(
                    IndexingPressureService.class,
                    coordinatingOnlyNode
                ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

                assertBusy(() -> {
                    assertEquals(
                        smallBulkShardRequestSize,
                        primaryShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                    );
                    assertEquals(0, primaryShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                    assertEquals(0, replicaShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                    assertEquals(
                        smallBulkShardRequestSize,
                        replicaShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes()
                    );
                    assertEquals(
                        0,
                        coordinatingShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes()
                    );
                    assertEquals(0, coordinatingShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                });

                replicaReleaseSmallRequest.close();
                successFutureSmallRequest.actionGet();
                assertEquals(0, primaryShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, primaryShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, replicaShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, replicaShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, coordinatingShardTrackerSmall.getCommonOperationTracker().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, coordinatingShardTrackerSmall.getReplicaOperationTracker().getStatsTracker().getCurrentBytes());
                assertEquals(0, primaryShardTrackerSmall.getPrimaryOperationTracker().getRejectionTracker().getTotalRejections());
            }
        }
    }

    private Tuple<String, String> getPrimaryReplicaNodeNames(String indexName) {
        IndicesStatsResponse response = client().admin().indices().prepareStats(indexName).get();
        String primaryId = Stream.of(response.getShards())
            .map(ShardStats::getShardRouting)
            .filter(ShardRouting::primary)
            .findAny()
            .get()
            .currentNodeId();
        String replicaId = Stream.of(response.getShards())
            .map(ShardStats::getShardRouting)
            .filter(sr -> sr.primary() == false)
            .findAny()
            .get()
            .currentNodeId();
        DiscoveryNodes nodes = client().admin().cluster().prepareState().get().getState().nodes();
        String primaryName = nodes.get(primaryId).getName();
        String replicaName = nodes.get(replicaId).getName();
        return new Tuple<>(primaryName, replicaName);
    }

    private String getCoordinatingOnlyNode() {
        return client().admin().cluster().prepareState().get().getState().nodes().getCoordinatingOnlyNodes().iterator().next().value
            .getName();
    }

    private Releasable blockReplicas(ThreadPool threadPool) {
        final CountDownLatch blockReplication = new CountDownLatch(1);
        final int threads = threadPool.info(ThreadPool.Names.WRITE).getMax();
        final CountDownLatch pointReached = new CountDownLatch(threads);
        for (int i = 0; i < threads; ++i) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
                try {
                    pointReached.countDown();
                    blockReplication.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        return () -> {
            if (blockReplication.getCount() > 0) {
                blockReplication.countDown();
            }
        };
    }

    private void restartCluster(Settings settings) throws Exception {
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(unboundedWriteQueue).put(settings).build();
            }
        });
    }
}
