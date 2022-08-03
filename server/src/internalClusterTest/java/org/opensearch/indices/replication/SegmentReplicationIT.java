/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.lucene.index.SegmentInfos;
import org.junit.BeforeClass;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 1;

    @BeforeClass
    public static void assumeFeatureFlag() {
        assumeTrue("Segment replication Feature flag is enabled", Boolean.parseBoolean(System.getProperty(FeatureFlags.REPLICATION_TYPE)));
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testReplicationAfterPrimaryRefreshAndFlush() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForReplicaUpdate();

            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            flushAndRefresh(INDEX_NAME);
            waitForReplicaUpdate();
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);

            ensureGreen(INDEX_NAME);
            assertSegmentStats(REPLICA_COUNT);
        }
    }

    public void testReplicationAfterForceMerge() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        final int additionalDocCount = scaledRandomIntBetween(0, 200);
        final int expectedHitCount = initialDocCount + additionalDocCount;
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);

            flush(INDEX_NAME);
            waitForReplicaUpdate();
            // wait a short amount of time to give replication a chance to complete.
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

            // Index a second set of docs so we can merge into one segment.
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
            client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
            refresh(INDEX_NAME);
            waitForReplicaUpdate();
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);

            ensureGreen(INDEX_NAME);
            assertSegmentStats(REPLICA_COUNT);
        }
    }

    public void testStartReplicaAfterPrimaryIndexesDocs() throws Exception {
        final String primaryNode = internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        // Index a doc to create the first set of segments. _s1.si
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        // Flush segments to disk and create a new commit point (Primary: segments_3, _s1.si)
        flushAndRefresh(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        // Index to create another segment
        client().prepareIndex(INDEX_NAME).setId("2").setSource("foo", "bar").get();

        // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
        refresh(INDEX_NAME);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        final String replicaNode = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("foo", "bar").get();

        waitForReplicaUpdate();
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);

        final Index index = resolveIndex(INDEX_NAME);
        IndexShard primaryShard = getIndexShard(index, primaryNode);
        IndexShard replicaShard = getIndexShard(index, replicaNode);
        assertEquals(
            primaryShard.translogStats().estimatedNumberOfOperations(),
            replicaShard.translogStats().estimatedNumberOfOperations()
        );
        assertSegmentStats(REPLICA_COUNT);
    }

    private void assertSegmentStats(int numberOfReplicas) throws IOException {
        final IndicesSegmentResponse indicesSegmentResponse = client().admin().indices().segments(new IndicesSegmentsRequest()).actionGet();

        List<ShardSegments[]> segmentsByIndex = getShardSegments(indicesSegmentResponse);

        // There will be an entry in the list for each index.
        for (ShardSegments[] replicationGroupSegments : segmentsByIndex) {

            // Separate Primary & replica shards ShardSegments.
            final Map<Boolean, List<ShardSegments>> segmentListMap = segmentsByShardType(replicationGroupSegments);
            final List<ShardSegments> primaryShardSegmentsList = segmentListMap.get(true);
            final List<ShardSegments> replicaShardSegments = segmentListMap.get(false);

            assertEquals("There should only be one primary in the replicationGroup", primaryShardSegmentsList.size(), 1);
            final ShardSegments primaryShardSegments = primaryShardSegmentsList.stream().findFirst().get();
            final Map<String, Segment> latestPrimarySegments = getLatestSegments(primaryShardSegments);

            assertEquals(
                "There should be a ShardSegment entry for each replica in the replicationGroup",
                numberOfReplicas,
                replicaShardSegments.size()
            );

            for (ShardSegments shardSegment : replicaShardSegments) {
                final Map<String, Segment> latestReplicaSegments = getLatestSegments(shardSegment);
                for (Segment replicaSegment : latestReplicaSegments.values()) {
                    final Segment primarySegment = latestPrimarySegments.get(replicaSegment.getName());
                    assertEquals(replicaSegment.getGeneration(), primarySegment.getGeneration());
                    assertEquals(replicaSegment.getNumDocs(), primarySegment.getNumDocs());
                    assertEquals(replicaSegment.getDeletedDocs(), primarySegment.getDeletedDocs());
                    assertEquals(replicaSegment.getSize(), primarySegment.getSize());
                }

                // Fetch the IndexShard for this replica and try and build its SegmentInfos from the previous commit point.
                // This ensures the previous commit point is not wiped.
                final ShardRouting replicaShardRouting = shardSegment.getShardRouting();
                ClusterState state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
                final DiscoveryNode replicaNode = state.nodes().resolveNode(replicaShardRouting.currentNodeId());
                final Index index = resolveIndex(INDEX_NAME);
                IndexShard indexShard = getIndexShard(index, replicaNode.getName());
                final String lastCommitSegmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(indexShard.store().directory());
                // calls to readCommit will fail if a valid commit point and all its segments are not in the store.
                SegmentInfos.readCommit(indexShard.store().directory(), lastCommitSegmentsFileName);
            }
        }
    }

    /**
     * Waits until the replica is caught up to the latest primary segments gen.
     * @throws Exception if assertion fails
     */
    private void waitForReplicaUpdate() throws Exception {
        // wait until the replica has the latest segment generation.
        assertBusy(() -> {
            final IndicesSegmentResponse indicesSegmentResponse = client().admin()
                .indices()
                .segments(new IndicesSegmentsRequest())
                .actionGet();
            List<ShardSegments[]> segmentsByIndex = getShardSegments(indicesSegmentResponse);
            for (ShardSegments[] replicationGroupSegments : segmentsByIndex) {
                final Map<Boolean, List<ShardSegments>> segmentListMap = segmentsByShardType(replicationGroupSegments);
                final List<ShardSegments> primaryShardSegmentsList = segmentListMap.get(true);
                final List<ShardSegments> replicaShardSegments = segmentListMap.get(false);

                final ShardSegments primaryShardSegments = primaryShardSegmentsList.stream().findFirst().get();
                final Map<String, Segment> latestPrimarySegments = getLatestSegments(primaryShardSegments);
                final Long latestPrimaryGen = latestPrimarySegments.values().stream().findFirst().map(Segment::getGeneration).get();
                for (ShardSegments shardSegments : replicaShardSegments) {
                    final boolean isReplicaCaughtUpToPrimary = shardSegments.getSegments()
                        .stream()
                        .anyMatch(segment -> segment.getGeneration() == latestPrimaryGen);
                    assertTrue(isReplicaCaughtUpToPrimary);
                }
            }
        });
    }

    private IndexShard getIndexShard(Index index, String node) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexServiceSafe(index);
        final Optional<Integer> shardId = indexService.shardIds().stream().findFirst();
        return indexService.getShard(shardId.get());
    }

    private List<ShardSegments[]> getShardSegments(IndicesSegmentResponse indicesSegmentResponse) {
        return indicesSegmentResponse.getIndices()
            .values()
            .stream() // get list of IndexSegments
            .flatMap(is -> is.getShards().values().stream()) // Map to shard replication group
            .map(IndexShardSegments::getShards) // get list of segments across replication group
            .collect(Collectors.toList());
    }

    private Map<String, Segment> getLatestSegments(ShardSegments segments) {
        final Long latestPrimaryGen = segments.getSegments().stream().map(Segment::getGeneration).max(Long::compare).get();
        return segments.getSegments()
            .stream()
            .filter(s -> s.getGeneration() == latestPrimaryGen)
            .collect(Collectors.toMap(Segment::getName, Function.identity()));
    }

    private Map<Boolean, List<ShardSegments>> segmentsByShardType(ShardSegments[] replicationGroupSegments) {
        return Arrays.stream(replicationGroupSegments).collect(Collectors.groupingBy(s -> s.getShardRouting().primary()));
    }
}
