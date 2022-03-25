/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Assert;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.segments.IndexSegments;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.Segment;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 1;

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_SEGMENT_REPLICATION, true)
            .build();
    }

    public void testReplicationAfterPrimaryRefreshAndFlush() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, "_doc", client(), -1, RandomizedTest.scaledRandomIntBetween(2, 5), false, random())) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            ensureGreen(INDEX_NAME);

            // wait a short amount of time to give replication a chance to complete.
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            ensureGreen(INDEX_NAME);

            flush(INDEX_NAME);
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
        }
        assertSegmentStats(REPLICA_COUNT);
    }

    private void assertSegmentStats(int numberOfReplicas) {
        client().admin().indices().segments(new IndicesSegmentsRequest(), new ActionListener<>() {
            @Override
            public void onResponse(IndicesSegmentResponse indicesSegmentResponse) {
                final Map<String, IndexSegments> indices = indicesSegmentResponse.getIndices();
                assertEquals("Test should only have a single index",1, indices.size());
                for (Map.Entry<String, IndexSegments> entry : indices.entrySet()) {
                    final IndexSegments value = entry.getValue();
                    final Map<Integer, IndexShardSegments> shardGroup = value.getShards();
                    assertEquals("There should only be one replicationGroup", 1, shardGroup.size());
                    for (IndexShardSegments shardEntry : shardGroup.values()) {
                        // get a list of segments across the whole group.
                        final ShardSegments[] shardSegments = shardEntry.getShards();

                        // get the primary shard's segment list.
                        final ShardSegments primaryShardSegments = Arrays.stream(shardSegments).filter(s -> s.getShardRouting().primary()).findFirst().get();
                        final Map<String, Segment> primarySegmentsMap = primaryShardSegments.getSegments().stream().collect(Collectors.toMap(Segment::getName, Function.identity()));

                        // compare primary segments with replicas.
                        assertEquals("There should be a ShardSegment entry for each replica in the replicationGroup", numberOfReplicas, shardSegments.length - 1);
                        logger.info("Primary segments {}", primaryShardSegments.getSegments());
                        for (ShardSegments shardSegment : shardSegments) {
                            if (shardSegment.getShardRouting().primary()) {
                                continue;
                            }
                            final List<Segment> segments = shardSegment.getSegments();
                            logger.info("wAT {}", primarySegmentsMap.values().size());
                            logger.info("{}", segments.size());
                            logger.info("{}", segments.size());
                            logger.info("Replica segments {}", segments);
//                            assertEquals("ShardSegment should have the same segment count as the primary", segments.size(), primarySegmentsMap.size());
                            for (Segment replicaSegment: segments) {
                                final Segment primarySegment = primarySegmentsMap.get(replicaSegment.getName());
                                assertNotNull(primarySegment);
                                assertEqualSegments(replicaSegment, primarySegment);
                            }
                        }
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail("Error fetching segment stats");
            }
        });
    }

    private void assertEqualSegments(Segment s1, Segment s2) {
        assertEquals(s1.getGeneration(), s2.getGeneration());
        assertEquals(s1.docCount, s2.docCount);
        assertEquals(s1.delDocCount, s2.delDocCount);
        assertEquals(s1.sizeInBytes, s2.sizeInBytes);
    }

    public void testReplicationAfterForceMerge() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, "_doc", client(), -1, RandomizedTest.scaledRandomIntBetween(2, 5), false, random())) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            ensureGreen(INDEX_NAME);

            flush(INDEX_NAME);
            // wait a short amount of time to give replication a chance to complete.
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;

            // Index a second set of docs so we can merge into one segment.
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            ensureGreen(INDEX_NAME);

            // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
            // This case tests that replicas preserve these files so the local store is not corrupt.
            client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
        }
        assertSegmentStats(REPLICA_COUNT);
    }

    public void testReplicaSetupAfterPrimaryIndexesDocs() {
        final String nodeA = internalCluster().startNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        // Index a doc to create the first set of segments. _s1.si
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
        // Flush segments to disk and create a new commit point (Primary: segments_3, _s1.si)
        flushAndRefresh(INDEX_NAME);
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        // Index to create another segment
        client().prepareIndex(INDEX_NAME).setId("2").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();

        // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
        // This case tests that we are still sending these older segments to replicas so the index on disk is not corrupt.
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
        refresh(INDEX_NAME);

        final String nodeB = internalCluster().startNode();
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen(INDEX_NAME);
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);
        assertSegmentStats(REPLICA_COUNT);
    }
}
