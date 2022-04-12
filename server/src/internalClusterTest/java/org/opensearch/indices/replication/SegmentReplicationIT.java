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

            // wait a short amount of time to give replication a chance to complete.
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            flushAndRefresh(INDEX_NAME);
            Thread.sleep(1000);
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
            // wait a short amount of time to give replication a chance to complete.
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;

            // Index a second set of docs so we can merge into one segment.
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
            // This case tests that replicas preserve these files so the local store is not corrupt.
            client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
            Thread.sleep(1000);
            refresh(INDEX_NAME);
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);

            ensureGreen(INDEX_NAME);
            assertSegmentStats(REPLICA_COUNT);
        }
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

    private void assertSegmentStats(int numberOfReplicas) {
        client().admin().indices().segments(new IndicesSegmentsRequest(), new ActionListener<>() {
            @Override
            public void onResponse(IndicesSegmentResponse indicesSegmentResponse) {

                List<ShardSegments[]> segmentsByIndex = indicesSegmentResponse.getIndices()
                    .values()
                    .stream() // get list of IndexSegments
                    .flatMap(is -> is.getShards().values().stream()) // Map to shard replication group
                    .map(IndexShardSegments::getShards) // get list of segments across replication group
                    .collect(Collectors.toList());

                // There will be an entry in the list for each index.
                for (ShardSegments[] replicationGroupSegments : segmentsByIndex) {

                    // Separate Primary & replica shards ShardSegments.
                    final Map<Boolean, List<ShardSegments>> segmentListMap = Arrays.stream(replicationGroupSegments)
                        .collect(Collectors.groupingBy(s -> s.getShardRouting().primary()));
                    final List<ShardSegments> primaryShardSegmentsList = segmentListMap.get(true);
                    final List<ShardSegments> replicaShardSegments = segmentListMap.get(false);

                    assertEquals("There should only be one primary in the replicationGroup", primaryShardSegmentsList.size(), 1);
                    final ShardSegments primaryShardSegments = primaryShardSegmentsList.stream().findFirst().get();

                    // create a map of the primary's segments keyed by segment name, allowing us to compare the same segment found on
                    // replicas.
                    final Map<String, Segment> primarySegmentsMap = primaryShardSegments.getSegments()
                        .stream()
                        .collect(Collectors.toMap(Segment::getName, Function.identity()));
                    // For every replica, ensure that its segments are in the same state as on the primary.
                    // It is possible the primary has not cleaned up old segments that are not required on replicas, so we can't do a
                    // list comparison.
                    // This equality check includes search/committed properties on the Segment. Combined with docCount checks,
                    // this ensures the replica has correctly copied the latest segments and has all segments referenced by the latest
                    // commit point, even if they are not searchable.
                    assertEquals(
                        "There should be a ShardSegment entry for each replica in the replicationGroup",
                        numberOfReplicas,
                        replicaShardSegments.size()
                    );

                    for (ShardSegments shardSegment : replicaShardSegments) {
                        for (Segment replicaSegment : shardSegment.getSegments()) {
                            final Segment primarySegment = primarySegmentsMap.get(replicaSegment.getName());
                            assertEquals("Replica's segment should be identical to primary's version", replicaSegment, primarySegment);
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

    public void testDeleteOperations() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();

        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();

        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        client().prepareIndex(INDEX_NAME)
            .setId("2")
            .setSource("fooo", "baar")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .get();

        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);

        // Now delete with blockUntilRefresh
        client().prepareDelete(INDEX_NAME, "1").setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        client().prepareDelete(INDEX_NAME, "2").setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 0);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 0);
    }
}
