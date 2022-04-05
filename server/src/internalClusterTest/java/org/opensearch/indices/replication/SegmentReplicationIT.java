/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

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
        try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, "_doc", client(), initialDocCount)) {
            waitForDocs(initialDocCount, indexer);
        }
        refresh(INDEX_NAME);
        // wait a short amount of time to give replication a chance to complete.
        Thread.sleep(1000);
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

        final int additionalDocCount = scaledRandomIntBetween(0, 200);
        final int totalDocs = initialDocCount + additionalDocCount;
        try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, "_doc", client(), additionalDocCount)) {
            waitForDocs(additionalDocCount, indexer);
        }
        flush(INDEX_NAME);
        Thread.sleep(1000);
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), totalDocs);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), totalDocs);
    }

    public void testReplicationAfterForceMerge() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
        try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, "_doc", client(), initialDocCount)) {
            waitForDocs(initialDocCount, indexer);
        }
        flush(INDEX_NAME);
        // wait a short amount of time to give replication a chance to complete.
        Thread.sleep(1000);
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

        final int additionalDocCount = scaledRandomIntBetween(0, 200);
        final int totalDocs = initialDocCount + additionalDocCount;
        try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, "_doc", client(), additionalDocCount)) {
            waitForDocs(additionalDocCount, indexer);
        }
        // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
        // This case tests that replicas preserve these files so the local store is not corrupt.
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
        Thread.sleep(1000);
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), totalDocs);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), totalDocs);
    }

    public void testReplicaSetupAfterPrimaryIndexesDocs() throws Exception {
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
    }

    public void testDelOps()throws Exception{
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
                .put(IndexMetadata.SETTING_SEGMENT_REPLICATION, true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        IndexResponse index = client().prepareIndex(INDEX_NAME)
            .setId("1")
            .setSource("foo", "bar")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .get();
//        indexRandom(true, client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar"));
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        IndexResponse index2 = client().prepareIndex(INDEX_NAME)
            .setId("2")
            .setSource("fooo", "baar")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .get();

//        indexRandom(true, client().prepareIndex(INDEX_NAME).setId("2").setSource("fooo", "baar"));
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);

        // Now delete with blockUntilRefresh
        DeleteResponse delete = client().prepareDelete(INDEX_NAME, "1").setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
        assertEquals(DocWriteResponse.Result.DELETED, delete.getResult());
        assertFalse("request shouldn't have forced a refresh", delete.forcedRefresh());
        assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);
        assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);
    }
}
