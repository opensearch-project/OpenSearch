/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

/**
 * Durability lifecycle for multi-valued ({@code multi_value: true}) keyword fields on a composite
 * parquet+lucene index: refresh, flush, force-merge, replication to a peer, and recovery.
 *
 * <p>A multi-valued field is stored as an Arrow/Parquet {@code LIST<element>} column rather than a
 * flat column, so every stage that moves or rewrites those files is a place the list encoding can
 * break independently of the read path: the VSR flush writes offsets, force-merge re-encodes each
 * column through {@code compute_leaves}, segment replication ships the files to a replica, and
 * recovery replays or re-fetches them. Each assertion below re-reads the array and requires the
 * values to be byte-identical, in document order, duplicates intact — the column is the source of
 * truth for derived {@code _source}, so any loss is silent data corruption rather than a query bug.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MultiValueFieldDurabilityIT extends DataFormatAwareReplicationBaseIT {

    private static final String MV_INDEX = "mv-durability-idx";

    /** Document id → the exact array it was indexed with. */
    private static final Map<String, List<String>> EXPECTED = Map.of(
        "d1",
        List.of("beta", "alpha", "beta"),
        "d2",
        List.of("solo"),
        "d3",
        List.of(),
        "d4",
        List.of("x", "y", "z", "x")
    );

    private Settings mvIndexSettings(int replicaCount) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(replicaCount, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, org.opensearch.indices.replication.common.ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    private static final String MAPPING = "{\"properties\":{"
        + "\"id\":{\"type\":\"keyword\"},"
        + "\"tags\":{\"type\":\"keyword\",\"multi_value\":true}"
        + "}}";

    private void createMvIndex(int replicaCount) {
        client().admin().indices().prepareCreate(MV_INDEX).setSettings(mvIndexSettings(replicaCount)).setMapping(MAPPING).get();
        ensureYellowAndNoInitializingShards(MV_INDEX);
    }

    /** Generated _id → fixture key, captured at index time (append-only index rejects custom _ids). */
    private final Map<String, String> idToFixture = new java.util.HashMap<>();

    /** Indexes the four fixture docs with the given refresh policy, recording their generated ids. */
    private void indexFixtures(WriteRequest.RefreshPolicy policy) {
        for (Map.Entry<String, List<String>> e : EXPECTED.entrySet()) {
            StringBuilder json = new StringBuilder("{\"id\":\"").append(e.getKey()).append("\",\"tags\":[");
            for (int i = 0; i < e.getValue().size(); i++) {
                if (i > 0) json.append(",");
                json.append("\"").append(e.getValue().get(i)).append("\"");
            }
            json.append("]}");
            org.opensearch.action.index.IndexResponse resp = client().prepareIndex(MV_INDEX)
                .setRefreshPolicy(policy)
                .setSource(json.toString(), XContentType.JSON)
                .get();
            idToFixture.put(resp.getId(), e.getKey());
        }
    }

    /**
     * Asserts every indexed document still carries exactly its own values.
     *
     * <p>Reads via get-by-id rather than {@code _search}: on a composite index
     * {@code IndexShard.applyOnEngine} rejects {@code DataFormatAwareEngine}, so there is no
     * searcher. The get path reconstructs {@code _source} from the Parquet columns (parquet-owned
     * fields have no Lucene stored fields), making this a direct test of the LIST column's
     * integrity rather than of a cached copy.
     */
    @SuppressWarnings("unchecked")
    private void assertArraysIntact(String stage) {
        assertFalse(stage + ": no documents were indexed", idToFixture.isEmpty());
        for (Map.Entry<String, String> entry : idToFixture.entrySet()) {
            org.opensearch.action.get.GetResponse resp = client().prepareGet(MV_INDEX, entry.getKey()).setRealtime(false).get();
            assertTrue(stage + ": document [" + entry.getKey() + "] must exist", resp.isExists());
            Map<String, Object> source = resp.getSourceAsMap();
            List<String> want = EXPECTED.get(entry.getValue());
            Object actual = source.get("tags");
            List<String> got = actual == null ? List.of() : ((List<Object>) actual).stream().map(String::valueOf).toList();
            assertEquals(stage + ": fixture [" + entry.getValue() + "] lost or reordered values", want, got);
        }
    }

    /**
     * refresh → flush → force-merge on a single primary. Each stage either makes the in-memory VSR
     * durable or rewrites the Parquet files; the array must survive all three unchanged.
     */
    public void testArraysSurviveRefreshFlushAndForceMerge() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createMvIndex(0);
        ensureGreen(MV_INDEX);

        // Refresh: flushes the active VSR into a readable Parquet generation.
        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        assertArraysIntact("after refresh");

        // Flush: commits the generation.
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();
        assertArraysIntact("after flush");

        // A second generation, so force-merge has more than one file to combine.
        indexFixtures(WriteRequest.RefreshPolicy.IMMEDIATE);
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();

        ForceMergeResponse merge = client().admin().indices().prepareForceMerge(MV_INDEX).setMaxNumSegments(1).get();
        assertEquals("force-merge must not fail any shard", 0, merge.getFailedShards());
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();

        assertArraysIntact("after force-merge");
    }

    /**
     * Segment replication of a LIST column to a peer: the replica reads the primary's Parquet files
     * verbatim, so a replica-side read failure would mean the file or its catalog entry is not
     * self-describing.
     */
    public void testArraysReplicateToPeer() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createMvIndex(1);
        ensureGreen(MV_INDEX);

        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();

        // Segment replication is asynchronous. Wait for the replica's catalog to carry the same
        // parquet files as the primary — the base helper's node lookups are bound to its own
        // INDEX_NAME, so resolve this index's shards here.
        assertBusy(() -> {
            org.opensearch.cluster.routing.IndexShardRoutingTable routing = getClusterState().routingTable().index(MV_INDEX).shard(0);
            String primaryNode = getClusterState().nodes().get(routing.primaryShard().currentNodeId()).getName();
            org.opensearch.index.shard.IndexShard primary = getIndexShard(primaryNode, MV_INDEX);
            java.util.Set<String> primaryFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
            assertFalse("primary must have parquet files in its catalog", primaryFiles.isEmpty());
            for (org.opensearch.cluster.routing.ShardRouting r : routing.replicaShards()) {
                assertTrue("replica must be started", r.started());
                String replicaNode = getClusterState().nodes().get(r.currentNodeId()).getName();
                org.opensearch.index.shard.IndexShard replica = getIndexShard(replicaNode, MV_INDEX);
                assertEquals(
                    "primary/replica catalog files must converge on " + replicaNode,
                    primaryFiles,
                    DataFormatAwareITUtils.catalogFilesExcludingSegments(replica)
                );
            }
        }, 60, java.util.concurrent.TimeUnit.SECONDS);

        // Values must still read back intact once the replica holds the same files.
        assertArraysIntact("after replication to peer");
    }

    /**
     * Recovery: restart the data node holding the primary and re-read. Exercises the commit +
     * catalog replay path over a LIST column — a lost or mis-sized offsets buffer would surface as
     * a read failure or altered values after recovery rather than at write time.
     */
    public void testArraysSurviveNodeRestartRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createMvIndex(0);
        ensureGreen(MV_INDEX);

        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();
        assertArraysIntact("before restart");

        internalCluster().restartRandomDataNode();
        ensureGreen(MV_INDEX);

        assertArraysIntact("after node restart recovery");
    }

    /**
     * A {@code BackgroundIndexer} whose documents carry a multi-valued {@code tags} array, so
     * concurrent load exercises the LIST write path (VSR rotation, offsets growth) rather than only
     * flat columns. Every document's array is derived from its id so the expected contents are
     * recomputable at verification time.
     */
    private static final class MultiValueIndexer extends org.opensearch.test.BackgroundIndexer {
        MultiValueIndexer(String index, org.opensearch.transport.client.Client client, int writerCount) {
            super(index, "_doc", client, -1, writerCount, false, random());
        }

        /** The array document {@code id} is indexed with — varies length 0..3 including duplicates. */
        static List<String> tagsFor(long id) {
            int shape = (int) Math.floorMod(id, 4L);
            return switch (shape) {
                case 0 -> List.of();
                case 1 -> List.of("t" + id);
                case 2 -> List.of("t" + id, "dup", "dup");
                default -> List.of("a" + id, "b" + id, "c" + id);
            };
        }

        @Override
        protected org.opensearch.core.xcontent.XContentBuilder generateSource(long id, java.util.Random random) throws java.io.IOException {
            org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
            builder.startObject().field("id", Long.toString(id));
            builder.startArray("tags");
            for (String tag : tagsFor(id)) {
                builder.value(tag);
            }
            builder.endArray();
            return builder.endObject();
        }
    }

    /**
     * Peer recovery of a LIST column while indexing continues.
     *
     * <p>Recovery under load is the case the stop-then-recover test cannot reach: the replica is
     * built from a catalog that is still advancing, so a generation can be shipped while the
     * primary's active VSR is mid-list (offsets written, values not yet complete). Asserts the
     * replica converges on the primary's parquet files and that no acknowledged write was lost.
     */
    public void testArraysSurviveConcurrentIndexingDuringPeerRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createMvIndex(0);
        ensureGreen(MV_INDEX);

        try (MultiValueIndexer indexer = new MultiValueIndexer(MV_INDEX, client(), scaledRandomIntBetween(2, 4))) {
            indexer.setUseAutoGeneratedIDs(true);
            indexer.start(-1);
            waitForIndexerDocs(200, indexer);

            // Adding a replica while writes are in flight triggers peer recovery under load.
            client().admin()
                .indices()
                .prepareUpdateSettings(MV_INDEX)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                .get();

            indexer.continueIndexing(200);
            ensureGreen(MV_INDEX);
            indexer.stopAndAwaitStopped();

            // No acknowledged write may be lost, and the replica must hold the same parquet files.
            indexer.assertNoFailures();
            assertTrue("indexing must have made progress", indexer.totalIndexedDocs() > 0);
            client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();
            assertReplicaCatalogConverged();
        }
    }

    /**
     * Partial write failure across the two formats must leave neither format holding the document.
     *
     * <p>A composite write calls the primary (parquet) then each secondary (lucene) in turn and
     * rolls back every writer it touched if any one fails (CompositeWriter#addDoc). A term
     * longer than Lucene's {@code IndexWriter.MAX_TERM_LENGTH} (32766 bytes) fails only in the
     * secondary, after parquet has already accepted the row and — for a multi-valued field — after
     * its list offsets have been advanced. The rollback therefore has to rewind a partially written
     * LIST cell, which is the failure mode a flat column cannot exercise. Subsequent good documents
     * must still index and read back correctly, proving the VSR was left consistent rather than
     * merely not crashing.
     */
    public void testPartialWriteFailureAcrossFormatsRollsBackBothFormats() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createMvIndex(0);
        ensureGreen(MV_INDEX);

        // One good document first, so the VSR already holds a completed list row.
        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        assertArraysIntact("before partial failure");
        int acceptedBefore = idToFixture.size();

        // A multi-valued document whose SECOND element exceeds Lucene's max term length: parquet
        // accepts the row (no term limit), lucene rejects it, so the composite writer must roll back.
        String hugeTerm = randomAlphaOfLength(40000);
        org.opensearch.action.index.IndexRequestBuilder bad = client().prepareIndex(MV_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
            .setSource("{\"id\":\"bad\",\"tags\":[\"ok\",\"" + hugeTerm + "\"]}", XContentType.JSON);
        Exception failure = expectThrows(Exception.class, bad::get);
        assertNotNull("oversized term must fail the write", failure);

        // The rejected document must exist in NEITHER format: a subsequent read of every previously
        // acknowledged document still returns its own values, and the doc count has not grown.
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        assertArraysIntact("after rolled-back partial failure");
        assertEquals("rolled-back document must not be acknowledged", acceptedBefore, idToFixture.size());

        // The writer must still accept good documents after the rollback — proving the VSR's list
        // offsets were rewound rather than left pointing into a half-written cell.
        idToFixture.clear();
        indexFixtures(WriteRequest.RefreshPolicy.NONE);
        client().admin().indices().prepareRefresh(MV_INDEX).get();
        client().admin().indices().prepareFlush(MV_INDEX).setForce(true).get();
        assertArraysIntact("after resuming writes post-rollback");
    }

    /** Waits until every started replica's catalog holds the same parquet files as the primary. */
    private void assertReplicaCatalogConverged() throws Exception {
        assertBusy(() -> {
            org.opensearch.cluster.routing.IndexShardRoutingTable routing = getClusterState().routingTable().index(MV_INDEX).shard(0);
            String primaryNode = getClusterState().nodes().get(routing.primaryShard().currentNodeId()).getName();
            org.opensearch.index.shard.IndexShard primary = getIndexShard(primaryNode, MV_INDEX);
            java.util.Set<String> primaryFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
            assertFalse("primary must have parquet files in its catalog", primaryFiles.isEmpty());
            for (org.opensearch.cluster.routing.ShardRouting r : routing.replicaShards()) {
                assertTrue("replica must be started", r.started());
                String replicaNode = getClusterState().nodes().get(r.currentNodeId()).getName();
                org.opensearch.index.shard.IndexShard replica = getIndexShard(replicaNode, MV_INDEX);
                assertEquals(
                    "primary/replica catalog files must converge on " + replicaNode,
                    primaryFiles,
                    DataFormatAwareITUtils.catalogFilesExcludingSegments(replica)
                );
            }
        }, 60, java.util.concurrent.TimeUnit.SECONDS);
    }
}
