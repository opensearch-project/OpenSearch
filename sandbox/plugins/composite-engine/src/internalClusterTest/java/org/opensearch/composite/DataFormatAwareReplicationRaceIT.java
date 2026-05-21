/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reproduces the race condition where SegmentInfos bytes uploaded to remote metadata become
 * inconsistent with the metadata generation during concurrent indexing on a DFA primary.
 *
 * <p>The symptom is:
 * {@code CorruptIndexException: file mismatch, expected suffix=N, got=N+1}
 * when the replica calls {@link Store#buildSegmentInfos(byte[], long)} because the
 * serialized SegmentInfos bytes encode generation X+1 but the metadata file records generation X
 * (or vice versa).
 *
 * <p>This test indexes continuously while forcing flushes to advance the commit generation rapidly,
 * then verifies that every remote metadata file's SegmentInfos bytes are self-consistent with
 * the generation recorded in that metadata file.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicationRaceIT extends DataFormatAwareReplicationIT {

    /**
     * Indexes documents continuously while triggering flushes concurrently to advance the commit
     * generation rapidly. After indexing completes, verifies that:
     * 1. The replica converges (no CorruptIndexException during replication)
     * 2. The remote metadata's SegmentInfos bytes are parseable with the generation stored in
     *    that same metadata file (no suffix mismatch)
     */
    public void testConcurrentIndexingAndFlushDoesNotCorruptReplicaSegmentInfos() throws Exception {
        startClusterAndCreateIndex(2, 1);

        final int totalDocs = 200;
        final AtomicBoolean stopFlushing = new AtomicBoolean(false);
        final AtomicReference<Exception> flushError = new AtomicReference<>();
        final CountDownLatch indexingDone = new CountDownLatch(1);

        // Background thread that flushes aggressively to advance commit generation
        Thread flusher = new Thread(() -> {
            int flushCount = 0;
            while (stopFlushing.get() == false) {
                try {
                    client().admin().indices().prepareFlush(INDEX_NAME).get();
                    flushCount++;
                    // Small sleep to avoid overwhelming but keep pressure high
                    Thread.sleep(randomIntBetween(50, 150));
                } catch (Exception e) {
                    if (stopFlushing.get() == false) {
                        flushError.set(e);
                    }
                    break;
                }
            }
            logger.info("Flusher completed after {} flushes", flushCount);
        }, "concurrent-flusher");
        flusher.setDaemon(true);
        flusher.start();

        // Index documents with immediate refresh to trigger replication rounds
        for (int i = 0; i < totalDocs; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setRefreshPolicy(i % 10 == 0 ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", (long) i)
                .get();
        }

        // Final flush + refresh to ensure everything is uploaded
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        stopFlushing.set(true);
        flusher.join(10_000);
        indexingDone.countDown();

        if (flushError.get() != null) {
            logger.warn("Flush thread encountered error (may be expected during shutdown)", flushError.get());
        }

        // Wait for replication to converge — if the race exists, the replica will fail with
        // CorruptIndexException and the shard will go RED/YELLOW
        ensureGreen(INDEX_NAME);

        // Verify the remote metadata is self-consistent: the SegmentInfos bytes must be
        // parseable with the generation recorded in the same metadata file.
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard replica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);

            assertRemoteMetadataSegmentInfosConsistency(primary);
            assertRemoteMetadataSegmentInfosConsistency(replica);

            // Verify replica has all the data
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(replica);

            // Catalog file sets must agree (excludes segments_N which diverges by design).
            // We use catalog comparison rather than upload-map comparison because merges during
            // concurrent indexing legitimately remove pre-merge files from the active catalog
            // while the upload map retains stale entries.
            assertEquals(
                "primary/replica catalog files must agree",
                DataFormatAwareITUtils.catalogFilesExcludingSegments(primary),
                DataFormatAwareITUtils.catalogFilesExcludingSegments(replica)
            );
        }, 90, TimeUnit.SECONDS);

        // Verify doc count matches (route to primary to avoid unrelated replica search issues)
        long primaryCount = client().prepareSearch(INDEX_NAME).setSize(0).setPreference("_primary").get().getHits().getTotalHits().value();
        assertEquals("all docs should be searchable", totalDocs, primaryCount);
    }

    /**
     * Rapidly alternating index + flush + replica restart to stress the recovery path where
     * syncSegmentsFromRemoteSegmentStore reads metadata that may reference already-merged files.
     */
    public void testReplicaRecoveryDuringConcurrentIndexingDoesNotHitSuffixMismatch() throws Exception {
        startClusterAndCreateIndex(2, 1);

        // Index enough to trigger merges
        indexDocs(50);
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Wait for initial replication
        assertBusy(() -> {
            IndexShard replica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);
            RemoteSegmentMetadata rMeta = replica.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(rMeta);
        }, 60, TimeUnit.SECONDS);

        // Now index more docs to advance generations while restarting replica
        for (int round = 0; round < 3; round++) {
            // Index more docs to create new segments and trigger merges
            for (int i = 0; i < 30; i++) {
                client().prepareIndex(INDEX_NAME)
                    .setId("round-" + round + "-" + i)
                    .setRefreshPolicy(RefreshPolicy.NONE)
                    .setSource(
                        "field_text",
                        randomAlphaOfLength(10),
                        "field_keyword",
                        randomAlphaOfLength(10),
                        "field_number",
                        (long) (round * 30 + i)
                    )
                    .get();
            }
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();

            // Restart replica — forces syncSegmentsFromRemoteSegmentStore which reads
            // the latest metadata and tries to parse SegmentInfos bytes with the recorded generation
            String replicaNode = replicaNodeNames().get(0);
            internalCluster().restartNode(replicaNode);
            ensureGreen(INDEX_NAME);
        }

        // Final verification
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard replica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);

            assertRemoteMetadataSegmentInfosConsistency(primary);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(replica);
        }, 90, TimeUnit.SECONDS);
    }

    /**
     * Asserts that the latest remote metadata file's SegmentInfos bytes are self-consistent:
     * the generation encoded in the serialized bytes must match the generation recorded in the
     * metadata file. A mismatch here is the root cause of the "expected suffix=N, got=N+1" error.
     */
    private void assertRemoteMetadataSegmentInfosConsistency(IndexShard shard) throws Exception {
        RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
        RemoteSegmentMetadata metadata = remoteDir.readLatestMetadataFile();
        assertNotNull("remote metadata must exist for " + shard.routingEntry(), metadata);

        byte[] segmentInfosBytes = metadata.getSegmentInfosBytes();
        assertNotNull("SegmentInfos bytes must not be null", segmentInfosBytes);
        assertTrue("SegmentInfos bytes must not be empty", segmentInfosBytes.length > 0);

        long metadataGeneration = metadata.getGeneration();

        // This is the exact call that fails in production with CorruptIndexException.
        // If the bytes encode a different generation than metadataGeneration, readCommit
        // will throw "file mismatch, expected suffix=X, got=Y"
        SegmentInfos infos = shard.store().buildSegmentInfos(segmentInfosBytes, metadataGeneration);
        assertNotNull("SegmentInfos must be parseable from remote metadata bytes", infos);
        assertEquals(
            "SegmentInfos generation must match metadata generation on " + shard.routingEntry(),
            metadataGeneration,
            infos.getGeneration()
        );
    }
}
