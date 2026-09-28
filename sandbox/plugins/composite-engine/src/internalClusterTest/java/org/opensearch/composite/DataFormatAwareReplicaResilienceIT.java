/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.IndexShard;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for DFA replica resilience: multiple consecutive replica restarts
 * and old parquet generation cleanup during replication.
 */
public class DataFormatAwareReplicaResilienceIT extends DataFormatAwareReplicationBaseIT {

    /**
     * Tests that a replica recovers correctly through 3 consecutive node restarts
     * with new documents indexed between each restart. Validates catalog grows
     * (new generations) and convergence holds after each recovery cycle.
     */
    public void testMultipleConsecutiveReplicaRestarts() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(1)).get();
        ensureGreen(INDEX_NAME);

        for (int restart = 0; restart < 3; restart++) {
            // Index new docs before each restart
            indexDocs(randomIntBetween(10, 20));
            client().admin().indices().prepareFlush(INDEX_NAME).get();

            // Verify catalog convergence before restart
            assertCatalogSnapshotsConverged(INDEX_NAME);

            // Restart the replica node
            String replicaNode = replicaNodeNames().get(0);
            internalCluster().restartNode(replicaNode);
            ensureGreen(INDEX_NAME);

            // Verify catalog convergence after restart
            assertCatalogSnapshotsConverged(INDEX_NAME);
        }

        // Final verification: catalog is non-empty and consistent
        IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
        Set<String> finalCatalog = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
        assertFalse("catalog must have files after 3 restart cycles", finalCatalog.isEmpty());
        DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
    }

    /**
     * Tests that after multiple flush cycles, old parquet generation files are cleaned up
     * on replicas during replication, leaving only the current generation's files.
     * The catalog file set on the replica should match the primary's.
     */
    public void testOldParquetGenerationCleanupOnReplica() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(1)).get();
        ensureGreen(INDEX_NAME);

        // Perform multiple flush cycles to create multiple parquet generations
        for (int gen = 0; gen < 4; gen++) {
            indexDocs(10);
            client().admin().indices().prepareFlush(INDEX_NAME).get();
        }

        // Wait for replication to converge
        assertCatalogSnapshotsConverged(INDEX_NAME);

        // Get the primary's current catalog files — this is the "truth"
        IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
        Set<String> primaryCatalogFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);

        // Replica should have the same catalog files (not accumulated old generations)
        assertBusy(() -> {
            String replicaNode = replicaNodeNames().get(0);
            IndexShard replica = getIndexShard(replicaNode, INDEX_NAME);
            Set<String> replicaCatalogFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(replica);
            assertEquals(
                "Replica should have same catalog files as primary (no stale generations)",
                primaryCatalogFiles,
                replicaCatalogFiles
            );
        }, 60, TimeUnit.SECONDS);

        // Restart replica and verify cleanup persists after recovery
        String replicaNode = replicaNodeNames().get(0);
        internalCluster().restartNode(replicaNode);
        ensureGreen(INDEX_NAME);

        assertBusy(() -> {
            String newReplicaNode = replicaNodeNames().get(0);
            IndexShard recoveredReplica = getIndexShard(newReplicaNode, INDEX_NAME);
            Set<String> recoveredFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(recoveredReplica);
            assertEquals("Recovered replica should have same catalog files as primary", primaryCatalogFiles, recoveredFiles);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Reproduces the indexSort mismatch fired when {@code LuceneCommitter.discoverAndTrimUnsafeCommits}
     * runs at engine reinit with many Lucene segments at the safe-commit point.
     *
     * <p>Without the fix, {@code discoverAndTrimUnsafeCommits} opens a temp {@code IndexWriter}
     * with default {@code TieredMergePolicy} and no {@code setIndexSort}. If the safe commit
     * has {@code >= ~10} segments, the temp writer fires a merge during its open/close cycle
     * that produces a segment with {@code source=merge} and {@code indexSort=null}. The
     * subsequent (sorted) {@code MergeIndexWriter} cannot open at that polluted commit and
     * throws {@code IllegalArgumentException: cannot change previous indexSort=null ...},
     * leaving the shard unable to start.
     *
     * <p>The fix pins {@code NoMergePolicy.INSTANCE} on that temp writer.
     */
    public void testEngineReinitDoesNotFailWithIndexSortMismatch() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        // Lucene-secondary index with merges suppressed so refreshes accumulate as separate
        // segments. The safe commit must reference >= TieredMergePolicy.DEFAULT_SEGMENTS_PER_TIER
        // (~10) Lucene segments to trigger the buggy temp-writer merge during engine reinit.
        Settings indexSettings = Settings.builder()
            .put(dfaIndexSettings(0))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .put("index.composite.merge_on_refresh_max_size", "0")
            .put("index.merge.policy.max_merge_at_once", 2)
            .put("index.merge.policy.segments_per_tier", 100)
            .put("index.merge.policy.floor_segment", "1gb")
            .build();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).get();
        ensureGreen(INDEX_NAME);

        // 15 batches × 5 docs with explicit refresh between → ~13 distinct Lucene segments,
        // well above the TieredMergePolicy default segments_per_tier threshold.
        int numBatches = 15;
        int docsPerBatch = 5;
        for (int b = 0; b < numBatches; b++) {
            for (int d = 0; d < docsPerBatch; d++) {
                client().prepareIndex(INDEX_NAME).setSource("name", "b" + b + "_d" + d, "value", b * 100 + d).get();
            }
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }

        // Flush so segments are persisted in a real Lucene commit (segments_<N>).
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();

        // Restart the data node holding the primary — forces engine teardown and reinit
        // through LuceneCommitter.<init> → SafeBootstrapCommitter ctor →
        // discoverAndTrimUnsafeCommits, which is where the bug fires.
        String primaryNode = primaryNodeName();
        internalCluster().restartNode(primaryNode);

        // Without the fix, the new MergeIndexWriter cannot open at the (now polluted)
        // commit and the shard fails to start, so the cluster never reaches GREEN.
        // Bound the wait so a buggy build fails fast rather than silently retrying for
        // the default ensureGreen window.
        ClusterHealthStatus status = client().admin()
            .cluster()
            .prepareHealth(INDEX_NAME)
            .setWaitForGreenStatus()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .get()
            .getStatus();
        assertEquals(
            "Index ["
                + INDEX_NAME
                + "] must reach GREEN after engine reinit; the indexSort bug in "
                + "discoverAndTrimUnsafeCommits prevents this when the safe commit has many segments.",
            ClusterHealthStatus.GREEN,
            status
        );
    }
}
