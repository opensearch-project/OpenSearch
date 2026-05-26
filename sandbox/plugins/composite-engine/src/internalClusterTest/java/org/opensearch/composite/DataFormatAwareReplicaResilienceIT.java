/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.shard.IndexShard;

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
}
