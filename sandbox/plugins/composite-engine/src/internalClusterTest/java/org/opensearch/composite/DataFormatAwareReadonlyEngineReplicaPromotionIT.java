/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;

import java.util.Set;

/**
 * Verifies that when the warm primary is cancelled (via reroute), the replica
 * is promoted to primary and continues using DataFormatAwareReadOnlyEngine
 * with the same catalog files.
 */
public class DataFormatAwareReadonlyEngineReplicaPromotionIT extends DataFormatAwareReadonlyEngineBaseIT {

    public void testReplicaPromotedAfterPrimaryCancelled() throws Exception {
        // 3 nodes: primary + replica + spare for re-allocation after promotion
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(3);
        createHotIndexAndTierToWarm(1);

        // Record state before promotion
        String warmPrimary = primaryNodeName();
        IndexShard primaryShard = getIndexShard(warmPrimary);
        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(primaryShard);
        assertFalse("catalog must have files", catalogBefore.isEmpty());

        // Cancel primary allocation → triggers promotion
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand(INDEX_NAME, 0, warmPrimary, true))
            .execute()
            .actionGet();

        ensureGreen(INDEX_NAME);

        // Verify primary term advanced
        String newPrimary = primaryNodeName();
        IndexShard newPrimaryShard = getIndexShard(newPrimary);
        assertTrue("primary term must advance", newPrimaryShard.getOperationPrimaryTerm() >= 2);

        // Verify new primary uses DataFormatAwareReadOnlyEngine
        Indexer newIndexer = IndexShardTestCase.getIndexer(newPrimaryShard);
        assertTrue(
            "promoted replica must use DataFormatAwareReadOnlyEngine, got: " + newIndexer.getClass().getSimpleName(),
            newIndexer instanceof DataFormatAwareReadOnlyEngine
        );

        // Verify catalog preserved
        Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(newPrimaryShard);
        assertEquals("catalog must be preserved across promotion", catalogBefore, catalogAfter);
    }
}
