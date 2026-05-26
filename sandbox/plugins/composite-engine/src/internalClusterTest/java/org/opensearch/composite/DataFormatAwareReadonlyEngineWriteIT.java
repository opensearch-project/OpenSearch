/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;

import java.util.Set;

/**
 * Verifies that the warm read-only engine rejects all write operations
 * and that flush/refresh are no-ops.
 */
public class DataFormatAwareReadonlyEngineWriteIT extends DataFormatAwareReadonlyEngineBaseIT {

    public void testWarmEngineFlipAndWriteRejection() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        createHotIndexAndTierToWarm(0);

        // Verify engine type
        String primaryNode = primaryNodeName();
        IndexShard primaryShard = getIndexShard(primaryNode);
        Indexer indexer = IndexShardTestCase.getIndexer(primaryShard);
        assertTrue(
            "warm primary must use DataFormatAwareReadOnlyEngine, got: " + indexer.getClass().getSimpleName(),
            indexer instanceof DataFormatAwareReadOnlyEngine
        );

        // Verify on warm-eligible node
        org.opensearch.cluster.node.DiscoveryNode dn = getClusterState().nodes().resolveNode(primaryNode);
        assertTrue("primary must be on warm node", dn.getRoles().contains(DiscoveryNodeRole.WARM_ROLE));

        // Verify catalog has files
        Set<String> catalogFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(primaryShard);
        assertFalse("catalog must have files after tiering", catalogFiles.isEmpty());

        // Index must be rejected
        try {
            client().prepareIndex(INDEX_NAME).setId("new-doc").setSource("field_text", "fail").get();
            fail("index should be rejected");
        } catch (Exception e) {
            assertTrue("expected rejection, got: " + e.getMessage(), e.getMessage().contains("does not support"));
        }

        // Delete must be rejected
        try {
            client().prepareDelete(INDEX_NAME, "0").get();
            fail("delete should be rejected");
        } catch (Exception e) {
            assertTrue("expected rejection, got: " + e.getMessage(), e.getMessage().contains("does not support"));
        }

        // Update must be rejected
        try {
            client().prepareUpdate(INDEX_NAME, "0").setDoc("field_text", "updated").get();
            fail("update should be rejected");
        } catch (Exception e) {
            assertTrue(
                "expected rejection, got: " + e.getMessage(),
                e.getMessage().contains("does not support") || e.getMessage().contains("Cannot apply function on indexer")
            );
        }

        // Flush is no-op (should not throw)
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Refresh is no-op (should not throw)
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
    }
}
