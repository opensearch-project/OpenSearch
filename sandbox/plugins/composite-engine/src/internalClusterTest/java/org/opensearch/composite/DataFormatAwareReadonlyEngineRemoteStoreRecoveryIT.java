/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Verifies that a warm index can recover from remote store after a node restart
 * or when a new replica is allocated to a fresh node.
 */
public class DataFormatAwareReadonlyEngineRemoteStoreRecoveryIT extends DataFormatAwareReadonlyEngineBaseIT {

    /**
     * Add a replica to a warm index (0 replicas initially) — the new replica
     * must recover from remote store and have the same catalog as the primary.
     */
    public void testNewReplicaRecoversFromRemoteStore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(3);

        // Create warm index with 0 replicas
        createHotIndexAndTierToWarm(0);

        // Record primary catalog
        String primaryNode = primaryNodeName();
        IndexShard primaryShard = getIndexShard(primaryNode);
        Set<String> primaryCatalog = DataFormatAwareITUtils.catalogFilesExcludingSegments(primaryShard);
        assertFalse("primary catalog must have files", primaryCatalog.isEmpty());

        // Increase replica count to 1 — triggers recovery from remote store
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        ensureGreen(INDEX_NAME);

        // Find the new replica
        String replicaNodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).replicaShards().get(0).currentNodeId();
        String replicaNode = getClusterState().nodes().get(replicaNodeId).getName();
        IndexShard replicaShard = getIndexShard(replicaNode);

        // Replica must have same catalog as primary
        assertBusy(() -> {
            Set<String> replicaCatalog = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(replicaNode));
            assertEquals("replica catalog must match primary after recovery", primaryCatalog, replicaCatalog);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Restart the warm primary node — after restart, the engine must recover
     * from remote store and maintain the same catalog.
     */
    public void testPrimaryRestartRecoversFromRemoteStore() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);

        createHotIndexAndTierToWarm(0);

        // Record catalog before restart
        String primaryNode = primaryNodeName();
        IndexShard primaryShard = getIndexShard(primaryNode);
        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(primaryShard);
        assertFalse("catalog must have files before restart", catalogBefore.isEmpty());

        // Restart the primary node
        internalCluster().restartNode(primaryNode);
        ensureGreen(INDEX_NAME);

        // After restart, verify engine type and catalog
        assertBusy(() -> {
            String newPrimary = primaryNodeName();
            IndexShard recovered = getIndexShard(newPrimary);
            Indexer indexer = IndexShardTestCase.getIndexer(recovered);
            assertTrue("recovered primary must use DataFormatAwareReadOnlyEngine", indexer instanceof DataFormatAwareReadOnlyEngine);
            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(recovered);
            assertEquals("catalog must be preserved after restart", catalogBefore, catalogAfter);
        }, 60, TimeUnit.SECONDS);
    }
}
