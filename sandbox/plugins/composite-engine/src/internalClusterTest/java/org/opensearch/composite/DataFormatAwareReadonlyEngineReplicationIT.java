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
 * Verifies that warm replicas receive segment replication from the warm primary
 * and have converged catalog snapshots.
 */
public class DataFormatAwareReadonlyEngineReplicationIT extends DataFormatAwareReadonlyEngineBaseIT {

    public void testWarmReplicaReceivesReplication() throws Exception {
        // 2 nodes: primary + replica
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        createHotIndexAndTierToWarm(1);

        // Both primary and replica should use DataFormatAwareReadOnlyEngine
        String primaryNode = primaryNodeName();
        IndexShard primaryShard = getIndexShard(primaryNode);
        Indexer primaryIndexer = IndexShardTestCase.getIndexer(primaryShard);
        assertTrue("warm primary must use DataFormatAwareReadOnlyEngine", primaryIndexer instanceof DataFormatAwareReadOnlyEngine);

        // Verify catalog convergence between primary and replica
        Set<String> primaryCatalog = DataFormatAwareITUtils.catalogFilesExcludingSegments(primaryShard);
        assertFalse("primary catalog must have files", primaryCatalog.isEmpty());

        // Find replica node
        String replicaNodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).replicaShards().get(0).currentNodeId();
        String replicaNode = getClusterState().nodes().get(replicaNodeId).getName();
        IndexShard replicaShard = getIndexShard(replicaNode);

        // Replica catalog must match primary
        Set<String> replicaCatalog = DataFormatAwareITUtils.catalogFilesExcludingSegments(replicaShard);
        assertEquals("replica catalog must match primary", primaryCatalog, replicaCatalog);
    }
}
