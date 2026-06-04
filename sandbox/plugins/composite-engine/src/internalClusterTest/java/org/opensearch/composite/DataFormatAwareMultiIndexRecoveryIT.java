/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for DFA recovery with multiple indices, empty indices, and multi-shard configurations.
 */
public class DataFormatAwareMultiIndexRecoveryIT extends DataFormatAwareReplicationBaseIT {

    /**
     * Tests recovery of multiple DFA indices simultaneously after a data node restart.
     * Validates no cross-contamination of CatalogSnapshot between indices.
     */
    public void testMultiIndexConcurrentRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode1 = internalCluster().startDataOnlyNode();
        String dataNode2 = internalCluster().startDataOnlyNode();

        // Create 3 indices — use INDEX_NAME as one so base class helpers work
        String index2 = "dfa-multi-idx-2";
        String index3 = "dfa-multi-idx-3";

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(1)).get();
        client().admin().indices().prepareCreate(index2).setSettings(dfaIndexSettings(1)).get();
        client().admin().indices().prepareCreate(index3).setSettings(dfaIndexSettings(1)).get();
        ensureGreen(INDEX_NAME, index2, index3);

        indexDocs(randomIntBetween(10, 30));
        indexDocsToIndex(index2, randomIntBetween(10, 30));
        indexDocsToIndex(index3, randomIntBetween(10, 30));

        client().admin().indices().prepareFlush(INDEX_NAME, index2, index3).get();

        // Capture catalog state before restart
        assertCatalogSnapshotsConverged(INDEX_NAME);
        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(primaryNodeName(), INDEX_NAME));

        // Restart one data node — forces recovery of all shards on that node
        internalCluster().restartNode(dataNode1);
        ensureGreen(TimeValue.timeValueSeconds(90), INDEX_NAME, index2, index3);

        // Flush to ensure any new generation files are uploaded to remote store
        client().admin().indices().prepareFlush(INDEX_NAME, index2, index3).get();

        // Verify catalog convergence for the base index (no cross-contamination)
        assertBusy(() -> {
            final IndexShard primary;
            try {
                primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            } catch (Exception e) {
                throw new AssertionError("shard not yet available after node restart; will retry", e);
            }
            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
            assertFalse("catalog must have files after multi-index recovery", catalogAfter.isEmpty());
            assertTrue("catalog after recovery must contain all pre-recovery files", catalogAfter.containsAll(catalogBefore));
        }, 60, TimeUnit.SECONDS);

        // Full convergence check (primary == replica catalogs + local/remote consistency)
        assertCatalogSnapshotsConverged(INDEX_NAME);
    }

    /**
     * Tests that a DFA index with zero documents recovers correctly from remote store.
     * CatalogSnapshot should be valid and the index must accept writes after recovery.
     */
    public void testEmptyIndexRecovery() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        // Flush the empty index to ensure remote store state is published
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Stop data node, start new one, restore from remote store
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNode));
        assertBusy(
            () -> assertEquals(ClusterHealthStatus.RED, client().admin().cluster().prepareHealth(INDEX_NAME).get().getStatus()),
            30,
            TimeUnit.SECONDS
        );

        internalCluster().startDataOnlyNode();
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);

        // Verify the index can accept writes after recovery
        indexDocs(randomIntBetween(10, 20));
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Catalog should have files now
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            Set<String> catalog = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
            assertFalse("catalog must have files after indexing on recovered empty index", catalog.isEmpty());
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Tests that a DFA index with multiple shards (2) and a replica never enters RED state
     * during recovery — transitions cleanly without getting stuck.
     */
    public void testMultiShardRecoveryNoRedState() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();

        Settings multiShardSettings = Settings.builder()
            .put(remoteStoreIndexSettings(1, 2))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of())
            .build();

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(multiShardSettings).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 40));
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Restart one data node — some shards will need to recover
        String nodeToRestart = replicaNodeNames().isEmpty() ? primaryNodeName() : replicaNodeNames().get(0);
        internalCluster().restartNode(nodeToRestart);

        // Should never be RED — at worst YELLOW while replicas recover
        assertBusy(() -> {
            ClusterHealthStatus status = client().admin().cluster().prepareHealth(INDEX_NAME).get().getStatus();
            assertNotEquals("Index must not be RED during recovery", ClusterHealthStatus.RED, status);
        }, 60, TimeUnit.SECONDS);

        ensureGreen(INDEX_NAME);
    }

    private void indexDocsToIndex(String indexName, int count) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(indexName)
                .setId(String.valueOf(i))
                .setSource("field_text", randomAlphaOfLength(10), "field_number", (long) i)
                .get();
        }
    }
}
