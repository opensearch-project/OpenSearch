/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Integration tests for the read-only engine on indices that transition to
 * the warm tier. The read-only engine is the flip target when a shard is
 * reopened after being marked with {@code isWarmIndex()}.
 *
 * <p>Flow: create hot DFA index → index docs → close → mark warm → reopen → verify engine.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareWarmEngineIT extends DataFormatAwareReplicationBaseIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // Add file cache (required for warm directory stack) on top of the base remote store settings
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(16, ByteSizeUnit.GB).toString())
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    /** Hot DFA index settings (not warm) — used for initial index creation. */
    private Settings hotDfaIndexSettings(int replicaCount) {
        return super.dfaIndexSettings(replicaCount);
    }

    /**
     * Verifies that after marking a DFA index as warm (close → update → reopen),
     * the primary uses DataFormatAwareReadOnlyEngine.
     */
    public void testReadOnlyEngineAfterWarmFlip() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);

        // Create as hot DFA index, index docs, flush
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(hotDfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);
        indexDocs(10);
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Close → mark warm → reopen
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true))
            .get();
        client().admin().indices().prepareOpen(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        // The primary should now be using DataFormatAwareReadOnlyEngine
        String primaryNode = primaryNodeName();
        IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
        Indexer indexer = IndexShardTestCase.getIndexer(primaryShard);
        assertNotNull("primary shard must have an indexer", indexer);
        assertTrue(
            "primary after warm flip must use DataFormatAwareReadOnlyEngine, got: " + indexer.getClass().getSimpleName(),
            indexer instanceof DataFormatAwareReadOnlyEngine
        );

        // Verify basic engine properties
        assertFalse("read-only primary must not identify as replica", indexer.isReplicaIndexer());
        indexer.ensureOpen();
    }
}
