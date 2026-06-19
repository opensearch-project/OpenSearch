/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.stats.CacheGroupStats;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;

import java.util.Set;

/**
 * Functional oracle for eager metadata-cache warming on warm shards.
 *
 * <p>When a shard is tiered to warm it runs {@link DataFormatAwareReadOnlyEngine}, whose
 * data lives on the (here, fs-backed) remote store via {@code TieredObjectStore}. The engine
 * fires {@code onFilesAdded} for the committed snapshot at open, which warms each parquet
 * footer into the node-level DataFusion metadata cache <em>before any query runs</em> — reading
 * the footer through the per-shard remote store pointer.
 *
 * <p>Proof: after tiering to warm and before issuing any query, the metadata cache on the
 * primary node already holds one entry per parquet segment ({@code entry_count == N}) with no
 * hits yet ({@code hit_count == 0}). Previously (lazy population) {@code entry_count} would be 0
 * until the first query touched each segment.
 *
 * <p>This uses the fs-backed native store from {@link DataFormatAwareReadonlyEngineBaseIT}
 * (ReloadableFsRepository + FsNativeObjectStorePlugin), so no real S3 is involved while still
 * exercising the {@code store_ptr > 0} warm path.
 */
public class DataFormatAwareWarmMetadataCacheIT extends DataFormatAwareReadonlyEngineBaseIT {

    public void testMetadataCacheEagerlyWarmedOnWarmEngineOpen() throws Exception {
        // Dedicated tiers: a hot data-only node and a warm-only node. The hot DFA index is created
        // on the data node (which warms ITS node-local cache during the hot phase). Tiering relocates
        // the shard to the warm-only node, whose DataFusion metadata cache is COLD (it never hosted
        // the hot shard). This isolates the warm engine's eager warming so we can assert a clean
        // hit_count == 0 in addition to entry_count == N.
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        internalCluster().startWarmOnlyNodes(1);
        createHotIndexAndTierToWarm(0);

        // Warm primary must run the read-only engine, and must be on a warm node.
        String primaryNode = primaryNodeName();
        IndexShard primaryShard = getIndexShard(primaryNode);
        Indexer indexer = IndexShardTestCase.getIndexer(primaryShard);
        assertTrue(
            "warm primary must use DataFormatAwareReadOnlyEngine, got: " + indexer.getClass().getSimpleName(),
            indexer instanceof DataFormatAwareReadOnlyEngine
        );
        assertTrue(
            "warm primary must be on a WARM_ROLE node",
            getClusterState().nodes().resolveNode(primaryNode).getRoles().contains(DiscoveryNodeRole.WARM_ROLE)
        );

        // Count the searchable parquet segment files in the catalog — these are exactly the
        // footers that should have been eagerly warmed (the metadata cache only caches *.parquet).
        Set<String> catalogFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(primaryShard);
        long parquetSegments = catalogFiles.stream().filter(f -> f.endsWith(".parquet")).count();
        assertTrue("warm shard must have at least one parquet segment, catalog=" + catalogFiles, parquetSegments > 0);

        // On the cold warm node, the metadata cache is populated solely by the read-only engine's
        // eager warming at open, before any query:
        // - entry_count == parquetSegments : every footer was cached proactively (was 0 when lazy).
        // - hit_count == 0 : nothing has read from the cache (no query, cold node).
        DataFusionService dataFusionService = internalCluster().getInstance(DataFusionService.class, primaryNode);
        CacheGroupStats metadataCache = dataFusionService.getStats().getCacheStats().getMetadataCache();

        assertEquals(
            "metadata cache should be eagerly warmed with one entry per parquet segment before any query",
            parquetSegments,
            metadataCache.entryCount
        );
        assertEquals("cold warm node, no query yet — cache must have zero hits", 0L, metadataCache.hitCount);
    }
}
