/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.function.Function;

/**
 * Reproduces the merge-pool starvation problem for the composite data format
 * (parquet primary + lucene secondary).
 *
 * <p>The native merge reserves its row-id mapping ({@code total_rows * 8} bytes) from the
 * merge pool with {@code PoolBehavior::Reject}. When the merge pool limit is smaller than
 * the mapping, {@code try_grow} fails and the merge is rejected. Because a rejected merge
 * never raises the pool's observed utilization, the rebalancer never grows the pool, so the
 * merge can never succeed on retry — a force merge fails permanently.
 *
 * <p>This test pins the merge pool to a 1–2 byte window and disables the rebalancer so the
 * failure is deterministic. It is the baseline that the planned soft-limit / over-commit fix
 * must turn green.
 */
// The Tokio IO runtime worker thread used by the Rust merge is a process-lifetime singleton
// that persists after tests complete (mirrors CompositeMergeIT).
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeMergePoolExhaustionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-merge-pool-exhaustion";

    // ══════════════════════════════════════════════════════════════════════
    // Framework lifecycle & configuration
    // ══════════════════════════════════════════════════════════════════════

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            // Starve the merge pool: 1-byte floor, 2-byte ceiling. Any real merge's row-id
            // mapping reservation (total_rows * 8 bytes) exceeds this and is rejected.
            .put("parquet.native.pool.merge.min", 1L)
            .put("parquet.native.pool.merge.max", 2L)
            // Disable the rebalancer so the merge pool cannot be grown back. A rejected merge
            // never registers utilization, so this reproduces the starvation deterministically.
            .put("native.allocator.rebalancer.enabled", false)
            .build();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Test
    // ══════════════════════════════════════════════════════════════════════

    /**
     * With the merge pool pinned to 2 bytes, a force merge of multiple segments must fail
     * because the native merge's row-id mapping reservation is rejected.
     */
    public void testForceMergeFailsWhenMergePoolExhausted() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        // Two ingest + refresh cycles → at least two committed segments to merge.
        indexAndRefresh(10);
        indexAndRefresh(10);

        assertTrue("expected multiple segments before force merge", getSegmentCount() > 1);

        // Over-commit is enabled by default; disable it explicitly so the exhausted merge pool
        // rejects the reservation, reproducing the force-merge-fails-forever behavior.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("native.allocator.overcommit.enabled", false))
            .get();

        // Force merge to a single segment. The 2-byte merge pool rejects the native merge's
        // row-id mapping reservation, so the merge fails. The failure surfaces either as a
        // failed shard in the response or as a thrown exception — accept both.
        boolean forceMergeFailed;
        String failureDetail;
        try {
            ForceMergeResponse response = client().admin()
                .indices()
                .prepareForceMerge(INDEX_NAME)
                .setMaxNumSegments(1)
                .setFlush(false)
                .get();
            forceMergeFailed = response.getFailedShards() >= 1 && response.getSuccessfulShards() == 0;
            failureDetail = "failedShards="
                + response.getFailedShards()
                + " successfulShards="
                + response.getSuccessfulShards()
                + " shardFailures="
                + Arrays.toString(response.getShardFailures());
            // When surfaced via the response, the reason should reference the exhausted merge pool.
            if (forceMergeFailed) {
                boolean mentionsPool = Arrays.stream(response.getShardFailures())
                    .anyMatch(f -> f.reason() != null && f.reason().toLowerCase(Locale.ROOT).contains("merge pool"));
                assertTrue("force merge failure should reference the merge pool; detail=" + failureDetail, mentionsPool);
            }
        } catch (Exception e) {
            forceMergeFailed = true;
            failureDetail = e.toString();
        }

        assertTrue("force merge should fail when the merge pool is exhausted; detail=" + failureDetail, forceMergeFailed);

        // The segments must not have been consolidated to a single segment.
        assertTrue("segments should not have been merged when the merge pool is exhausted", getSegmentCount() > 1);
    }

    /**
     * With the same 2-byte merge pool but the over-commit fallback enabled and node native-memory
     * pressure below the threshold, the force merge must now succeed: the rejected row-id mapping
     * reservation is granted an over-commit by the allocator, and the merge consolidates to a single
     * segment.
     */
    public void testForceMergeSucceedsWithOverCommitEnabled() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        indexAndRefresh(10);
        indexAndRefresh(10);
        assertTrue("expected multiple segments before force merge", getSegmentCount() > 1);

        // Over-commit is enabled by default. Raise the pressure threshold so the node's (low)
        // native-memory pressure is comfortably below it; the pressure signal itself is the real
        // injected node signal, so no per-instance stubbing is needed. With the fallback active, the
        // merge's row-id mapping reservation is granted an over-commit instead of being rejected.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("native.allocator.overcommit.pressure_threshold", 100.0))
            .get();

        ForceMergeResponse response = client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();

        assertEquals(
            "no shard should fail the merge once over-commit is enabled; shardFailures=" + Arrays.toString(response.getShardFailures()),
            0,
            response.getFailedShards()
        );
        assertTrue("at least one shard should have merged", response.getSuccessfulShards() >= 1);

        // Segments consolidated to a single segment.
        assertBusy(() -> assertEquals("segments should be merged to one", 1, getSegmentCount()), 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * Over-commit is enabled, but the node native-memory pressure is manually driven above the
     * configured threshold. The allocator's decider must refuse the over-commit, so the force merge
     * still fails on the exhausted merge pool.
     */
    public void testForceMergeRejectedWhenNativePressureAboveThreshold() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        indexAndRefresh(10);
        indexAndRefresh(10);
        assertTrue("expected multiple segments before force merge", getSegmentCount() > 1);

        // Over-commit is enabled by default; set a threshold below the pressure we will inject.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("native.allocator.overcommit.pressure_threshold", 80.0))
            .get();

        // Drive native-memory pressure above the threshold on every node's allocator. The pressure
        // signal has no cluster setting, and the shared-JVM decider may resolve to any node's
        // allocator, so inject it everywhere.
        for (String node : internalCluster().getNodeNames()) {
            internalCluster().getInstance(ArrowNativeAllocator.class, node).setNativeMemoryPressureSupplier(() -> 95.0);
        }

        boolean forceMergeFailed;
        String failureDetail;
        try {
            ForceMergeResponse response = client().admin()
                .indices()
                .prepareForceMerge(INDEX_NAME)
                .setMaxNumSegments(1)
                .setFlush(false)
                .get();
            forceMergeFailed = response.getFailedShards() >= 1 && response.getSuccessfulShards() == 0;
            failureDetail = Arrays.toString(response.getShardFailures());
        } catch (Exception e) {
            forceMergeFailed = true;
            failureDetail = e.toString();
        }

        assertTrue(
            "force merge should fail when native pressure exceeds the over-commit threshold; detail=" + failureDetail,
            forceMergeFailed
        );
        assertTrue("segments should not have been merged when over-commit is refused", getSegmentCount() > 1);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: index settings & indexing
    // ══════════════════════════════════════════════════════════════════════

    private Settings compositeSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    /**
     * Indexes {@code numDocs} documents sequentially, then refreshes once. Sequential indexing
     * keeps each refresh cycle to a single writer (one committed segment), and avoids triggering
     * merge-on-refresh so the segment count is deterministic.
     */
    private void indexAndRefresh(int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setSource("name", randomAlphaOfLength(10), "age", randomIntBetween(1, 1000))
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: shard/catalog accessors
    // ══════════════════════════════════════════════════════════════════════

    private int getSegmentCount() throws IOException {
        return getCatalogSnapshot().getSegments().size();
    }

    private IndexShard getPrimaryShard() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeName = getClusterState().nodes().get(nodeId).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    private DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndexShard shard = getPrimaryShard();
        try (GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot()) {
            return DataformatAwareCatalogSnapshot.deserializeFromString(snapshot.get().serializeToString(), Function.identity());
        }
    }
}
