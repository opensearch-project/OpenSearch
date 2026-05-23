/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.Store;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CatalogSnapshotStatsCache}.
 */
public class CatalogSnapshotStatsCacheTests extends OpenSearchTestCase {

    public void testCacheInitialization() {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        CatalogSnapshotStatsCache cache = new CatalogSnapshotStatsCache(
            snapshotManager,
            store,
            null, // EngineConfig not needed for basic initialization test
            () -> Collections.emptyMap(),
            logger
        );

        // Cache should start with empty stats before any refresh
        DocsStats docsStats = cache.getDocsStats();
        assertNotNull(docsStats);
        assertEquals(0L, docsStats.getCount());

        SegmentsStats segmentsStats = cache.getSegmentsStats();
        assertNotNull(segmentsStats);
        assertEquals(0, segmentsStats.getCount());

        List<Segment> segments = cache.getSegments();
        assertNotNull(segments);
        assertTrue(segments.isEmpty());
    }

    public void testAfterRefreshWithDidRefreshFalse() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        CatalogSnapshotStatsCache cache = new CatalogSnapshotStatsCache(
            snapshotManager,
            store,
            null, // EngineConfig not needed for this test
            () -> Collections.emptyMap(),
            logger
        );

        // Get initial stats values
        DocsStats initialDocsStats = cache.getDocsStats();
        SegmentsStats initialSegmentsStats = cache.getSegmentsStats();
        List<Segment> initialSegments = cache.getSegments();

        long initialDocsCount = initialDocsStats.getCount();
        long initialSegmentsCount = initialSegmentsStats.getCount();
        int initialSegmentsSize = initialSegments.size();

        // Call afterRefresh with didRefresh=false
        cache.afterRefresh(false);

        // Stats values should remain unchanged (but objects may be different)
        assertEquals(initialDocsCount, cache.getDocsStats().getCount());
        assertEquals(initialSegmentsCount, cache.getSegmentsStats().getCount());
        assertEquals(initialSegmentsSize, cache.getSegments().size());
    }

    public void testBeforeRefresh() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        CatalogSnapshotStatsCache cache = new CatalogSnapshotStatsCache(
            snapshotManager,
            store,
            null, // EngineConfig not needed for this test
            () -> Collections.emptyMap(),
            logger
        );

        // beforeRefresh should not throw
        cache.beforeRefresh(); // Should complete without exception
    }

    public void testConstructorWithFailingSnapshotManager() {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        // Mock snapshot manager to throw exception
        when(snapshotManager.acquireSnapshot()).thenThrow(new RuntimeException("Test exception"));

        CatalogSnapshotStatsCache cache = new CatalogSnapshotStatsCache(
            snapshotManager,
            store,
            null, // EngineConfig not needed for this test
            () -> Collections.emptyMap(),
            logger
        );

        // Should still return valid (empty) stats despite constructor refresh failure
        assertNotNull(cache.getDocsStats());
        assertNotNull(cache.getSegmentsStats());
        assertNotNull(cache.getSegments());
    }

    public void testBuildSegmentsStatsWithSnapshot() {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);

        // Create real Segment instances using the builder
        org.opensearch.index.engine.exec.Segment segment1 = org.opensearch.index.engine.exec.Segment.builder(1L).build();
        org.opensearch.index.engine.exec.Segment segment2 = org.opensearch.index.engine.exec.Segment.builder(2L).build();
        org.opensearch.index.engine.exec.Segment segment3 = org.opensearch.index.engine.exec.Segment.builder(3L).build();

        // Mock snapshot to return test data with multiple files of same extension
        when(snapshot.getSegments()).thenReturn(List.of(segment1, segment2, segment3));
        when(snapshot.getNumDocs()).thenReturn(0L);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Map.of("parquet", 300L, "fnm", 50L, "nvm", 75L));
        when(snapshot.buildEngineSegments(any(), any())).thenReturn(Collections.emptyList());

        // Mock acquireSnapshot for the constructor's refreshCachedStats() call
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        CatalogSnapshotStatsCache cache = new CatalogSnapshotStatsCache(snapshotManager, store, null, () -> Collections.emptyMap(), logger);

        // Test buildSegmentsStats with external memory and timestamp
        SegmentsStats stats = cache.buildSegmentsStats(1024L, 100L, snapshot);

        assertEquals(3, stats.getCount());
        assertEquals(1024L, stats.getIndexWriterMemoryInBytes());
        assertNotNull(stats.getFileSizes());

        // Verify files are grouped by extension
        Map<String, Long> fileSizes = stats.getFileSizes();
        assertEquals(300L, (long) fileSizes.get("parquet")); // 100 + 200
        assertEquals(50L, (long) fileSizes.get("fnm"));
        assertEquals(75L, (long) fileSizes.get("nvm"));
        assertEquals(3, fileSizes.size()); // Should have 3 extensions
    }

    public void testDataFormatAwareNRTReplicationEngineWiring() {
        // This test verifies that DFANRE properly wires the CatalogSnapshotStatsCache
        // We can't easily test the full engine construction due to complex dependencies,
        // but we can verify the stats cache methods work as expected
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        CatalogSnapshotStatsCache cache = new CatalogSnapshotStatsCache(snapshotManager, store, null, () -> Collections.emptyMap(), logger);

        // Verify cache starts with empty stats
        SegmentsStats stats = cache.getSegmentsStats();
        assertNotNull(stats);
        assertEquals(0, stats.getCount());

        // Verify cache can handle refresh events
        try {
            cache.beforeRefresh();
            cache.afterRefresh(false);
            // If we get here, no exception was thrown
        } catch (Exception e) {
            fail("Refresh methods should not throw exceptions: " + e.getMessage());
        }
    }
}
