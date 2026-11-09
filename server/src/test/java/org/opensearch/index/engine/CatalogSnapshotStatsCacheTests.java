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
import org.opensearch.index.engine.exec.DocCounts;
import org.opensearch.index.engine.exec.WriterFileSet;
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

    /**
     * Builds a cache whose liveness resolver reports nothing, which is what a shard made only of
     * formats that never hide a row (Parquet alone, for instance) does.
     */
    private static CatalogSnapshotStatsCache newCache(CatalogSnapshotManager snapshotManager, Store store, Logger logger) {
        return newCache(snapshotManager, store, logger, Map.of());
    }

    private static CatalogSnapshotStatsCache newCache(
        CatalogSnapshotManager snapshotManager,
        Store store,
        Logger logger,
        Map<Long, DocCounts> docCounts
    ) {
        return new CatalogSnapshotStatsCache(
            snapshotManager,
            store,
            null, // EngineConfig is only used for the index sort, which these tests do not exercise
            Collections::emptyMap,
            snapshot -> docCounts,
            logger
        );
    }

    private static void useRealDocCountAggregation(CatalogSnapshot snapshot) {
        when(snapshot.aggregateDocCounts(any())).thenCallRealMethod();
    }

    public void testCacheInitialization() {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

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

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

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

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

        // beforeRefresh should not throw
        cache.beforeRefresh(); // Should complete without exception
    }

    public void testConstructorWithFailingSnapshotManager() {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        // Mock snapshot manager to throw exception
        when(snapshotManager.acquireSnapshot()).thenThrow(new RuntimeException("Test exception"));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

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
        useRealDocCountAggregation(snapshot);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Map.of("parquet", 300L, "fnm", 50L, "nvm", 75L));
        when(snapshot.buildEngineSegments(any(), any(), any())).thenReturn(Collections.emptyList());

        // Mock acquireSnapshot for the constructor's refreshCachedStats() call
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

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

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

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

    public void testComputeDocsStatsTotalSizeFromMultipleSegments() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        // Create temp files with known sizes
        java.nio.file.Path tempDir = createTempDir();
        java.nio.file.Files.write(tempDir.resolve("seg1.parquet"), new byte[1000]);
        java.nio.file.Files.write(tempDir.resolve("seg2.parquet"), new byte[2500]);
        java.nio.file.Files.write(tempDir.resolve("seg3.lucene"), new byte[500]);

        WriterFileSet parquetSet1 = new WriterFileSet(tempDir.toString(), 1L, java.util.Set.of("seg1.parquet"), 5, 1L);
        WriterFileSet parquetSet2 = new WriterFileSet(tempDir.toString(), 2L, java.util.Set.of("seg2.parquet"), 3, 1L);
        WriterFileSet luceneSet = new WriterFileSet(tempDir.toString(), 1L, java.util.Set.of("seg3.lucene"), 5, 1L);

        org.opensearch.index.engine.exec.Segment segment1 = org.opensearch.index.engine.exec.Segment.builder(1L)
            .addSearchableFiles("parquet", parquetSet1)
            .addSearchableFiles("lucene", luceneSet)
            .build();
        org.opensearch.index.engine.exec.Segment segment2 = org.opensearch.index.engine.exec.Segment.builder(2L)
            .addSearchableFiles("parquet", parquetSet2)
            .build();

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment1, segment2));
        useRealDocCountAggregation(snapshot);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Collections.emptyMap());
        when(snapshot.buildEngineSegments(any(), any(), any())).thenReturn(Collections.emptyList());
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);
        cache.afterRefresh(true);

        DocsStats docsStats = cache.getDocsStats();
        // No format reported liveness, so all 5 + 3 rows count as live
        assertEquals(8L, docsStats.getCount());
        assertEquals(0L, docsStats.getDeleted());
        // totalSizeInBytes = 1000 + 2500 + 500 = 4000
        assertEquals(4000L, docsStats.getTotalSizeInBytes());
    }

    public void testComputeDocsStatsTotalSizeWithEmptySegments() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        // Segment with no file sets
        org.opensearch.index.engine.exec.Segment emptySegment = org.opensearch.index.engine.exec.Segment.builder(1L).build();

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(emptySegment));
        useRealDocCountAggregation(snapshot);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Collections.emptyMap());
        when(snapshot.buildEngineSegments(any(), any(), any())).thenReturn(Collections.emptyList());
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);
        cache.afterRefresh(true);

        DocsStats docsStats = cache.getDocsStats();
        assertEquals(0L, docsStats.getCount());
        assertEquals(0L, docsStats.getTotalSizeInBytes());
    }

    public void testComputeDocsStatsTotalSizeWithMissingFiles() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        // Create one real file and reference a non-existent file
        java.nio.file.Path tempDir = createTempDir();
        java.nio.file.Files.write(tempDir.resolve("exists.parquet"), new byte[750]);

        WriterFileSet fileSet = new WriterFileSet(tempDir.toString(), 1L, java.util.Set.of("exists.parquet", "missing.parquet"), 10, 1L);

        org.opensearch.index.engine.exec.Segment segment = org.opensearch.index.engine.exec.Segment.builder(1L)
            .addSearchableFiles("parquet", fileSet)
            .build();

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        useRealDocCountAggregation(snapshot);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Collections.emptyMap());
        when(snapshot.buildEngineSegments(any(), any(), any())).thenReturn(Collections.emptyList());
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

        cache.afterRefresh(true);
        DocsStats docsStats = cache.getDocsStats();
        assertEquals(10L, docsStats.getCount());
        // Only the existing file contributes; missing file returns 0
        assertEquals(750L, docsStats.getTotalSizeInBytes());
    }

    public void testComputeDocsStatsTotalSizeNoSegments() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(Collections.emptyList());
        useRealDocCountAggregation(snapshot);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Collections.emptyMap());
        when(snapshot.buildEngineSegments(any(), any(), any())).thenReturn(Collections.emptyList());
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger);

        cache.afterRefresh(true);
        DocsStats docsStats = cache.getDocsStats();
        assertEquals(0L, docsStats.getCount());
        assertEquals(0L, docsStats.getTotalSizeInBytes());
    }

    public void testDocsStatsReportsLiveAndDeletedCountsWhenReported() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        java.nio.file.Path tempDir = createTempDir();
        java.nio.file.Files.write(tempDir.resolve("gen1.parquet"), new byte[100]);
        java.nio.file.Files.write(tempDir.resolve("gen2.parquet"), new byte[100]);

        // Generation 1 wrote 10 rows, generation 2 wrote 4 rows: 14 rows on disk in total.
        WriterFileSet gen1Files = new WriterFileSet(tempDir.toString(), 1L, java.util.Set.of("gen1.parquet"), 10, 1L);
        WriterFileSet gen2Files = new WriterFileSet(tempDir.toString(), 2L, java.util.Set.of("gen2.parquet"), 4, 1L);

        org.opensearch.index.engine.exec.Segment gen1 = org.opensearch.index.engine.exec.Segment.builder(1L)
            .addSearchableFiles("parquet", gen1Files)
            .build();
        org.opensearch.index.engine.exec.Segment gen2 = org.opensearch.index.engine.exec.Segment.builder(2L)
            .addSearchableFiles("parquet", gen2Files)
            .build();

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(gen1, gen2));
        useRealDocCountAggregation(snapshot);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Collections.emptyMap());
        when(snapshot.buildEngineSegments(any(), any(), any())).thenReturn(Collections.emptyList());
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        // 6 of generation 1's rows were superseded or deleted, and 1 of generation 2's.
        Map<Long, DocCounts> reported = Map.of(1L, new DocCounts(4, 6), 2L, new DocCounts(3, 1));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger, reported);
        cache.afterRefresh(true);

        DocsStats docsStats = cache.getDocsStats();
        assertEquals(7L, docsStats.getCount());
        assertEquals(7L, docsStats.getDeleted());
        // The hidden rows still occupy their files, so the reported size covers all 14 rows.
        assertEquals(200L, docsStats.getTotalSizeInBytes());
    }

    public void testDocsStatsFallsBackToPhysicalRowsForUnreportedGenerations() throws IOException {
        CatalogSnapshotManager snapshotManager = mock(CatalogSnapshotManager.class);
        Store store = mock(Store.class);
        Logger logger = mock(Logger.class);

        java.nio.file.Path tempDir = createTempDir();
        java.nio.file.Files.write(tempDir.resolve("gen1.parquet"), new byte[100]);
        java.nio.file.Files.write(tempDir.resolve("gen2.parquet"), new byte[100]);

        WriterFileSet gen1Files = new WriterFileSet(tempDir.toString(), 1L, java.util.Set.of("gen1.parquet"), 10, 1L);
        WriterFileSet gen2Files = new WriterFileSet(tempDir.toString(), 2L, java.util.Set.of("gen2.parquet"), 5, 1L);

        org.opensearch.index.engine.exec.Segment gen1 = org.opensearch.index.engine.exec.Segment.builder(1L)
            .addSearchableFiles("parquet", gen1Files)
            .build();
        org.opensearch.index.engine.exec.Segment gen2 = org.opensearch.index.engine.exec.Segment.builder(2L)
            .addSearchableFiles("parquet", gen2Files)
            .build();

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(List.of(gen1, gen2));
        useRealDocCountAggregation(snapshot);
        when(snapshot.collectFileSizesGroupedByExtension(store)).thenReturn(Collections.emptyMap());
        when(snapshot.buildEngineSegments(any(), any(), any())).thenReturn(Collections.emptyList());
        when(snapshotManager.acquireSnapshot()).thenReturn(new GatedCloseable<>(snapshot, () -> {}));

        // Only generation 1 is reported; generation 2's 5 rows must all count as live.
        Map<Long, DocCounts> reported = Map.of(1L, new DocCounts(8, 2));

        CatalogSnapshotStatsCache cache = newCache(snapshotManager, store, logger, reported);
        cache.afterRefresh(true);

        DocsStats docsStats = cache.getDocsStats();
        assertEquals(13L, docsStats.getCount());
        assertEquals(2L, docsStats.getDeleted());
    }
}
