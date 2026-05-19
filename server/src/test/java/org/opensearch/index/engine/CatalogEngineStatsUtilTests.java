/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CatalogEngineStatsUtilTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(CatalogEngineStatsUtilTests.class);

    // ── buildSegmentsStats ──────────────────────────────────────────────────

    public void testBuildSegmentsStatsBasic() {
        SegmentsStats stats = CatalogEngineStatsUtil.buildSegmentsStats(3, 1024L, 100L, false, Collections.emptyList(), null, logger);
        assertEquals(3, stats.getCount());
        assertEquals(1024L, stats.getIndexWriterMemoryInBytes());
        assertTrue(stats.getFileSizes() == null || stats.getFileSizes().isEmpty());
    }

    public void testBuildSegmentsStatsWithFileSizes() throws IOException {
        Path dir = createTempDir();
        Path file = dir.resolve("test.parquet");
        Files.write(file, new byte[100]);

        WriterFileSet wfs = new WriterFileSet(dir.getFileName().toString(), 10L, Set.of("test.parquet"), 100L, 0L);

        SegmentsStats stats = CatalogEngineStatsUtil.buildSegmentsStats(1, 0L, 0L, true, List.of(wfs), dir.getParent(), logger);

        assertNotNull(stats.getFileSizes());
        assertFalse(stats.getFileSizes().isEmpty());
        assertEquals(100L, (long) stats.getFileSizes().get("test.parquet"));
    }

    public void testBuildSegmentsStatsNullDataPathSkipsFileSizes() {
        WriterFileSet wfs = new WriterFileSet("dir", 10L, Set.of("test.parquet"), 100L, 0L);
        SegmentsStats stats = CatalogEngineStatsUtil.buildSegmentsStats(1, 0L, 0L, true, List.of(wfs), null, logger);
        assertTrue(stats.getFileSizes() == null || stats.getFileSizes().isEmpty());
    }

    public void testBuildSegmentsStatsMissingFileReportsZero() throws IOException {
        Path dir = createTempDir();
        WriterFileSet wfs = new WriterFileSet(dir.getFileName().toString(), 10L, Set.of("nonexistent.parquet"), 100L);

        SegmentsStats stats = CatalogEngineStatsUtil.buildSegmentsStats(1, 0L, 0L, true, List.of(wfs), dir.getParent(), logger);

        assertNotNull(stats.getFileSizes());
        assertEquals(0L, (long) stats.getFileSizes().get("nonexistent.parquet"));
    }

    public void testBuildSegmentsStatsIncludeFileSizesFalseSkipsResolution() throws IOException {
        Path dir = createTempDir();
        Path file = dir.resolve("test.parquet");
        Files.write(file, new byte[100]);
        WriterFileSet wfs = new WriterFileSet(dir.getFileName().toString(), 10L, Set.of("test.parquet"), 100L);

        SegmentsStats stats = CatalogEngineStatsUtil.buildSegmentsStats(1, 0L, 0L, false, List.of(wfs), dir.getParent(), logger);

        assertTrue(stats.getFileSizes() == null || stats.getFileSizes().isEmpty());
    }

    // ── buildSegments ───────────────────────────────────────────────────────

    public void testBuildSegmentsEmpty() {
        List<org.opensearch.index.engine.Segment> result = CatalogEngineStatsUtil.buildSegments(
            Collections.emptyList(),
            true,
            null,
            logger
        );
        assertTrue(result.isEmpty());
    }

    public void testBuildSegmentsCommittedState() {
        Segment dfSeg = buildCatalogSegment(1L, 10L, 1024L);
        List<org.opensearch.index.engine.Segment> result = CatalogEngineStatsUtil.buildSegments(List.of(dfSeg), true, null, logger);
        assertEquals(1, result.size());
        assertTrue(result.get(0).committed);
        assertTrue(result.get(0).search);
    }

    public void testBuildSegmentsNotCommitted() {
        Segment dfSeg = buildCatalogSegment(1L, 10L, 1024L);
        List<org.opensearch.index.engine.Segment> result = CatalogEngineStatsUtil.buildSegments(List.of(dfSeg), false, null, logger);
        assertFalse(result.get(0).committed);
        assertTrue(result.get(0).search);
    }

    public void testBuildSegmentsSortedByGeneration() {
        Segment seg1 = buildCatalogSegment(3L, 5L, 100L);
        Segment seg2 = buildCatalogSegment(1L, 5L, 100L);
        Segment seg3 = buildCatalogSegment(2L, 5L, 100L);

        List<org.opensearch.index.engine.Segment> result = CatalogEngineStatsUtil.buildSegments(
            List.of(seg1, seg2, seg3),
            false,
            null,
            logger
        );

        assertEquals(1L, result.get(0).getGeneration());
        assertEquals(2L, result.get(1).getGeneration());
        assertEquals(3L, result.get(2).getGeneration());
    }

    public void testBuildSegmentsDocCountClamped() {
        long overMax = (long) Integer.MAX_VALUE + 1;
        Segment dfSeg = buildCatalogSegment(1L, overMax, 1024L);
        List<org.opensearch.index.engine.Segment> result = CatalogEngineStatsUtil.buildSegments(List.of(dfSeg), false, null, logger);
        assertEquals(Integer.MAX_VALUE, result.get(0).docCount);
    }

    public void testBuildSegmentsWithIndexSort() {
        Sort sort = new Sort(new SortField("age", SortField.Type.INT));
        Segment dfSeg = buildCatalogSegment(1L, 5L, 100L);
        List<org.opensearch.index.engine.Segment> result = CatalogEngineStatsUtil.buildSegments(List.of(dfSeg), false, sort, logger);
        assertEquals(sort, result.get(0).segmentSort);
    }

    public void testBuildSegmentsEmptySearchableFiles() {
        Segment dfSeg = Segment.builder(1L).build();
        List<org.opensearch.index.engine.Segment> result = CatalogEngineStatsUtil.buildSegments(List.of(dfSeg), false, null, logger);
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).docCount);
        assertEquals(0L, result.get(0).sizeInBytes);
    }

    // ── resolveIsCommitted ──────────────────────────────────────────────────

    public void testResolveIsCommittedNullSnapshot() {
        assertFalse(CatalogEngineStatsUtil.resolveIsCommitted(null, Map.of(), logger));
    }

    public void testResolveIsCommittedNullIdInCommitData() {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getId()).thenReturn(1L);
        assertFalse(CatalogEngineStatsUtil.resolveIsCommitted(snapshot, Map.of(), logger));
    }

    public void testResolveIsCommittedMatching() {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getId()).thenReturn(42L);
        Map<String, String> commitData = Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_ID, "42");
        assertTrue(CatalogEngineStatsUtil.resolveIsCommitted(snapshot, commitData, logger));
    }

    public void testResolveIsCommittedNotMatching() {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getId()).thenReturn(42L);
        Map<String, String> commitData = Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_ID, "99");
        assertFalse(CatalogEngineStatsUtil.resolveIsCommitted(snapshot, commitData, logger));
    }

    public void testResolveIsCommittedInvalidFormat() {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getId()).thenReturn(42L);
        Map<String, String> commitData = Map.of(CatalogSnapshot.CATALOG_SNAPSHOT_ID, "not-a-number");
        assertFalse(CatalogEngineStatsUtil.resolveIsCommitted(snapshot, commitData, logger));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private Segment buildCatalogSegment(long generation, long numRows, long totalSize) {
        WriterFileSet wfs = new WriterFileSet("parquet", numRows, Set.of(), totalSize);
        return Segment.builder(generation).addSearchableFiles(mock(org.opensearch.index.engine.dataformat.DataFormat.class), wfs).build();
    }
}
