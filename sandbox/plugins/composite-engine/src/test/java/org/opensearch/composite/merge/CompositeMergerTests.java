/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.composite.CompositeDataFormat;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.composite.stats.CompositeShardStatsTracker;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.AuxiliaryDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.merge.DataFormatAwareMergePolicy;
import org.opensearch.index.engine.dataformat.merge.MergeHandler;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeMerger}.
 */
public class CompositeMergerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("test-index", "uuid"), 0);
    private static final RowIdMapping STUB_ROW_ID_MAPPING = new PackedRowIdMapping(new long[] { 0 }, false);

    private DataFormat primaryFormat;
    private DataFormat secondaryFormat;
    private Merger primaryMerger;
    private Merger secondaryMerger;
    private CompositeIndexingExecutionEngine compositeEngine;
    private CompositeDataFormat compositeDataFormat;
    private Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryFormat = stubFormat("lucene");
        secondaryFormat = stubFormat("parquet");
        primaryMerger = mock(Merger.class);
        secondaryMerger = mock(Merger.class);
        snapshotSupplier = () -> new GatedCloseable<>(null, () -> {});

        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        IndexingExecutionEngine<?, ?> secondaryEngine = mockEngine(secondaryFormat, secondaryMerger);

        compositeEngine = mock(CompositeIndexingExecutionEngine.class);
        when(compositeEngine.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        doReturn(primaryEngine).when(compositeEngine).getPrimaryDelegate();
        doReturn(Set.of(secondaryEngine)).when(compositeEngine).getSecondaryDelegates();
        when(compositeEngine.getNextWriterGeneration()).thenReturn(99L);

        compositeDataFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat, secondaryFormat));
    }

    // ========== doMerge: successful primary + secondary ==========

    public void testDoMergeSuccessWithPrimaryAndSecondary() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p1.dat"), 10);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s1.dat"), 10);

        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 10);
        WriterFileSet mergedSecondaryWfs = wfs(tempDir, 99L, Set.of("ms.dat"), 10);

        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        MergeResult secondaryResult = new MergeResult(Map.of(secondaryFormat, mergedSecondaryWfs));

        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenReturn(secondaryResult);

        MergeHandler handler = createHandler();
        MergeResult result = handler.doMerge(oneMerge);

        assertNotNull(result);
        assertEquals(2, result.getMergedWriterFileSet().size());
        assertSame(mergedPrimaryWfs, result.getMergedWriterFileSetForDataformat(primaryFormat));
        assertSame(mergedSecondaryWfs, result.getMergedWriterFileSetForDataformat(secondaryFormat));
    }

    // ========== doMerge: primary only (no secondaries) ==========

    public void testDoMergePrimaryOnlyNoSecondaries() throws IOException {
        CompositeIndexingExecutionEngine engineNoSecondary = mock(CompositeIndexingExecutionEngine.class);
        when(engineNoSecondary.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        doReturn(primaryEngine).when(engineNoSecondary).getPrimaryDelegate();
        doReturn(Set.of()).when(engineNoSecondary).getSecondaryDelegates();
        when(engineNoSecondary.getNextWriterGeneration()).thenReturn(50L);

        CompositeDataFormat primaryOnlyFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat));

        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, primaryWfs).build();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedWfs = wfs(tempDir, 50L, Set.of("merged.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedWfs));
        when(primaryMerger.merge(any())).thenReturn(primaryResult);

        MergeHandler handler = new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(engineNoSecondary, primaryOnlyFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );

        MergeResult result = handler.doMerge(oneMerge);
        assertNotNull(result);
        assertEquals(1, result.getMergedWriterFileSet().size());
        assertSame(mergedWfs, result.getMergedWriterFileSetForDataformat(primaryFormat));
    }

    // ========== doMerge: primary merge throws IOException ==========

    public void testDoMergePrimaryFailureThrowsUncheckedIOException() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        when(primaryMerger.merge(any())).thenThrow(new IOException("primary disk error"));

        MergeHandler handler = createHandler();
        UncheckedIOException ex = expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));
        assertNotNull(ex.getCause());
        assertEquals("primary disk error", ex.getCause().getMessage());
    }

    // ========== doMerge: single secondary failure ==========

    public void testDoMergeSingleSecondaryFailureThrowsUncheckedIOException() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("secondary disk error"));

        MergeHandler handler = createHandler();
        UncheckedIOException ex = expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));
        assertNotNull(ex.getCause());
        assertEquals("secondary disk error", ex.getCause().getMessage());
    }

    // ========== doMerge: multiple secondaries — fails fast on first error ==========

    public void testDoMergeMultipleSecondariesFailsFastOnFirstError() throws IOException {
        DataFormat secondaryFormat2 = stubFormat("arrow");
        Merger secondaryMerger2 = mock(Merger.class);

        CompositeIndexingExecutionEngine multiEngine = mock(CompositeIndexingExecutionEngine.class);
        when(multiEngine.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        doReturn(primaryEngine).when(multiEngine).getPrimaryDelegate();
        doReturn(Set.of(mockEngine(secondaryFormat, secondaryMerger), mockEngine(secondaryFormat2, secondaryMerger2))).when(multiEngine)
            .getSecondaryDelegates();
        when(multiEngine.getNextWriterGeneration()).thenReturn(99L);

        CompositeDataFormat multiFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat, secondaryFormat, secondaryFormat2));

        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet sWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        WriterFileSet s2Wfs = wfs(tempDir, 1L, Set.of("s2.dat"), 5);
        Segment segment = Segment.builder(0L)
            .addSearchableFiles(primaryFormat, pWfs)
            .addSearchableFiles(secondaryFormat, sWfs)
            .addSearchableFiles(secondaryFormat2, s2Wfs)
            .build();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("parquet error"));
        when(secondaryMerger2.merge(any())).thenThrow(new IOException("arrow error"));

        MergeHandler handler = new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(multiEngine, multiFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );

        UncheckedIOException ex = expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));
        assertNotNull(ex.getCause());
        // Fail-fast: only the first secondary failure is reported, no suppressed exceptions
        assertEquals(0, ex.getCause().getSuppressed().length);
    }

    // ========== doMerge: missing rowIdMapping throws IllegalStateException ==========

    public void testDoMergeMissingRowIdMappingThrowsIllegalState() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        // Primary result without rowIdMapping
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs));
        when(primaryMerger.merge(any())).thenReturn(primaryResult);

        MergeHandler handler = createHandler();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> handler.doMerge(oneMerge));
        assertTrue(ex.getMessage().contains("row-ID mapping"));
        assertTrue(ex.getMessage().contains("secondaries"));
    }

    // ========== doMerge: cleanup on failure deletes stale files ==========

    public void testDoMergeCleanupDeletesStaleMergedFilesOnFailure() throws IOException {
        Path tempDir = createTempDir();

        Path staleFile = tempDir.resolve("mp.dat");
        Files.createFile(staleFile);
        assertTrue(Files.exists(staleFile));

        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("secondary fail"));

        MergeHandler handler = createHandler();
        expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));

        assertFalse("Stale merged file should be deleted on failure", Files.exists(staleFile));
    }

    // ========== doMerge: cleanup handles non-existent files gracefully ==========

    public void testDoMergeCleanupHandlesNonExistentFilesGracefully() throws IOException {
        Path tempDir = createTempDir();

        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("nonexistent.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("fail"));

        MergeHandler handler = createHandler();
        // Should not throw during cleanup even though file doesn't exist
        expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));
    }

    // ========== doMerge: no cleanup when mergedWriterFileSet is empty ==========

    public void testDoMergeNoCleanupWhenPrimaryFails() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        when(primaryMerger.merge(any())).thenThrow(new IOException("primary fail"));

        MergeHandler handler = createHandler();
        UncheckedIOException ex = expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("primary fail", ex.getCause().getMessage());
    }

    // ========== doMerge: multiple segments ==========

    public void testDoMergeWithMultipleSegments() throws IOException {
        Path tempDir = createTempDir();
        WriterFileSet pWfs1 = wfs(tempDir, 1L, Set.of("p1.dat"), 5);
        WriterFileSet sWfs1 = wfs(tempDir, 1L, Set.of("s1.dat"), 5);
        WriterFileSet pWfs2 = wfs(tempDir, 2L, Set.of("p2.dat"), 5);
        WriterFileSet sWfs2 = wfs(tempDir, 2L, Set.of("s2.dat"), 5);

        Segment seg1 = buildSegment(1L, primaryFormat, pWfs1, secondaryFormat, sWfs1);
        Segment seg2 = buildSegment(2L, primaryFormat, pWfs2, secondaryFormat, sWfs2);
        OneMerge oneMerge = new OneMerge(List.of(seg1, seg2));

        WriterFileSet mergedPWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 10);
        WriterFileSet mergedSWfs = wfs(tempDir, 99L, Set.of("ms.dat"), 10);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPWfs), STUB_ROW_ID_MAPPING);
        MergeResult secondaryResult = new MergeResult(Map.of(secondaryFormat, mergedSWfs));

        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenReturn(secondaryResult);

        MergeHandler handler = createHandler();
        MergeResult result = handler.doMerge(oneMerge);

        assertNotNull(result);
        assertEquals(2, result.getMergedWriterFileSet().size());
        verify(primaryMerger, times(1)).merge(any());
        verify(secondaryMerger, times(1)).merge(any());
    }

    // ========== doMerge: secondary format equals primary is skipped ==========

    public void testDoMergeSkipsSecondaryThatEqualsPrimary() throws IOException {
        // The duplicate secondary has the same DataFormat as primary, so it should be skipped
        // in the secondary loop. We use the same primaryMerger for both to avoid NPE in the
        // constructor's dataFormatMergerMap (last-write-wins for same key).
        IndexingExecutionEngine<?, ?> primaryEngine = mockEngine(primaryFormat, primaryMerger);
        IndexingExecutionEngine<?, ?> duplicateEngine = mockEngine(primaryFormat, primaryMerger);

        CompositeIndexingExecutionEngine dupEngine = mock(CompositeIndexingExecutionEngine.class);
        when(dupEngine.statsTracker()).thenReturn(new CompositeShardStatsTracker());
        doReturn(primaryEngine).when(dupEngine).getPrimaryDelegate();
        doReturn(Set.of(duplicateEngine)).when(dupEngine).getSecondaryDelegates();
        when(dupEngine.getNextWriterGeneration()).thenReturn(99L);

        CompositeDataFormat dupFormat = new CompositeDataFormat(primaryFormat, List.of(primaryFormat));

        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        WriterFileSet mergedWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);

        MergeHandler handler = new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(dupEngine, dupFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );

        MergeResult result = handler.doMerge(oneMerge);
        assertNotNull(result);
        assertEquals(1, result.getMergedWriterFileSet().size());
    }

    // ========== findMerges ==========

    public void testFindMergesReturnsEmptyWhenNoSegments() {
        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(Collections.emptyList());
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        Collection<OneMerge> merges = handler.findMerges();
        assertNotNull(merges);
        assertTrue(merges.isEmpty());
    }

    public void testFindMergesThrowsOnSnapshotFailure() {
        snapshotSupplier = () -> { throw new RuntimeException("snapshot unavailable"); };

        MergeHandler handler = createHandler();
        RuntimeException ex = expectThrows(RuntimeException.class, handler::findMerges);
        assertTrue(ex.getMessage().contains("snapshot unavailable"));
    }

    // ========== findForceMerges ==========

    public void testFindForceMergesReturnsEmptyWhenNoSegments() {
        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(Collections.emptyList());
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        Collection<OneMerge> merges = handler.findForceMerges(1);
        assertNotNull(merges);
        assertTrue(merges.isEmpty());
    }

    public void testFindForceMergesThrowsOnSnapshotFailure() {
        snapshotSupplier = () -> { throw new RuntimeException("snapshot unavailable"); };

        MergeHandler handler = createHandler();
        RuntimeException ex = expectThrows(RuntimeException.class, () -> handler.findForceMerges(1));
        assertTrue(ex.getMessage().contains("snapshot unavailable"));
    }

    // ========== registerMerge / onMergeFinished / onMergeFailure ==========

    public void testRegisterMergeAndOnMergeFinished() {
        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(List.of(segment));
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        handler.registerMerge(oneMerge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFinished(oneMerge, false);
    }

    public void testRegisterMergeAndOnMergeFailure() {
        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(List.of(segment));
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        handler.registerMerge(oneMerge);
        assertTrue(handler.hasPendingMerges());

        handler.onMergeFailure(oneMerge);
        assertFalse(handler.hasPendingMerges());
    }

    public void testGetNextMergeReturnsNullWhenEmpty() {
        MergeHandler handler = createHandler();
        assertNull(handler.getNextMerge());
        assertFalse(handler.hasPendingMerges());
    }

    public void testGetNextMergeReturnsMergeAfterRegister() {
        Path tempDir = createTempDir();
        WriterFileSet pWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        Segment segment = Segment.builder(0L).addSearchableFiles(primaryFormat, pWfs).build();

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(List.of(segment));
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandler();
        OneMerge oneMerge = new OneMerge(List.of(segment));

        handler.registerMerge(oneMerge);
        OneMerge retrieved = handler.getNextMerge();
        assertNotNull(retrieved);
        assertSame(oneMerge, retrieved);
        assertFalse(handler.hasPendingMerges());
    }

    // ========== findMerges with merge candidates ==========

    public void testFindMergesReturnsMergeCandidates() throws IOException {
        Path tempDir = createTempDir();
        // Create many small segments with real files to trigger TieredMergePolicy
        List<Segment> segments = new java.util.ArrayList<>();
        for (int i = 0; i < 15; i++) {
            Path file = tempDir.resolve("seg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet pWfs = wfs(tempDir, i, Set.of("seg" + i + ".dat"), 10);
            segments.add(Segment.builder(i).addSearchableFiles(primaryFormat, pWfs).build());
        }

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(segments);
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandlerWithRealPolicy();
        Collection<OneMerge> merges = handler.findMerges();
        assertNotNull(merges);
        // TieredMergePolicy should find merge candidates with 15 small segments
        assertFalse("Expected merge candidates from 15 small segments", merges.isEmpty());
        for (OneMerge merge : merges) {
            assertFalse(merge.getSegmentsToMerge().isEmpty());
        }
    }

    // ========== findForceMerges with merge candidates ==========

    public void testFindForceMergesReturnsMergeCandidates() throws IOException {
        Path tempDir = createTempDir();
        List<Segment> segments = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Path file = tempDir.resolve("fseg" + i + ".dat");
            Files.write(file, new byte[100]);
            WriterFileSet pWfs = wfs(tempDir, i, Set.of("fseg" + i + ".dat"), 10);
            segments.add(Segment.builder(i).addSearchableFiles(primaryFormat, pWfs).build());
        }

        CatalogSnapshot catalogSnapshot = mockCatalogSnapshot(segments);
        snapshotSupplier = () -> new GatedCloseable<>(catalogSnapshot, () -> {});

        MergeHandler handler = createHandlerWithRealPolicy();
        // Force merge down to 1 segment should produce candidates
        Collection<OneMerge> merges = handler.findForceMerges(1);
        assertNotNull(merges);
        assertFalse("Expected force merge candidates when targeting 1 segment from 5", merges.isEmpty());
    }

    // ========== cleanup: exception during file deletion is logged but not thrown ==========

    public void testCleanupStaleMergedFilesLogsExceptionOnDeleteFailure() throws IOException {
        Path tempDir = createTempDir();
        // Create a directory with the same name as the file to delete — deleteIfExists on a
        // non-empty directory throws DirectoryNotEmptyException
        Path dirAsFile = tempDir.resolve("mp.dat");
        Files.createDirectory(dirAsFile);
        Files.createFile(dirAsFile.resolve("child.txt"));

        WriterFileSet primaryWfs = wfs(tempDir, 1L, Set.of("p.dat"), 5);
        WriterFileSet secondaryWfs = wfs(tempDir, 1L, Set.of("s.dat"), 5);
        Segment segment = buildSegment(0L, primaryFormat, primaryWfs, secondaryFormat, secondaryWfs);
        OneMerge oneMerge = new OneMerge(List.of(segment));

        // mergedPrimaryWfs points to "mp.dat" which is a non-empty directory
        WriterFileSet mergedPrimaryWfs = wfs(tempDir, 99L, Set.of("mp.dat"), 5);
        MergeResult primaryResult = new MergeResult(Map.of(primaryFormat, mergedPrimaryWfs), STUB_ROW_ID_MAPPING);
        when(primaryMerger.merge(any())).thenReturn(primaryResult);
        when(secondaryMerger.merge(any())).thenThrow(new IOException("secondary fail"));

        MergeHandler handler = createHandler();
        // The merge fails due to secondary, cleanup tries to delete "mp.dat" (a non-empty dir)
        // which throws DirectoryNotEmptyException — caught and logged, not re-thrown
        expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));
        // The directory should still exist since deleteIfExists fails on non-empty dirs
        assertTrue(Files.exists(dirAsFile));
    }

    // ========== Helper methods ==========

    private MergeHandler createHandler() {
        return new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(compositeEngine, compositeDataFormat),
            SHARD_ID,
            mock(MergeHandler.MergePolicy.class),
            mock(MergeHandler.MergeListener.class),
            () -> 1L
        );
    }

    private MergeHandler createHandlerWithRealPolicy() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(indexSettings.getMergePolicy(true), SHARD_ID);
        return new MergeHandler(
            snapshotSupplier,
            new CompositeMerger(compositeEngine, compositeDataFormat),
            SHARD_ID,
            policy,
            policy,
            () -> 1L
        );
    }

    private static DataFormat stubFormat(String name) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return 1;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }

            @Override
            public String toString() {
                return "StubFormat{" + name + "}";
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static IndexingExecutionEngine<?, ?> mockEngine(DataFormat format, Merger merger) {
        IndexingExecutionEngine<DataFormat, ?> engine = mock(IndexingExecutionEngine.class);
        when(engine.getDataFormat()).thenReturn(format);
        when(engine.getMerger()).thenReturn(merger);
        return engine;
    }

    private static WriterFileSet wfs(Path dir, long gen, Set<String> files, long numRows) {
        return new WriterFileSet(dir.toString(), gen, files, numRows, 0L);
    }

    private static Segment buildSegment(long generation, DataFormat fmt1, WriterFileSet wfs1, DataFormat fmt2, WriterFileSet wfs2) {
        return Segment.builder(generation).addSearchableFiles(fmt1, wfs1).addSearchableFiles(fmt2, wfs2).build();
    }

    private static CatalogSnapshot mockCatalogSnapshot(List<Segment> segments) {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(segments);
        return snapshot;
    }

    // ── Cross-format merge verification tests ──

    public void testExecutorThrowsWhenSecondaryReturnsNullButPrimaryHasOutput() throws IOException {
        Merger primaryMerger = mock(Merger.class);
        Merger secondaryMerger = mock(Merger.class);

        DataFormat primary = stubFormat("parquet", 0);
        DataFormat secondary = stubFormat("lucene", 50);

        String dir = createTempDir().toString();
        WriterFileSet primaryFiles = new WriterFileSet(dir, 10L, Set.of("file.parquet"), 100, 1L);

        RowIdMapping mapping = mock(RowIdMapping.class);
        when(mapping.size()).thenReturn(100);

        when(primaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of(primary, primaryFiles), mapping));
        when(secondaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of()));

        CompositeMergeExecutor executor = new CompositeMergeExecutor(Map.of(primary, primaryMerger, secondary, secondaryMerger));

        WriterFileSet inputP = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.parquet"), 50, 1L);
        WriterFileSet inputS = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.si"), 50, 1L);

        MergePlan plan = new MergePlan(10L, primary, List.of(secondary), Map.of(primary, List.of(inputP), secondary, List.of(inputS)));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> executor.execute(plan));
        assertTrue(ex.getMessage().contains("returned null"));
    }

    public void testExecutorThrowsOnRowCountMismatch() throws IOException {
        Merger primaryMerger = mock(Merger.class);
        Merger secondaryMerger = mock(Merger.class);

        DataFormat primary = stubFormat("parquet", 0);
        DataFormat secondary = stubFormat("lucene", 50);

        WriterFileSet primaryFiles = new WriterFileSet(createTempDir().toString(), 10L, Set.of("file.parquet"), 100, 1L);
        WriterFileSet secondaryFiles = new WriterFileSet(createTempDir().toString(), 10L, Set.of("file.si"), 90, 1L);

        RowIdMapping mapping = mock(RowIdMapping.class);
        when(mapping.size()).thenReturn(100);

        when(primaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of(primary, primaryFiles), mapping));
        when(secondaryMerger.merge(any(MergeInput.class))).thenReturn(new MergeResult(Map.of(secondary, secondaryFiles)));

        CompositeMergeExecutor executor = new CompositeMergeExecutor(Map.of(primary, primaryMerger, secondary, secondaryMerger));

        WriterFileSet inputP = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.parquet"), 50, 1L);
        WriterFileSet inputS = new WriterFileSet(createTempDir().toString(), 1L, Set.of("in.si"), 50, 1L);

        MergePlan plan = new MergePlan(10L, primary, List.of(secondary), Map.of(primary, List.of(inputP), secondary, List.of(inputS)));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> executor.execute(plan));
        assertTrue(ex.getMessage().contains("Row count mismatch"));
    }

    private static DataFormat stubFormat(String name, long priority) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return priority;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
    }

    // ── Side tables (nested child table) ──

    /** The child table sits beside the *secondary* (parquet) format in this fixture. */
    private String childFormatName() {
        return AuxiliaryDataFormat.nameFor(secondaryFormat.name(), AuxiliaryDataFormat.NESTED_CHILD_ROLE);
    }

    private Segment childSegment(Path dir, long parentGeneration, long elementRows) {
        long generation = AuxiliaryDataFormat.generationFor(parentGeneration);
        return Segment.builder(generation)
            .addSearchableFiles(childFormatName(), wfs(dir, generation, Set.of("child_" + generation + ".dat"), elementRows))
            .build();
    }

    public void testChildTableIsMergedThroughItsStorageFormatsMerger() throws IOException {
        Path tempDir = createTempDir();
        Segment documentSegment = buildSegment(
            1L,
            primaryFormat,
            wfs(tempDir, 1L, Set.of("p1.dat"), 2),
            secondaryFormat,
            wfs(tempDir, 1L, Set.of("s1.dat"), 2)
        );
        Segment child = childSegment(tempDir, 1L, 3);

        WriterFileSet mergedPrimary = wfs(tempDir, 99L, Set.of("mp.dat"), 2);
        WriterFileSet mergedSecondary = wfs(tempDir, 99L, Set.of("ms.dat"), 2);
        long expectedChildGeneration = AuxiliaryDataFormat.generationFor(99L);
        WriterFileSet mergedChild = wfs(tempDir, expectedChildGeneration, Set.of("mc.dat"), 3);

        when(primaryMerger.merge(any())).thenReturn(new MergeResult(Map.of(primaryFormat, mergedPrimary), STUB_ROW_ID_MAPPING));
        // The document parquet merge first, then the child table's — both are parquet, so both go to
        // the same merger.
        when(secondaryMerger.merge(any())).thenReturn(
            new MergeResult(Map.of(secondaryFormat, mergedSecondary)),
            new MergeResult(Map.of(secondaryFormat, mergedChild))
        );

        CompositeMerger merger = new CompositeMerger(compositeEngine, compositeDataFormat);
        MergeResult result = merger.merge(new MergeInput(List.of(documentSegment, child), null, 99L));

        // The documents merged as before.
        assertSame(mergedPrimary, result.getMergedWriterFileSetForDataformat(primaryFormat));
        assertSame(mergedSecondary, result.getMergedWriterFileSetForDataformat(secondaryFormat));

        // The child came back as its own segment, keyed by its catalog name at its derived generation.
        assertEquals(1, result.auxiliarySegments().size());
        Segment mergedChildSegment = result.auxiliarySegments().get(0);
        assertEquals(expectedChildGeneration, mergedChildSegment.generation());
        assertTrue(mergedChildSegment.isAuxiliaryOnly());
        assertSame(mergedChild, mergedChildSegment.dfGroupedSearchableFiles().get(childFormatName()));

        // The MergeInput handed to the parquet merger must be keyed by parquet — the merger looks its
        // inputs up by its own name, so a catalog-keyed input would find nothing to merge.
        ArgumentCaptor<MergeInput> inputs = ArgumentCaptor.forClass(MergeInput.class);
        verify(secondaryMerger, times(2)).merge(inputs.capture());
        MergeInput childInput = inputs.getAllValues().get(1);
        assertEquals(expectedChildGeneration, childInput.newWriterGeneration());
        assertEquals("child files must be reachable under the storage name", 1, childInput.getFilesForFormat(secondaryFormat.name()).size());
        assertTrue("child files must not be keyed by the catalog name", childInput.getFilesForFormat(childFormatName()).isEmpty());
    }

    public void testChildMergeReceivesTheParentRowIdMappingAsAHook() throws IOException {
        Path tempDir = createTempDir();
        Segment documentSegment = buildSegment(
            1L,
            primaryFormat,
            wfs(tempDir, 1L, Set.of("p1.dat"), 2),
            secondaryFormat,
            wfs(tempDir, 1L, Set.of("s1.dat"), 2)
        );

        when(primaryMerger.merge(any())).thenReturn(
            new MergeResult(Map.of(primaryFormat, wfs(tempDir, 99L, Set.of("mp.dat"), 2)), STUB_ROW_ID_MAPPING)
        );
        when(secondaryMerger.merge(any())).thenReturn(
            new MergeResult(Map.of(secondaryFormat, wfs(tempDir, 99L, Set.of("ms.dat"), 2))),
            new MergeResult(Map.of(secondaryFormat, wfs(tempDir, AuxiliaryDataFormat.generationFor(99L), Set.of("mc.dat"), 3)))
        );

        CompositeMerger merger = new CompositeMerger(compositeEngine, compositeDataFormat);
        merger.merge(new MergeInput(List.of(documentSegment, childSegment(tempDir, 1L, 3)), null, 99L));

        // Nothing applies it to a column yet (Phase 4b), but the mapping the documents' merge produced
        // is what a foreign-key rewrite would need, so it reaches the child merge.
        ArgumentCaptor<MergeInput> inputs = ArgumentCaptor.forClass(MergeInput.class);
        verify(secondaryMerger, times(2)).merge(inputs.capture());
        assertSame(STUB_ROW_ID_MAPPING, inputs.getAllValues().get(1).rowIdMapping());
    }

    public void testMergeOfOnlySideTablesThrows() {
        Path tempDir = createTempDir();
        CompositeMerger merger = new CompositeMerger(compositeEngine, compositeDataFormat);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> merger.merge(new MergeInput(List.of(childSegment(tempDir, 1L, 3)), null, 99L))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("holds only side tables"));
    }

    public void testSideTableOfAnUnknownStorageFormatThrows() throws IOException {
        Path tempDir = createTempDir();
        Segment documentSegment = buildSegment(
            1L,
            primaryFormat,
            wfs(tempDir, 1L, Set.of("p1.dat"), 2),
            secondaryFormat,
            wfs(tempDir, 1L, Set.of("s1.dat"), 2)
        );
        long childGeneration = AuxiliaryDataFormat.generationFor(1L);
        Segment strayChild = Segment.builder(childGeneration)
            .addSearchableFiles(
                AuxiliaryDataFormat.nameFor("some-other-format", AuxiliaryDataFormat.NESTED_CHILD_ROLE),
                wfs(tempDir, childGeneration, Set.of("x.dat"), 3)
            )
            .build();

        when(primaryMerger.merge(any())).thenReturn(
            new MergeResult(Map.of(primaryFormat, wfs(tempDir, 99L, Set.of("mp.dat"), 2)), STUB_ROW_ID_MAPPING)
        );
        when(secondaryMerger.merge(any())).thenReturn(new MergeResult(Map.of(secondaryFormat, wfs(tempDir, 99L, Set.of("ms.dat"), 2))));

        CompositeMerger merger = new CompositeMerger(compositeEngine, compositeDataFormat);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> merger.merge(new MergeInput(List.of(documentSegment, strayChild), null, 99L))
        );
        // Named by storage, not by catalog key — the point of the message is which merger is missing.
        assertTrue(e.getMessage(), e.getMessage().contains("some-other-format"));
        assertTrue(e.getMessage(), e.getMessage().contains("this composite does not hold"));
    }

    public void testSideTablesOfTwoDifferentRolesCannotShareAMerge() throws IOException {
        Path tempDir = createTempDir();
        Segment documentSegment = buildSegment(
            1L,
            primaryFormat,
            wfs(tempDir, 1L, Set.of("p1.dat"), 2),
            secondaryFormat,
            wfs(tempDir, 1L, Set.of("s1.dat"), 2)
        );
        long childGeneration = AuxiliaryDataFormat.generationFor(1L);
        Segment twoRoles = Segment.builder(childGeneration)
            .addSearchableFiles(childFormatName(), wfs(tempDir, childGeneration, Set.of("c1.dat"), 3))
            .addSearchableFiles(
                AuxiliaryDataFormat.nameFor(secondaryFormat.name(), "othertable"),
                wfs(tempDir, childGeneration, Set.of("c2.dat"), 4)
            )
            .build();

        when(primaryMerger.merge(any())).thenReturn(
            new MergeResult(Map.of(primaryFormat, wfs(tempDir, 99L, Set.of("mp.dat"), 2)), STUB_ROW_ID_MAPPING)
        );
        when(secondaryMerger.merge(any())).thenReturn(new MergeResult(Map.of(secondaryFormat, wfs(tempDir, 99L, Set.of("ms.dat"), 2))));

        CompositeMerger merger = new CompositeMerger(compositeEngine, compositeDataFormat);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> merger.merge(new MergeInput(List.of(documentSegment, twoRoles), null, 99L))
        );
        // One offset yields one auxiliary generation, so two roles would collide in the catalog.
        assertTrue(e.getMessage(), e.getMessage().contains("different roles"));
    }
}
