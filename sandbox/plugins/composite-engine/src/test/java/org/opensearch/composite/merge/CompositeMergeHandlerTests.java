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
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CompositeMergeHandler}.
 */
public class CompositeMergeHandlerTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("test-index", "uuid"), 0);
    private static final RowIdMapping STUB_ROW_ID_MAPPING = (oldId, oldGen) -> oldId;

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

        MergeHandler handler = CompositeMergeHandler.create(
            engineNoSecondary,
            primaryOnlyFormat,
            snapshotSupplier,
            createIndexSettings(),
            SHARD_ID
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
        assertEquals("Merge failed for shard", ex.getMessage());
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
        assertEquals("Merge failed for shard", ex.getMessage());
        assertEquals("secondary disk error", ex.getCause().getMessage());
    }

    // ========== doMerge: multiple secondaries — fails fast on first error ==========

    public void testDoMergeMultipleSecondariesFailsFastOnFirstError() throws IOException {
        DataFormat secondaryFormat2 = stubFormat("arrow");
        Merger secondaryMerger2 = mock(Merger.class);

        CompositeIndexingExecutionEngine multiEngine = mock(CompositeIndexingExecutionEngine.class);
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

        MergeHandler handler = CompositeMergeHandler.create(
            multiEngine,
            multiFormat,
            snapshotSupplier,
            createIndexSettings(),
            SHARD_ID
        );

        UncheckedIOException ex = expectThrows(UncheckedIOException.class, () -> handler.doMerge(oneMerge));
        assertEquals("Merge failed for shard", ex.getMessage());
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
        // IllegalStateException is not IOException — it propagates out of the secondary loop catch,
        // through the outer try, and is wrapped by the catch(IOException) only if it's an IOException.
        // Since it's not, it escapes as a raw IllegalStateException.
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> handler.doMerge(oneMerge));
        assertTrue(ex.getMessage().contains("row-ID mapping"));
        assertTrue(ex.getMessage().contains("secondary formats"));
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

        MergeHandler handler = CompositeMergeHandler.create(dupEngine, dupFormat, snapshotSupplier, createIndexSettings(), SHARD_ID);

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

        handler.onMergeFinished(oneMerge);
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

        MergeHandler handler = createHandler();
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

        MergeHandler handler = createHandler();
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
        return CompositeMergeHandler.create(compositeEngine, compositeDataFormat, snapshotSupplier, createIndexSettings(), SHARD_ID);
    }

    private static IndexSettings createIndexSettings() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
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
        return new WriterFileSet(dir.toString(), gen, files, numRows);
    }

    private static Segment buildSegment(long generation, DataFormat fmt1, WriterFileSet wfs1, DataFormat fmt2, WriterFileSet wfs2) {
        return Segment.builder(generation).addSearchableFiles(fmt1, wfs1).addSearchableFiles(fmt2, wfs2).build();
    }

    private static CatalogSnapshot mockCatalogSnapshot(List<Segment> segments) {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(segments);
        return snapshot;
    }
}
