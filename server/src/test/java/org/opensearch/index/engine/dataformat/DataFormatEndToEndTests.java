/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareReaderManager;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.IndexFilterContext;
import org.opensearch.index.engine.exec.IndexFilterProvider;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;
import org.opensearch.search.SearchExecutionContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end test: write path (DataFormatPlugin) → catalog snapshot → read path (SearchAnalyticsBackEndPlugin).
 * Everything is mocked — no real files, no JNI, no cluster.
 */
public class DataFormatEndToEndTests extends OpenSearchTestCase {

    /**
     * Full lifecycle:
     * 1. DataFormatPlugin writes docs, flushes, refreshes → produces segments in a CatalogSnapshot
     * 2. CatalogSnapshot is published to SearchAnalyticsBackEndPlugin's reader manager
     * 3. Reader manager creates a reader from the snapshot's files
     * 4. SearchExecEngine creates a context from the reader, executes, returns results
     * 5. CatalogSnapshot is deleted → reader manager cleans up
     */
    public void testWritePathToReadPath() throws IOException {
        Path shardDir = createTempDir();

        // ---- WRITE PATH ----
        MockDataFormat format = new MockDataFormat();
        MockIndexingEngine engine = new MockIndexingEngine(format, shardDir);

        // Write 3 docs across 2 writer generations
        Writer<MockDocInput> w1 = engine.createWriter(1L);
        MockDocInput d1 = engine.newDocumentInput();
        d1.addField(mock(MappedFieldType.class), "Alice");
        d1.setRowId("_row_id", 0);
        w1.addDoc(d1);

        MockDocInput d2 = engine.newDocumentInput();
        d2.addField(mock(MappedFieldType.class), "Bob");
        d2.setRowId("_row_id", 1);
        w1.addDoc(d2);
        FileInfos batch1 = w1.flush();
        w1.close();

        Writer<MockDocInput> w2 = engine.createWriter(2L);
        MockDocInput d3 = engine.newDocumentInput();
        d3.addField(mock(MappedFieldType.class), "Charlie");
        d3.setRowId("_row_id", 2);
        w2.addDoc(d3);
        FileInfos batch2 = w2.flush();
        w2.close();

        // Refresh → produces segments
        WriterFileSet fs1 = batch1.getWriterFileSet(format).get();
        WriterFileSet fs2 = batch2.getWriterFileSet(format).get();
        RefreshResult refreshResult = engine.refresh(
            RefreshInput.builder().addWriterFileSet(fs1).addWriterFileSet(fs2).build()
        );
        assertEquals(2, refreshResult.refreshedSegments().size());

        // Build a mock CatalogSnapshot from the refresh result
        MockCatalogSnapshot catalogSnapshot = new MockCatalogSnapshot(1L, refreshResult.refreshedSegments(), format);

        // ---- READ PATH ----
        MockSearchBackendPlugin searchPlugin = new MockSearchBackendPlugin(format);

        // Create reader manager and search engine (what SearchBackendFactory would do)
        CatalogSnapshotAwareReaderManager<?> readerManager = searchPlugin.createReaderManager(MockDataFormat.SPI_FORMAT, null);
        SearchExecEngine<?, ?> searchEngine = searchPlugin.createSearchExecEngine(MockDataFormat.SPI_FORMAT, null);

        // Simulate refresh notification → reader manager picks up the catalog snapshot
        readerManager.beforeRefresh();
        readerManager.afterRefresh(true, catalogSnapshot);

        // Acquire reader from the snapshot
        Object reader = readerManager.getReader(catalogSnapshot);
        assertNotNull("Reader should be available after refresh", reader);

        // The reader should see all 3 docs' files
        @SuppressWarnings("unchecked")
        MockReader mockReader = (MockReader) reader;
        assertEquals(3, mockReader.totalRows);
        assertEquals(2, mockReader.fileNames.size()); // 2 writer file sets

        // Create search context and execute
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        SearchShardTarget target = new SearchShardTarget("node1",
            new org.opensearch.core.index.shard.ShardId("index", "uuid", 0), null, org.opensearch.action.OriginalIndices.NONE);
        SearchShardTask task = mock(SearchShardTask.class);

        @SuppressWarnings("unchecked")
        SearchExecEngine<MockSearchContext, String> typedEngine = (SearchExecEngine<MockSearchContext, String>) searchEngine;
        MockSearchContext ctx = typedEngine.createContext(reader, request, target, task);
        assertNotNull(ctx);
        assertEquals(3, ctx.totalRows);

        // Execute search
        typedEngine.execute(ctx);
        assertTrue("Search should have executed", ctx.executed);
        assertEquals(3, ctx.resultCount);

        // ---- CLEANUP ----
        // Simulate catalog snapshot deletion → reader manager cleans up
        readerManager.onDeleted(catalogSnapshot);
        assertTrue("Reader should be closed after snapshot deletion", mockReader.closed);
    }

    /**
     * Tests overlapping catalog snapshots: snapshot 1 is still in use when snapshot 2 arrives.
     * Deleting snapshot 1 should close its reader but leave snapshot 2's reader alive.
     * File add/delete notifications should propagate correctly.
     */
    public void testOverlappingSnapshotsAndFileLifecycle() throws IOException {
        Path shardDir = createTempDir();
        MockDataFormat format = new MockDataFormat();
        MockIndexingEngine engine = new MockIndexingEngine(format, shardDir);

        // Write batch 1
        Writer<MockDocInput> w1 = engine.createWriter(1L);
        MockDocInput d1 = engine.newDocumentInput();
        d1.addField(mock(MappedFieldType.class), "Alice");
        d1.setRowId("_row_id", 0);
        w1.addDoc(d1);
        FileInfos batch1 = w1.flush();
        w1.close();

        WriterFileSet fs1 = batch1.getWriterFileSet(format).get();
        RefreshResult refresh1 = engine.refresh(RefreshInput.builder().addWriterFileSet(fs1).build());
        MockCatalogSnapshot snapshot1 = new MockCatalogSnapshot(1L, refresh1.refreshedSegments(), format);

        // Setup read path
        MockSearchBackendPlugin searchPlugin = new MockSearchBackendPlugin(format);
        MockReaderManager readerManager = (MockReaderManager) searchPlugin.createReaderManager(MockDataFormat.SPI_FORMAT, null);

        // Publish snapshot 1
        readerManager.beforeRefresh();
        readerManager.afterRefresh(true, snapshot1);
        MockReader reader1 = readerManager.getReader(snapshot1);
        assertNotNull(reader1);
        assertEquals(1, reader1.totalRows);
        assertFalse(reader1.closed);

        // Notify files added (simulates what CompositeEngine does after refresh)
        readerManager.onFilesAdded(List.of("data_gen1.parquet"));
        assertTrue(readerManager.addedFiles.contains("data_gen1.parquet"));

        // Write batch 2 while snapshot 1 is still alive
        Writer<MockDocInput> w2 = engine.createWriter(2L);
        MockDocInput d2 = engine.newDocumentInput();
        d2.addField(mock(MappedFieldType.class), "Bob");
        d2.setRowId("_row_id", 1);
        w2.addDoc(d2);
        FileInfos batch2 = w2.flush();
        w2.close();

        WriterFileSet fs2 = batch2.getWriterFileSet(format).get();
        RefreshResult refresh2 = engine.refresh(RefreshInput.builder().addWriterFileSet(fs1).addWriterFileSet(fs2).build());
        MockCatalogSnapshot snapshot2 = new MockCatalogSnapshot(2L, refresh2.refreshedSegments(), format);

        // Publish snapshot 2 — both snapshots now coexist
        readerManager.beforeRefresh();
        readerManager.afterRefresh(true, snapshot2);
        MockReader reader2 = readerManager.getReader(snapshot2);
        assertNotNull(reader2);
        assertEquals(2, reader2.totalRows); // sees both batches
        assertFalse(reader2.closed);

        // Both readers alive
        assertFalse(reader1.closed);
        assertFalse(reader2.closed);
        assertEquals(2, readerManager.readerCount());

        // Delete snapshot 1 — its reader should close, snapshot 2 stays
        readerManager.onDeleted(snapshot1);
        assertTrue("Reader 1 should be closed", reader1.closed);
        assertFalse("Reader 2 should still be alive", reader2.closed);
        assertEquals(1, readerManager.readerCount());
        assertNull("getReader for deleted snapshot should return null", readerManager.getReader(snapshot1));

        // Notify files deleted (old files from snapshot 1 that are no longer needed)
        readerManager.onFilesDeleted(List.of("data_gen1_old_merged.parquet"));
        assertTrue(readerManager.deletedFiles.contains("data_gen1_old_merged.parquet"));

        // Snapshot 2 reader still works
        MockReader stillAlive = readerManager.getReader(snapshot2);
        assertSame(reader2, stillAlive);
        assertFalse(stillAlive.closed);

        // Delete snapshot 2 — everything cleaned up
        readerManager.onDeleted(snapshot2);
        assertTrue("Reader 2 should now be closed", reader2.closed);
        assertEquals(0, readerManager.readerCount());
    }

    /**
     * Tests that afterRefresh with didRefresh=false is a no-op,
     * and duplicate afterRefresh for same snapshot doesn't create a second reader.
     */
    public void testRefreshEdgeCases() throws IOException {
        Path shardDir = createTempDir();
        MockDataFormat format = new MockDataFormat();
        MockIndexingEngine engine = new MockIndexingEngine(format, shardDir);

        Writer<MockDocInput> w = engine.createWriter(1L);
        MockDocInput d = engine.newDocumentInput();
        d.addField(mock(MappedFieldType.class), "x");
        d.setRowId("_row_id", 0);
        w.addDoc(d);
        FileInfos batch = w.flush();
        w.close();

        RefreshResult rr = engine.refresh(RefreshInput.builder().addWriterFileSet(batch.getWriterFileSet(format).get()).build());
        MockCatalogSnapshot snapshot = new MockCatalogSnapshot(1L, rr.refreshedSegments(), format);

        MockSearchBackendPlugin plugin = new MockSearchBackendPlugin(format);
        MockReaderManager rm = (MockReaderManager) plugin.createReaderManager(MockDataFormat.SPI_FORMAT, null);

        // didRefresh=false → no reader created
        rm.afterRefresh(false, snapshot);
        assertNull(rm.getReader(snapshot));
        assertEquals(0, rm.readerCount());

        // didRefresh=true → reader created
        rm.afterRefresh(true, snapshot);
        assertNotNull(rm.getReader(snapshot));
        assertEquals(1, rm.readerCount());

        // Duplicate afterRefresh → same reader, no new one
        MockReader first = rm.getReader(snapshot);
        rm.afterRefresh(true, snapshot);
        assertSame(first, rm.getReader(snapshot));
        assertEquals(1, rm.readerCount());
    }

    /**
     * Tests that IndexFilterProvider works through the same lifecycle.
     */
    public void testIndexFilterProviderIntegration() throws IOException {
        MockDataFormat format = new MockDataFormat();
        MockSearchBackendPlugin plugin = new MockSearchBackendPlugin(format);

        IndexFilterProvider<?, ?> filterProvider = plugin.createIndexFilterProvider(MockDataFormat.SPI_FORMAT, null);
        assertNotNull(filterProvider);

        // Create a filter context with a mock query and reader
        @SuppressWarnings("unchecked")
        IndexFilterProvider<String, MockFilterContext> typedProvider = (IndexFilterProvider<String, MockFilterContext>) filterProvider;
        MockFilterContext filterCtx = typedProvider.createContext("field:Alice", new MockReader(List.of("f1.parquet"), 3));

        assertEquals(1, filterCtx.segmentCount());
        assertEquals(3, filterCtx.segmentMaxDoc(0));

        // Create collector, collect docs, release
        int collectorKey = typedProvider.createCollector(filterCtx, 0, 0, 3);
        assertTrue(collectorKey >= 0);

        long[] matchingDocs = typedProvider.collectDocs(filterCtx, collectorKey, 0, 3);
        assertNotNull(matchingDocs);
        // Mock returns all docs as matching
        assertTrue(matchingDocs.length > 0);

        typedProvider.releaseCollector(filterCtx, collectorKey);
        filterCtx.close();
    }

    // ========== Mock implementations ==========

    static class MockDataFormat implements DataFormat {
        /** The spi enum used by SearchAnalyticsBackEndPlugin. */
        static final org.opensearch.plugins.spi.vectorized.DataFormat SPI_FORMAT =
            org.opensearch.plugins.spi.vectorized.DataFormat.PARQUET;

        @Override
        public String name() {
            return "mock-columnar";
        }

        @Override
        public long priority() {
            return 100L;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of(new FieldTypeCapabilities("text", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)));
        }
    }

    /** What the reader manager hands to the search engine. */
    static class MockReader {
        final List<String> fileNames;
        final long totalRows;
        boolean closed = false;

        MockReader(List<String> fileNames, long totalRows) {
            this.fileNames = fileNames;
            this.totalRows = totalRows;
        }

        void close() {
            closed = true;
        }
    }

    /** Search context produced by the mock search engine. */
    static class MockSearchContext implements SearchExecutionContext {
        final long totalRows;
        boolean executed = false;
        int resultCount = 0;
        private final ShardSearchRequest request;
        private final SearchShardTarget target;

        MockSearchContext(long totalRows, ShardSearchRequest request, SearchShardTarget target) {
            this.totalRows = totalRows;
            this.request = request;
            this.target = target;
        }

        @Override
        public ShardSearchRequest request() {
            return request;
        }

        @Override
        public SearchShardTarget shardTarget() {
            return target;
        }

        @Override
        public void close() {}
    }

    /** Mock filter context for IndexFilterProvider. */
    static class MockFilterContext implements IndexFilterContext {
        final int totalDocs;
        final Map<Integer, Boolean> collectors = new HashMap<>();
        private int nextKey = 1;

        MockFilterContext(int totalDocs) {
            this.totalDocs = totalDocs;
        }

        @Override
        public int segmentCount() {
            return 1;
        }

        @Override
        public int segmentMaxDoc(int segmentOrd) {
            return totalDocs;
        }

        int addCollector() {
            int key = nextKey++;
            collectors.put(key, true);
            return key;
        }

        void removeCollector(int key) {
            collectors.remove(key);
        }

        @Override
        public void close() {
            collectors.clear();
        }
    }

    /** Mock CatalogSnapshot built from refresh results. */
    static class MockCatalogSnapshot extends CatalogSnapshot {
        private final List<Segment> segments;
        private final MockDataFormat format;

        MockCatalogSnapshot(long generation, List<Segment> segments, MockDataFormat format) {
            super("mock", generation, 1L);
            this.segments = segments;
            this.format = format;
        }

        @Override
        public Collection<org.opensearch.index.engine.exec.FileMetadata> getFileMetadataList() {
            return List.of();
        }

        @Override
        public Map<String, String> getUserData() {
            return Map.of();
        }

        @Override
        public long getId() {
            return generation;
        }

        @Override
        public List<org.opensearch.index.engine.exec.coord.Segment> getSegments() {
            return List.of();
        }

        @Override
        public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
            List<WriterFileSet> result = new ArrayList<>();
            for (Segment seg : segments) {
                WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(dataFormat);
                if (wfs != null) result.add(wfs);
            }
            return result;
        }

        @Override
        public Set<String> getDataFormats() {
            return Set.of(format.name());
        }

        @Override
        public long getLastWriterGeneration() {
            return generation;
        }

        @Override
        public String serializeToString() {
            return "mock-snapshot-" + generation;
        }

        @Override
        public void remapPaths(Path newShardDataPath) {}

        @Override
        public void setIndexFileDeleterSupplier(java.util.function.Supplier<org.opensearch.index.engine.exec.coord.IndexFileDeleter> s) {}

        @Override
        public void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> map) {}

        @Override
        public void setUserData(Map<String, String> userData, boolean b) {}

        @Override
        protected void closeInternal() {}
    }

    // ---- Write-path mocks (reused from DataFormatPluginTests) ----

    static class MockDocInput implements DocumentInput<Map<String, Object>> {
        private final Map<String, Object> fields = new HashMap<>();

        @Override
        public Map<String, Object> getFinalInput() {
            return Collections.unmodifiableMap(fields);
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            fields.put(fieldType != null ? fieldType.name() : "field_" + fields.size(), value);
        }

        @Override
        public void setRowId(String name, long rowId) {
            fields.put(name, rowId);
        }

        @Override
        public void close() {}
    }

    static class MockWriter implements Writer<MockDocInput> {
        private final long gen;
        private final DataFormat format;
        private final Path dir;
        private final List<MockDocInput> docs = new ArrayList<>();
        private final AtomicLong seqNo;

        MockWriter(long gen, DataFormat format, Path dir, AtomicLong seqNo) {
            this.gen = gen;
            this.format = format;
            this.dir = dir;
            this.seqNo = seqNo;
        }

        @Override
        public WriteResult addDoc(MockDocInput d) {
            docs.add(d);
            return new WriteResult.Success(1L, 1L, seqNo.getAndIncrement());
        }

        @Override
        public FileInfos flush() {
            WriterFileSet wfs = WriterFileSet.builder()
                .directory(dir)
                .writerGeneration(gen)
                .addFile("data_gen" + gen + ".parquet")
                .addNumRows(docs.size())
                .build();
            return FileInfos.builder().putWriterFileSet(format, wfs).build();
        }

        @Override
        public void sync() {}

        @Override
        public void close() {}
    }

    static class MockIndexingEngine implements IndexingExecutionEngine<DataFormat, MockDocInput> {
        private final MockDataFormat format;
        private final Path dir;
        private final AtomicLong seqNo = new AtomicLong(0);

        MockIndexingEngine(MockDataFormat format, Path dir) {
            this.format = format;
            this.dir = dir;
        }

        @Override
        public Writer<MockDocInput> createWriter(long gen) {
            return new MockWriter(gen, format, dir, seqNo);
        }

        @Override
        public Merger getMerger() {
            return null;
        }

        @Override
        public RefreshResult refresh(RefreshInput input) {
            List<Segment> segs = new ArrayList<>();
            long gen = 0;
            for (WriterFileSet wfs : input.writerFiles()) {
                segs.add(Segment.builder(gen++).addSearchableFiles(format, wfs).build());
            }
            return new RefreshResult(segs);
        }

        @Override
        public DataFormat getDataFormat() {
            return format;
        }

        @Override
        public void deleteFiles(Map<String, Collection<String>> f) {}

        @Override
        public MockDocInput newDocumentInput() {
            return new MockDocInput();
        }
    }

    // ---- Read-path mock plugin ----

    static class MockSearchBackendPlugin implements SearchAnalyticsBackEndPlugin {
        private final MockDataFormat format;

        MockSearchBackendPlugin(MockDataFormat format) {
            this.format = format;
        }

        @Override
        public String name() {
            return "mock-backend";
        }

        @Override
        public List<org.opensearch.plugins.spi.vectorized.DataFormat> getSupportedFormats() {
            return List.of(MockDataFormat.SPI_FORMAT);
        }

        @Override
        public CatalogSnapshotAwareReaderManager<MockReader> createReaderManager(
            org.opensearch.plugins.spi.vectorized.DataFormat fmt,
            ShardPath shardPath
        ) {
            return new MockReaderManager(format.name());
        }

        @Override
        public SearchExecEngine<MockSearchContext, String> createSearchExecEngine(
            org.opensearch.plugins.spi.vectorized.DataFormat fmt,
            ShardPath shardPath
        ) {
            return new MockSearchExecEngine();
        }

        @Override
        public IndexFilterProvider<String, MockFilterContext> createIndexFilterProvider(
            org.opensearch.plugins.spi.vectorized.DataFormat fmt,
            ShardPath shardPath
        ) {
            return new MockIndexFilterProvider();
        }
    }

    /** Reader manager: catalog snapshot → MockReader. Tracks file add/delete notifications. */
    static class MockReaderManager implements CatalogSnapshotAwareReaderManager<MockReader> {
        private final String formatName;
        private final Map<CatalogSnapshot, MockReader> readers = new HashMap<>();
        final List<String> addedFiles = new ArrayList<>();
        final List<String> deletedFiles = new ArrayList<>();

        MockReaderManager(String formatName) {
            this.formatName = formatName;
        }

        @Override
        public MockReader getReader(CatalogSnapshot snapshot) {
            return readers.get(snapshot);
        }

        int readerCount() {
            return readers.size();
        }

        @Override
        public void beforeRefresh() {}

        @Override
        public void afterRefresh(boolean didRefresh, CatalogSnapshot snapshot) {
            if (didRefresh == false || readers.containsKey(snapshot)) return;

            Collection<WriterFileSet> files = snapshot.getSearchableFiles(formatName);
            List<String> allFiles = new ArrayList<>();
            long totalRows = 0;
            for (WriterFileSet wfs : files) {
                allFiles.addAll(wfs.getFiles());
                totalRows += wfs.getNumRows();
            }
            readers.put(snapshot, new MockReader(allFiles, totalRows));
        }

        @Override
        public void onDeleted(CatalogSnapshot snapshot) {
            MockReader reader = readers.remove(snapshot);
            if (reader != null) reader.close();
        }

        @Override
        public void onFilesDeleted(Collection<String> files) {
            deletedFiles.addAll(files);
        }

        @Override
        public void onFilesAdded(Collection<String> files) {
            addedFiles.addAll(files);
        }
    }

    /** Search engine: reader → context → execute. */
    static class MockSearchExecEngine implements SearchExecEngine<MockSearchContext, String> {
        @Override
        public MockSearchContext createContext(Object reader, ShardSearchRequest req, SearchShardTarget target, SearchShardTask task) {
            MockReader r = (MockReader) reader;
            return new MockSearchContext(r.totalRows, req, target);
        }

        @Override
        public void execute(MockSearchContext ctx) {
            ctx.executed = true;
            ctx.resultCount = (int) ctx.totalRows; // mock: all rows match
        }
    }

    /** Index filter provider: query → bitset of matching docs. */
    static class MockIndexFilterProvider implements IndexFilterProvider<String, MockFilterContext> {
        @Override
        public MockFilterContext createContext(String query, Object reader) {
            MockReader r = (MockReader) reader;
            return new MockFilterContext((int) r.totalRows);
        }

        @Override
        public int createCollector(MockFilterContext ctx, int segmentOrd, int minDoc, int maxDoc) {
            return ctx.addCollector();
        }

        @Override
        public long[] collectDocs(MockFilterContext ctx, int collectorKey, int minDoc, int maxDoc) {
            // Mock: return a bitset with all docs matching
            java.util.BitSet bs = new java.util.BitSet(maxDoc - minDoc);
            bs.set(0, maxDoc - minDoc);
            return bs.toLongArray();
        }

        @Override
        public void releaseCollector(MockFilterContext ctx, int collectorKey) {
            ctx.removeCollector(collectorKey);
        }

        @Override
        public void close() {}
    }
}
