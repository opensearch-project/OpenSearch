/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.CatalogSnapshot;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;

/**
 * Tests demonstrating the data format plugin API lifecycle:
 * <ol>
 *   <li>A {@link DataFormatPlugin} provides a {@link DataFormat} and creates an {@link IndexingExecutionEngine}</li>
 *   <li>The engine creates {@link Writer} instances to write documents via {@link DocumentInput}</li>
 *   <li>Writers flush to produce {@link FileInfos} containing {@link WriterFileSet}s</li>
 *   <li>A {@link Merger} merges multiple writer file sets into a {@link MergeResult}</li>
 *   <li>The engine refreshes to produce searchable {@link Segment}s via {@link RefreshResult}</li>
 * </ol>
 */
public class DataFormatPluginTests extends OpenSearchTestCase {

    /**
     * End-to-end test: plugin → engine → write docs → flush → merge → refresh.
     */
    public void testFullDataFormatLifecycle() throws IOException {
        // 1. Create a mock DataFormatPlugin and obtain the engine
        DataFormatPlugin plugin = new MockDataFormatPlugin();
        DataFormat format = plugin.getDataFormat();
        assertEquals("mock-columnar", format.name());
        assertEquals(100L, format.priority());
        assertFalse(format.supportedFields().isEmpty());

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .build();
        @SuppressWarnings("unchecked")
        IndexingExecutionEngine<DataFormat, MockDocumentInput> engine = (IndexingExecutionEngine<DataFormat, MockDocumentInput>) plugin
            .indexingEngine(
                mock(MapperService.class),
                new ShardPath(false, Path.of("/tmp/uuid/0"), Path.of("/tmp/uuid/0"), new ShardId("index", "uuid", 0)),
                new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings),
                null
            );
        assertEquals(format, engine.getDataFormat());

        // 2. Create a writer and write documents
        Writer<MockDocumentInput> writer = engine.createWriter(1L);

        MockDocumentInput doc1 = engine.newDocumentInput();
        doc1.setRowId("_row_id", 0);
        doc1.addField(mock(MappedFieldType.class), "Alice");
        WriteResult result1 = writer.addDoc(doc1);
        assertEquals(WriteResult.Success.class, result1.getClass());

        MockDocumentInput doc2 = engine.newDocumentInput();
        doc2.setRowId("_row_id", 1);
        doc2.addField(mock(MappedFieldType.class), 30);
        WriteResult result2 = writer.addDoc(doc2);
        assertEquals(WriteResult.Success.class, result2.getClass());

        // 3. Flush the writer to produce file metadata
        FileInfos fileInfos = writer.flush();
        Optional<WriterFileSet> writerFileSet = fileInfos.getWriterFileSet(format);
        assertTrue(writerFileSet.isPresent());
        assertFalse(writerFileSet.get().files().isEmpty());
        assertEquals(2, writerFileSet.get().numRows());
        assertEquals(1L, writerFileSet.get().writerGeneration());

        writer.sync();
        writer.close();

        // 4. Write a second batch with a new writer generation
        Writer<MockDocumentInput> writer2 = engine.createWriter(2L);
        MockDocumentInput doc3 = engine.newDocumentInput();
        doc3.setRowId("_row_id", 2);
        doc3.addField(mock(MappedFieldType.class), "Bob");
        writer2.addDoc(doc3);
        FileInfos fileInfos2 = writer2.flush();
        writer2.close();

        WriterFileSet fileSet1 = fileInfos.getWriterFileSet(format).get();
        WriterFileSet fileSet2 = fileInfos2.getWriterFileSet(format).get();

        // 5. Merge the two writer file sets
        Merger merger = engine.getMerger();
        MergeInput mergeInput = MergeInput.builder().fileMetadataList(List.of(fileSet1, fileSet2)).newWriterGeneration(3L).build();
        MergeResult mergeResult = merger.merge(mergeInput);
        WriterFileSet merged = mergeResult.getMergedWriterFileSetForDataformat(format);
        assertNotNull(merged);
        assertEquals(3L, merged.writerGeneration());
        assertTrue(mergeResult.rowIdMapping().isPresent());

        // Verify row ID mapping
        RowIdMapping mapping = mergeResult.rowIdMapping().get();
        assertEquals(0L, mapping.getNewRowId(0, 1L));
        assertEquals(1L, mapping.getNewRowId(1, 1L));
        assertEquals(2L, mapping.getNewRowId(0, 2L));

        // 6. Merge with an existing RowIdMapping (secondary data format merge)
        MergeInput secondaryMergeInput = MergeInput.builder()
            .fileMetadataList(List.of(fileSet1, fileSet2))
            .rowIdMapping(mapping)
            .newWriterGeneration(4L)
            .build();
        MergeResult secondaryMerge = merger.merge(secondaryMergeInput);
        assertNotNull(secondaryMerge.getMergedWriterFileSetForDataformat(format));

        // 7. Refresh to produce searchable segments
        RefreshInput refreshInput = RefreshInput.builder().addWriterFileSet(merged).build();
        RefreshResult refreshResult = engine.refresh(refreshInput);
        assertFalse(refreshResult.refreshedSegments().isEmpty());
        Segment segment = refreshResult.refreshedSegments().get(0);
        assertNotNull(segment.dfGroupedSearchableFiles().get(format.name()));

        // 8. Delete files
        engine.deleteFiles(Map.of(merged.directory(), merged.files()));
        assertEquals(0L, engine.getNativeBytesUsed());
    }

    /**
     * Tests DataFormat equality semantics and field capabilities.
     */
    public void testDataFormatCapabilities() {
        MockDataFormat format = new MockDataFormat();
        Set<FieldTypeCapabilities> fields = format.supportedFields();
        assertEquals(1, fields.size());

        FieldTypeCapabilities cap = fields.iterator().next();
        assertEquals("integer", cap.fieldType());
        assertTrue(cap.capabilities().contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        assertTrue(cap.capabilities().contains(FieldTypeCapabilities.Capability.STORED_FIELDS));
    }

    /**
     * Tests FileInfos builder and empty factory.
     */
    public void testFileInfosBuilder() {
        DataFormat format = new MockDataFormat();
        Path dir = createTempDir();
        WriterFileSet fileSet = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("data.parquet").addNumRows(10).build();

        FileInfos infos = FileInfos.builder().putWriterFileSet(format, fileSet).build();
        assertTrue(infos.getWriterFileSet(format).isPresent());
        assertEquals(10, infos.getWriterFileSet(format).get().numRows());

        FileInfos empty = FileInfos.empty();
        assertTrue(empty.writerFilesMap().isEmpty());
    }

    /**
     * Tests WriteResult record fields.
     */
    public void testWriteResult() {
        WriteResult.Success success = new WriteResult.Success(1L, 1L, 42L);
        assertEquals(1L, success.version());
        assertEquals(1L, success.term());
        assertEquals(42L, success.seqNo());

        Exception ex = new IOException("disk full");
        WriteResult.Failure failure = new WriteResult.Failure(ex, -1L, -1L, -1L);
        assertSame(ex, failure.cause());
    }

    /**
     * Tests MergeResult with and without RowIdMapping.
     */
    public void testMergeResultWithAndWithoutRowIdMapping() {
        DataFormat format = new MockDataFormat();
        Path dir = createTempDir();
        WriterFileSet fileSet = WriterFileSet.builder().directory(dir).writerGeneration(1L).addNumRows(5).addFile("merged.parquet").build();

        MergeResult withoutMapping = new MergeResult(Map.of(format, fileSet));
        assertFalse(withoutMapping.rowIdMapping().isPresent());
        assertEquals(fileSet, withoutMapping.getMergedWriterFileSetForDataformat(format));

        RowIdMapping mapping = (oldId, oldGen) -> oldId;
        MergeResult withMapping = new MergeResult(Map.of(format, fileSet), mapping);
        assertTrue(withMapping.rowIdMapping().isPresent());
    }

    /**
     * Tests RefreshInput accumulation of writer files.
     */
    public void testRefreshInput() {
        RefreshInput empty = RefreshInput.builder().build();
        assertTrue(empty.writerFiles().isEmpty());
        assertTrue(empty.existingSegments().isEmpty());

        Path dir = createTempDir();
        WriterFileSet fs1 = new WriterFileSet(dir.toString(), 1L, Set.of(), 10);
        WriterFileSet fs2 = new WriterFileSet(dir.toString(), 2L, Set.of(), 20);
        Segment seg = new Segment(0L, Map.of());

        RefreshInput input = RefreshInput.builder().addWriterFileSet(fs1).addWriterFileSet(fs2).existingSegments(List.of(seg)).build();
        assertEquals(2, input.writerFiles().size());
        assertEquals(1, input.existingSegments().size());
    }

    static class MockDataFormat extends DataFormat {
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
            return Set.of(
                new FieldTypeCapabilities(
                    "integer",
                    Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.STORED_FIELDS)
                )
            );
        }
    }

    static class MockDocumentInput implements DocumentInput<Map<String, Object>> {
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
        public void setRowId(String rowIdFieldName, long rowId) {
            fields.put(rowIdFieldName, rowId);
        }

        @Override
        public void close() {}
    }

    static class MockWriter implements Writer<MockDocumentInput> {
        private final long writerGeneration;
        private final DataFormat dataFormat;
        private final Path directory;
        private final List<MockDocumentInput> docs = new ArrayList<>();
        private final AtomicLong seqNo;

        MockWriter(long writerGeneration, DataFormat dataFormat, Path directory, AtomicLong seqNo) {
            this.writerGeneration = writerGeneration;
            this.dataFormat = dataFormat;
            this.directory = directory;
            this.seqNo = seqNo;
        }

        @Override
        public WriteResult addDoc(MockDocumentInput d) {
            docs.add(d);
            long seq = seqNo.getAndIncrement();
            return new WriteResult.Success(1L, 1L, seq);
        }

        @Override
        public FileInfos flush() {
            WriterFileSet fileSet = WriterFileSet.builder()
                .directory(directory)
                .writerGeneration(writerGeneration)
                .addFile("data_gen" + writerGeneration + ".parquet")
                .addNumRows(docs.size())
                .build();
            return FileInfos.builder().putWriterFileSet(dataFormat, fileSet).build();
        }

        @Override
        public void sync() {}

        @Override
        public void close() {}
    }

    static class MockMerger implements Merger {
        private final DataFormat dataFormat;
        private final Path directory;

        MockMerger(DataFormat dataFormat, Path directory) {
            this.dataFormat = dataFormat;
            this.directory = directory;
        }

        @Override
        public MergeResult merge(MergeInput mergeInput) {
            List<WriterFileSet> fileMetadataList = mergeInput.writerFiles();
            long newWriterGeneration = mergeInput.newWriterGeneration();
            RowIdMapping existingMapping = mergeInput.rowIdMapping();

            String prefix = existingMapping != null ? "secondary_merged_gen" : "merged_gen";
            WriterFileSet merged = WriterFileSet.builder()
                .directory(directory)
                .writerGeneration(newWriterGeneration)
                .addFile(prefix + newWriterGeneration + ".parquet")
                .addNumRows(fileMetadataList.stream().mapToLong(WriterFileSet::numRows).sum())
                .build();

            if (existingMapping != null) {
                return new MergeResult(Map.of(dataFormat, merged), existingMapping);
            }

            // Build a simple sequential row ID mapping
            Map<Long, Long> genOffsets = new HashMap<>();
            long offset = 0;
            for (WriterFileSet fs : fileMetadataList) {
                genOffsets.put(fs.writerGeneration(), offset);
                offset += fs.numRows();
            }
            RowIdMapping mapping = (oldId, oldGeneration) -> genOffsets.getOrDefault(oldGeneration, 0L) + oldId;

            return new MergeResult(Map.of(dataFormat, merged), mapping);
        }
    }

    static class MockIndexingExecutionEngine implements IndexingExecutionEngine<DataFormat, MockDocumentInput> {
        private final MockDataFormat dataFormat;
        private final Path directory;
        private final AtomicLong seqNo = new AtomicLong(0);

        MockIndexingExecutionEngine(MockDataFormat dataFormat) {
            this.dataFormat = dataFormat;
            this.directory = createTempDir();
        }

        @Override
        public Writer<MockDocumentInput> createWriter(long writerGeneration) {
            return new MockWriter(writerGeneration, dataFormat, directory, seqNo);
        }

        @Override
        public Merger getMerger() {
            return new MockMerger(dataFormat, directory);
        }

        @Override
        public RefreshResult refresh(RefreshInput refreshInput) {
            List<Segment> segments = new ArrayList<>();
            long gen = 0;
            for (WriterFileSet wfs : refreshInput.writerFiles()) {
                segments.add(Segment.builder(gen++).addSearchableFiles(dataFormat, wfs).build());
            }
            return new RefreshResult(segments);
        }

        @Override
        public DataFormat getDataFormat() {
            return dataFormat;
        }

        @Override
        public void deleteFiles(Map<String, Collection<String>> filesToDelete) {
            // no-op for mock
        }

        @Override
        public MockDocumentInput newDocumentInput() {
            return new MockDocumentInput();
        }
    }

    static class MockDataFormatPlugin implements DataFormatPlugin {
        private final MockDataFormat dataFormat = new MockDataFormat();

        @Override
        public DataFormat getDataFormat() {
            return dataFormat;
        }

        @Override
        public IndexingExecutionEngine<?, ?> indexingEngine(
            MapperService mapperService,
            ShardPath shardPath,
            IndexSettings indexSettings,
            DataformatAwareLockableWriterPool<?> writerPool
        ) {
            return new MockIndexingExecutionEngine(dataFormat);
        }
    }

    /**
     * Search holds snapshot alive while refresh replaces it.
     * <p>
     * Timeline:
     * 1. new s1 → refcount = 1 (construction)
     * 2. setLatestSnapshot(s1) → refcount = 1 (engine takes over construction ref)
     * 3. acquireReader() → refcount = 2 (search adds ref)
     * 4. setLatestSnapshot(s2) → s1 refcount = 1 (engine releases s1)
     * 5. readerManager.onDeleted(s1) → reader closed, but s1 alive (search ref)
     * 6. compositeReader.close() → s1 refcount = 0 → dead
     */
    public void testSearchHoldsSnapshotAliveWhileRefreshDeletesFiles() throws IOException {
        MockDataFormat format = new MockDataFormat();
        MockIndexingExecutionEngine indexEngine = new MockIndexingExecutionEngine(format);

        // Batch 1
        Writer<MockDocumentInput> w1 = indexEngine.createWriter(1L);
        MockDocumentInput d1 = indexEngine.newDocumentInput();
        d1.addField(mock(MappedFieldType.class), "Alice");
        d1.setRowId("_row_id", 0);
        w1.addDoc(d1);
        WriterFileSet fs1 = w1.flush().getWriterFileSet(format).get();
        w1.close();

        RefreshResult rr1 = indexEngine.refresh(RefreshInput.builder().addWriterFileSet(fs1).build());
        MockCatalogSnapshot snapshot1 = new MockCatalogSnapshot(1L, rr1.refreshedSegments(), format);

        MockReaderManager readerManager = new MockReaderManager(format.name());
        readerManager.afterRefresh(true, snapshot1);

        DataFormatAwareEngine dataFormatAwareEngine = new DataFormatAwareEngine(Map.of(format, readerManager));
        dataFormatAwareEngine.setLatestSnapshot(snapshot1); // takes over construction ref, refcount: 1

        // Search acquires reader — refcount: 2
        DataFormatAwareEngine.DataFormatAwareReader dataFormatAwareReader = dataFormatAwareEngine.acquireReader();
        MockReader searchReader = (MockReader) dataFormatAwareReader.getReader(format);
        assertEquals(1, searchReader.totalRows);

        // New refresh arrives — setLatestSnapshot(s2) decRefs s1 → refcount: 1
        Writer<MockDocumentInput> w2 = indexEngine.createWriter(2L);
        MockDocumentInput d2 = indexEngine.newDocumentInput();
        d2.addField(mock(MappedFieldType.class), "Bob");
        d2.setRowId("_row_id", 1);
        w2.addDoc(d2);
        WriterFileSet fs2 = w2.flush().getWriterFileSet(format).get();
        w2.close();

        RefreshResult rr2 = indexEngine.refresh(RefreshInput.builder().addWriterFileSet(fs1).addWriterFileSet(fs2).build());
        MockCatalogSnapshot snapshot2 = new MockCatalogSnapshot(2L, rr2.refreshedSegments(), format);
        readerManager.afterRefresh(true, snapshot2);
        dataFormatAwareEngine.setLatestSnapshot(snapshot2); // s1 refcount: 1 (only search ref)

        // Old snapshot deleted from reader manager — reader closes
        readerManager.onDeleted(snapshot1);
        assertTrue("Reader for snapshot1 closed in reader manager", searchReader.closed);

        // But snapshot1 still alive — search holds the last ref
        assertTrue("Snapshot1 alive while search holds ref", snapshot1.tryIncRef());
        snapshot1.decRef(); // undo probe

        // Search completes — s1 refcount: 0 → dead
        dataFormatAwareReader.close();
        assertFalse("Snapshot1 dead after search releases", snapshot1.tryIncRef());

        // Snapshot 2 still works
        try (DataFormatAwareEngine.DataFormatAwareReader cr2 = dataFormatAwareEngine.acquireReader()) {
            MockReader r2 = (MockReader) cr2.getReader(format);
            assertEquals(2, r2.totalRows);
        }
    }

    /**
     * CompositeReader provides per-format reader access from a single catalog snapshot.
     */
    public void testCompositeReaderMultiFormat() throws IOException {
        MockDataFormat format1 = new MockDataFormat();
        DataFormat format2 = new DataFormat() {
            @Override
            public String name() {
                return "mock-lucene";
            }

            @Override
            public long priority() {
                return 50L;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };

        MockReaderManager rm1 = new MockReaderManager(format1.name());
        MockReaderManager rm2 = new MockReaderManager(format2.name());

        Path dir = createTempDir();
        WriterFileSet wfs1 = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("data.parquet").addNumRows(10).build();
        WriterFileSet wfs2 = WriterFileSet.builder().directory(dir).writerGeneration(1L).addFile("data.lucene").addNumRows(10).build();
        Segment seg = Segment.builder(0L).addSearchableFiles(format1, wfs1).addSearchableFiles(format2, wfs2).build();
        MockCatalogSnapshot snapshot = new MockCatalogSnapshot(1L, List.of(seg), format1) {
            @Override
            public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
                if ("mock-lucene".equals(dataFormat)) return List.of(wfs2);
                return super.getSearchableFiles(dataFormat);
            }

            @Override
            public Set<String> getDataFormats() {
                return Set.of(format1.name(), format2.name());
            }
        };

        rm1.afterRefresh(true, snapshot);
        rm2.afterRefresh(true, snapshot);

        DataFormatAwareEngine dataFormatAwareEngine = new DataFormatAwareEngine(Map.of(format1, rm1, format2, rm2));
        dataFormatAwareEngine.setLatestSnapshot(snapshot);

        try (DataFormatAwareEngine.DataFormatAwareReader cr = dataFormatAwareEngine.acquireReader()) {
            MockReader r1 = (MockReader) cr.getReader(format1);
            MockReader r2 = (MockReader) cr.getReader(format2);
            assertNotNull(r1);
            assertNotNull(r2);
            assertEquals(10, r1.totalRows);
            assertEquals(10, r2.totalRows);
            assertTrue(r1.fileNames.contains("data.parquet"));
            assertTrue(r2.fileNames.contains("data.lucene"));
        }
    }

    /**
     * afterRefresh(false) is a no-op; duplicate afterRefresh for same snapshot reuses reader.
     */
    public void testRefreshEdgeCases() throws IOException {
        MockDataFormat format = new MockDataFormat();
        MockIndexingExecutionEngine indexEngine = new MockIndexingExecutionEngine(format);

        Writer<MockDocumentInput> w = indexEngine.createWriter(1L);
        MockDocumentInput d = indexEngine.newDocumentInput();
        d.addField(mock(MappedFieldType.class), "x");
        d.setRowId("_row_id", 0);
        w.addDoc(d);
        WriterFileSet fs = w.flush().getWriterFileSet(format).get();
        w.close();

        RefreshResult rr = indexEngine.refresh(RefreshInput.builder().addWriterFileSet(fs).build());
        MockCatalogSnapshot snapshot = new MockCatalogSnapshot(1L, rr.refreshedSegments(), format);

        MockReaderManager rm = new MockReaderManager(format.name());

        rm.afterRefresh(false, snapshot);
        assertNull(rm.getReader(snapshot));
        assertEquals(0, rm.readerCount());

        rm.afterRefresh(true, snapshot);
        assertNotNull(rm.getReader(snapshot));
        assertEquals(1, rm.readerCount());

        MockReader first = rm.getReader(snapshot);
        rm.afterRefresh(true, snapshot);
        assertSame(first, rm.getReader(snapshot));
        assertEquals(1, rm.readerCount());
    }

    /**
     * File add/delete notifications propagate through reader manager.
     */
    public void testFileLifecycleNotifications() throws IOException {
        MockReaderManager rm = new MockReaderManager("mock-columnar");

        rm.onFilesAdded(List.of("a.parquet", "b.parquet"));
        assertEquals(2, rm.addedFiles.size());
        assertTrue(rm.addedFiles.contains("a.parquet"));

        rm.onFilesDeleted(List.of("a.parquet"));
        assertEquals(1, rm.deletedFiles.size());
        assertTrue(rm.deletedFiles.contains("a.parquet"));
    }

    static class MockReader {
        final List<String> fileNames;
        final long totalRows;
        boolean closed;

        MockReader(List<String> fileNames, long totalRows) {
            this.fileNames = fileNames;
            this.totalRows = totalRows;
        }

        void close() {
            closed = true;
        }
    }

    static class MockReaderManager implements EngineReaderManager<MockReader> {
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
                allFiles.addAll(wfs.files());
                totalRows += wfs.numRows();
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

    static class MockCatalogSnapshot extends CatalogSnapshot {
        private final List<Segment> segments;
        private final MockDataFormat format;

        MockCatalogSnapshot(long generation, List<Segment> segments, MockDataFormat format) {
            super("mock-snapshot", generation, 1L);
            this.segments = segments;
            this.format = format;
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
        public List<Segment> getSegments() {
            return segments;
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
        public void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> map) {}

        @Override
        public void setUserData(Map<String, String> userData, boolean b) {}

        @Override
        public Object getReader(DataFormat dataFormat) {
            return null;
        }

        @Override
        protected void closeInternal() {}
    }
}
