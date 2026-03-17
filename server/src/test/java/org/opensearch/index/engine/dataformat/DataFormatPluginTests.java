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
        IndexingExecutionEngine<DataFormat, MockDocumentInput> engine = plugin.indexingEngine(
            mock(MapperService.class),
            new ShardPath(false, Path.of("/tmp/uuid/0"), Path.of("/tmp/uuid/0"), new ShardId("index", "uuid", 0)),
            new IndexSettings(IndexMetadata.builder("index").settings(settings).build(), settings)
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
        assertFalse(writerFileSet.get().getFiles().isEmpty());
        assertEquals(2, writerFileSet.get().getNumRows());
        assertEquals(1L, writerFileSet.get().getWriterGeneration());

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
        assertEquals(3L, merged.getWriterGeneration());
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
        assertNotNull(segment.getDFGroupedSearchableFiles().get(format.name()));

        // 8. Delete files
        engine.deleteFiles(Map.of(merged.getDirectory(), merged.getFiles()));
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
        assertEquals("integer", cap.getFieldType());
        assertTrue(cap.getCapabilities().contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        assertTrue(cap.getCapabilities().contains(FieldTypeCapabilities.Capability.STORED_FIELDS));
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
        assertEquals(10, infos.getWriterFileSet(format).get().getNumRows());

        FileInfos empty = FileInfos.empty();
        assertTrue(empty.getWriterFilesMap().isEmpty());
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
        assertTrue(empty.getWriterFiles().isEmpty());
        assertTrue(empty.getExistingSegments().isEmpty());

        Path dir = createTempDir();
        WriterFileSet fs1 = new WriterFileSet(dir, 1L, 10);
        WriterFileSet fs2 = new WriterFileSet(dir, 2L, 20);
        Segment seg = new Segment(0L);

        RefreshInput input = RefreshInput.builder().addWriterFileSet(fs1).addWriterFileSet(fs2).existingSegments(List.of(seg)).build();
        assertEquals(2, input.getWriterFiles().size());
        assertEquals(1, input.getExistingSegments().size());
    }

    static class MockDataFormat implements DataFormat {
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
            List<WriterFileSet> fileMetadataList = mergeInput.getFileMetadataList();
            long newWriterGeneration = mergeInput.getNewWriterGeneration();
            RowIdMapping existingMapping = mergeInput.getRowIdMapping();

            String prefix = existingMapping != null ? "secondary_merged_gen" : "merged_gen";
            WriterFileSet merged = WriterFileSet.builder()
                .directory(directory)
                .writerGeneration(newWriterGeneration)
                .addFile(prefix + newWriterGeneration + ".parquet")
                .addNumRows(fileMetadataList.stream().mapToLong(WriterFileSet::getNumRows).sum())
                .build();

            if (existingMapping != null) {
                return new MergeResult(Map.of(dataFormat, merged), existingMapping);
            }

            // Build a simple sequential row ID mapping
            Map<Long, Long> genOffsets = new HashMap<>();
            long offset = 0;
            for (WriterFileSet fs : fileMetadataList) {
                genOffsets.put(fs.getWriterGeneration(), offset);
                offset += fs.getNumRows();
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
            for (WriterFileSet wfs : refreshInput.getWriterFiles()) {
                Segment segment = new Segment(gen++);
                segment.addSearchableFiles(dataFormat.name(), wfs);
                segments.add(segment);
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
        @SuppressWarnings("unchecked")
        public <T extends DataFormat, P extends DocumentInput<?>> IndexingExecutionEngine<T, P> indexingEngine(
            MapperService mapperService,
            ShardPath shardPath,
            IndexSettings indexSettings
        ) {
            return (IndexingExecutionEngine<T, P>) new MockIndexingExecutionEngine(dataFormat);
        }
    }
}
