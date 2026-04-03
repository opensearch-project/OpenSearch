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
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.stub.MockCatalogSnapshot;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.index.engine.dataformat.stub.MockIndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.stub.MockReader;
import org.opensearch.index.engine.dataformat.stub.MockReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
                null,
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
        DataFormat format = new MockDataFormat();
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

    /**
     * Search holds snapshot alive while refresh replaces it.
     * CatalogSnapshotManager handles ref counting: acquireReader increments,
     * commitNewSnapshot replaces the latest, and closing the reader releases the old snapshot.
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

        CatalogSnapshotManager manager = new CatalogSnapshotManager(1L, 1L, 0L, rr1.refreshedSegments(), 1L, Map.of());

        MockReaderManager readerManager = new MockReaderManager(format.name());
        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            readerManager.afterRefresh(true, ref.get());
        }

        DataFormatAwareEngine dataFormatAwareEngine = new DataFormatAwareEngine(Map.of(format, readerManager), manager);

        // Search acquires reader on snapshot1 — holds a ref
        var dataFormatAwareReader = dataFormatAwareEngine.acquireReader();
        CatalogSnapshot snapshot1 = dataFormatAwareReader.get().catalogSnapshot();
        MockReader searchReader = (MockReader) dataFormatAwareReader.get().reader(format);
        assertEquals(1, searchReader.totalRows);
        assertEquals(1L, snapshot1.getGeneration());

        // New refresh arrives — commit replaces snapshot
        Writer<MockDocumentInput> w2 = indexEngine.createWriter(2L);
        MockDocumentInput d2 = indexEngine.newDocumentInput();
        d2.addField(mock(MappedFieldType.class), "Bob");
        d2.setRowId("_row_id", 1);
        w2.addDoc(d2);
        WriterFileSet fs2 = w2.flush().getWriterFileSet(format).get();
        w2.close();

        RefreshResult rr2 = indexEngine.refresh(RefreshInput.builder().addWriterFileSet(fs1).addWriterFileSet(fs2).build());
        manager.commitNewSnapshot(rr2.refreshedSegments());

        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            readerManager.afterRefresh(true, ref.get());
        }

        // Snapshot1 still alive — search reader still works because the ref is held
        assertFalse("Snapshot1 should still be alive while search holds ref", ((DataformatAwareCatalogSnapshot) snapshot1).isClosed());
        assertEquals(1, searchReader.totalRows);
        assertSame(snapshot1, dataFormatAwareReader.get().catalogSnapshot());

        // New acquireSnapshot returns snapshot2, not snapshot1
        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            assertEquals(2L, ref.get().getGeneration());
            assertNotSame(snapshot1, ref.get());
        }

        // Search completes — releases the old snapshot ref
        dataFormatAwareReader.close();

        // Snapshot1 is now dead
        assertTrue(
            "Snapshot1 should be closed after search releases the last ref",
            ((DataformatAwareCatalogSnapshot) snapshot1).isClosed()
        );

        // Snapshot1 is now dead — tryIncRef would fail (verified via new acquire returning snapshot2)
        // Snapshot 2 works
        try (var cr2 = dataFormatAwareEngine.acquireReader()) {
            MockReader r2 = (MockReader) cr2.get().reader(format);
            assertEquals(2, r2.totalRows);
            assertEquals(2L, cr2.get().catalogSnapshot().getGeneration());
        }

        manager.close();
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

        CatalogSnapshotManager manager = new CatalogSnapshotManager(1L, 1L, 0L, List.of(seg), 1L, Map.of());

        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            rm1.afterRefresh(true, ref.get());
            rm2.afterRefresh(true, ref.get());
        }

        DataFormatAwareEngine dataFormatAwareEngine = new DataFormatAwareEngine(Map.of(format1, rm1, format2, rm2), manager);

        try (var cr = dataFormatAwareEngine.acquireReader()) {
            MockReader r1 = (MockReader) cr.get().reader(format1);
            MockReader r2 = (MockReader) cr.get().reader(format2);
            assertNotNull(r1);
            assertNotNull(r2);
            assertEquals(10, r1.totalRows);
            assertEquals(10, r2.totalRows);
            assertTrue(r1.fileNames.contains("data.parquet"));
            assertTrue(r2.fileNames.contains("data.lucene"));
        }

        manager.close();
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
}
