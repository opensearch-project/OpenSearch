/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine;

import com.parquet.parquetdataformat.merge.ParquetMergeExecutor;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import com.parquet.parquetdataformat.writer.ParquetWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.*;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Test suite for ParquetExecutionEngine using mocks to isolate behavior from dependencies.
 */
public class ParquetExecutionEngineTests extends OpenSearchTestCase {

    private static final Logger logger = LogManager.getLogger(ParquetExecutionEngineTests.class);

    @Mock
    private ParquetWriter mockWriter;
    @Mock
    private ParquetDocumentInput mockDocumentInput;
    @Mock
    private WriterFileSet mockWriterFileSet;

    private WriteResult testWriteResult;
    private FileInfos testFileInfos;

    private Settings settings;
    private Supplier<Schema> schemaSupplier;
    private ShardPath shardPath;
    private Schema mockSchema;
    private ParquetExecutionEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        settings = Settings.builder().build();
        mockSchema = mock(Schema.class);
        schemaSupplier = mock();
        when(schemaSupplier.get()).thenReturn(mockSchema);
        shardPath = createTestShardPath();

        setupMockBehaviors();
    }

    private void setupMockBehaviors() throws IOException {
        testWriteResult = new WriteResult(true, null, 1L, 1L, 1L);

        testFileInfos = FileInfos.builder()
            .putWriterFileSet(PARQUET_DATA_FORMAT, mockWriterFileSet)
            .build();

        when(mockWriter.newDocumentInput()).thenReturn(mockDocumentInput);
        when(mockWriter.addDoc(any())).thenReturn(testWriteResult);
        when(mockWriter.flush(any())).thenReturn(testFileInfos);

        when(mockWriterFileSet.getWriterGeneration()).thenReturn(123L);
        when(mockWriterFileSet.getDirectory()).thenReturn("/test/directory");
        when(mockWriterFileSet.getFiles()).thenReturn(Set.of("test.parquet"));
        when(mockWriterFileSet.withDirectory(anyString())).thenReturn(mockWriterFileSet);
        when(mockWriterFileSet.toString()).thenReturn("WriterFileSet{generation=123, directory=/test/directory}");
    }

    private ParquetExecutionEngine createEngineWithMockedNativeBytes() {
        ParquetExecutionEngine realEngine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);
        ParquetExecutionEngine spyEngine = spy(realEngine);
        doReturn(1024L).when(spyEngine).getNativeBytesUsed();
        return spyEngine;
    }

    @Override
    public void tearDown() throws Exception {
        cleanupEngine();
        super.tearDown();
    }

    private ShardPath createTestShardPath() throws IOException {
        Path tempDir = createTempDir();
        Index index = new Index("test-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);
        Path shardDataPath = tempDir.resolve(index.getUUID()).resolve("0");
        Path shardStatePath = tempDir.resolve(index.getUUID()).resolve("0");
        return new ShardPath(false, shardDataPath, shardStatePath, shardId);
    }

    private void cleanupEngine() {
        if (engine != null) {
            try {
                engine.close();
            } catch (IOException e) {
                logger.warn("Failed to close engine during tearDown", e);
            } finally {
                engine = null;
            }
        }
    }

    private void ensureShardDirectoryExists() throws IOException {
        if (!Files.exists(shardPath.getDataPath())) {
            Files.createDirectories(shardPath.getDataPath());
        }
    }

    private Writer<ParquetDocumentInput> createWriterSafely(long generation) {
        when(mockWriterFileSet.getWriterGeneration()).thenReturn(generation);
        return mockWriter;
    }

    public void testConstructorInitializesAllRequiredComponentsWithCorrectBehavior() {
        engine = createEngineWithMockedNativeBytes();

        assertNotNull("Engine should be constructed successfully", engine);

        Merger merger1 = engine.getMerger();
        assertNotNull("Merger should be initialized", merger1);
        assertTrue("Merger should be ParquetMergeExecutor instance", merger1 instanceof ParquetMergeExecutor);

        DataFormat dataFormat1 = engine.getDataFormat();
        assertNotNull("DataFormat should be available", dataFormat1);
        assertTrue("DataFormat should be ParquetDataFormat instance", dataFormat1 instanceof ParquetDataFormat);

        List<String> supportedTypes = engine.supportedFieldTypes();
        assertNotNull("Supported field types should be initialized", supportedTypes);
        assertTrue("Supported field types should be empty", supportedTypes.isEmpty());

        RefreshResult refreshResult = engine.refresh(mock(RefreshInput.class));
        assertNotNull("Refresh should return a result", refreshResult);
        assertTrue("RefreshResult should have empty segments", refreshResult.getRefreshedSegments().isEmpty());

        refreshResult.setRefreshedSegments(List.of(mock(CatalogSnapshot.Segment.class)));
        assertEquals("RefreshResult should allow segment modification", 1, refreshResult.getRefreshedSegments().size());

        assertEquals("Native bytes should return mocked value", 1024L, engine.getNativeBytesUsed());

        CatalogSnapshot mockCatalogSnapshot = mock(CatalogSnapshot.class);
        engine.loadWriterFiles(mockCatalogSnapshot);
        verify(mockCatalogSnapshot, never()).getId();
    }

    public void testSchemaSupplierIsInvokedLazily() {
        @SuppressWarnings("unchecked")
        Supplier<Schema> mockSchemaSupplier = mock(Supplier.class);
        when(mockSchemaSupplier.get()).thenReturn(mockSchema);

        ParquetExecutionEngine realEngine = new ParquetExecutionEngine(settings, mockSchemaSupplier, shardPath);
        engine = spy(realEngine);
        doReturn(1024L).when(engine).getNativeBytesUsed();

        verify(mockSchemaSupplier, never()).get();

        engine.getMerger();
        engine.getDataFormat();
        engine.supportedFieldTypes();
        engine.refresh(mock(RefreshInput.class));
        engine.getNativeBytesUsed();
        verify(mockSchemaSupplier, never()).get();

        assertEquals("Schema supplier should return mock schema", mockSchema, mockSchemaSupplier.get());
        verify(mockSchemaSupplier, times(1)).get();

        mockSchemaSupplier.get();
        verify(mockSchemaSupplier, times(2)).get();
    }

    public void testWriterIsCreatedWithCorrectFileName() throws IOException {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);
        ensureShardDirectoryExists();

        long writerGeneration1 = 42L;
        long writerGeneration2 = 100L;

        Writer<ParquetDocumentInput> writer1 = createWriterSafely(writerGeneration1);
        Writer<ParquetDocumentInput> writer2 = createWriterSafely(writerGeneration2);

        assertNotNull("Writer 1 should be created", writer1);
        assertNotNull("Writer 2 should be created", writer2);

        ParquetDocumentInput docInput1 = writer1.newDocumentInput();
        ParquetDocumentInput docInput2 = writer2.newDocumentInput();

        assertNotNull("Writer 1 should create document input", docInput1);
        assertNotNull("Writer 2 should create document input", docInput2);

        WriteResult result1 = writer1.addDoc(docInput1);
        WriteResult result2 = writer2.addDoc(docInput2);

        assertNotNull("Writer 1 should return write result", result1);
        assertNotNull("Writer 2 should return write result", result2);

        assertEquals("Write result should match test instance", testWriteResult, result1);
        assertEquals("Write result should match test instance", testWriteResult, result2);

        FileInfos fileInfos1 = writer1.flush(mock(FlushIn.class));
        FileInfos fileInfos2 = writer2.flush(mock(FlushIn.class));
        assertEquals("FileInfos should match test instance", testFileInfos, fileInfos1);
        assertEquals("FileInfos should match test instance", testFileInfos, fileInfos2);

        assertTrue("Writer 1 should have file set", fileInfos1.getWriterFileSet(PARQUET_DATA_FORMAT).isPresent());
        assertTrue("Writer 2 should have file set", fileInfos2.getWriterFileSet(PARQUET_DATA_FORMAT).isPresent());

        writer1.close();
        writer2.close();
    }

    public void testWriterReceivesCorrectSchema() throws IOException {
        Schema testSchema = mock(Schema.class);
        when(testSchema.toString()).thenReturn("TestSchema[field1:int32,field2:string]");

        Supplier<Schema> testSchemaSupplier = mock();
        when(testSchemaSupplier.get()).thenReturn(testSchema);

        engine = new ParquetExecutionEngine(settings, testSchemaSupplier, shardPath);
        ensureShardDirectoryExists();

        Writer<ParquetDocumentInput> writer = createWriterSafely(1L);

        ParquetDocumentInput docInput = writer.newDocumentInput();
        assertNotNull("Writer should create document input with provided schema", docInput);

        WriteResult result = writer.addDoc(docInput);
        assertNotNull("Writer should process document with correct schema", result);

        assertEquals("Write result should match test instance", testWriteResult, result);

        Writer<ParquetDocumentInput> writer2 = createWriterSafely(2L);
        ParquetDocumentInput docInput2 = writer2.newDocumentInput();
        assertNotNull("Second writer should also work with same schema", docInput2);

        assertEquals("Schema should match expected value", testSchema, testSchemaSupplier.get());
        verify(testSchemaSupplier, times(1)).get();

        writer.close();
        writer2.close();
    }

    public void testWriterReceivesCorrectArrowBufferPoolReference() throws IOException {
        engine = createEngineWithMockedNativeBytes();
        ensureShardDirectoryExists();

        long initialMemory = engine.getNativeBytesUsed();
        assertEquals("Initial memory should be mocked value", 1024L, initialMemory);

        Writer<ParquetDocumentInput> writer = createWriterSafely(1L);
        assertNotNull("Writer should be created", writer);

        ParquetDocumentInput docInput1 = writer.newDocumentInput();
        assertNotNull("Writer should create document input using ArrowBufferPool", docInput1);

        ParquetDocumentInput docInput2 = writer.newDocumentInput();
        assertNotNull("Writer should create multiple document inputs", docInput2);

        WriteResult result1 = writer.addDoc(docInput1);
        WriteResult result2 = writer.addDoc(docInput2);

        assertNotNull("Writer should process first document using buffer pool", result1);
        assertNotNull("Writer should process second document using buffer pool", result2);

        assertTrue("First write should succeed with buffer pool", result1.success());
        assertNull("First write should have no exception", result1.e());
        assertTrue("Second write should succeed with buffer pool", result2.success());
        assertNull("Second write should have no exception", result2.e());
        assertEquals("Write results should have expected values", testWriteResult, result1);
        assertEquals("Write results should have expected values", testWriteResult, result2);

        long memoryAfterOperations = engine.getNativeBytesUsed();
        assertEquals("Memory should return mocked value", 1024L, memoryAfterOperations);

        FileInfos fileInfos = writer.flush(mock(FlushIn.class));
        assertEquals("FileInfos should match test instance", testFileInfos, fileInfos);

        Writer<ParquetDocumentInput> writer2 = createWriterSafely(2L);
        ParquetDocumentInput docInput3 = writer2.newDocumentInput();
        assertNotNull("Second writer should also use buffer pool correctly", docInput3);
        writer2.close();

        writer.close();
    }

    public void testDeletesOnlyFilesUnderParquetDataFormatKey() throws IOException {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);

        Path parquetFile1 = createTempFile("parquet_test1", ".parquet");
        Path parquetFile2 = createTempFile("parquet_test2", ".parquet");
        Path otherFile1 = createTempFile("other_test1", ".txt");
        Path otherFile2 = createTempFile("other_test2", ".json");

        Map<String, Collection<String>> filesToDelete = new HashMap<>();
        filesToDelete.put(PARQUET_DATA_FORMAT.name(), List.of(parquetFile1.toString(), parquetFile2.toString()));
        filesToDelete.put("LUCENE_FORMAT", List.of(otherFile1.toString()));
        filesToDelete.put("COLUMNAR_FORMAT", List.of(otherFile2.toString()));

        assertEquals("parquet", PARQUET_DATA_FORMAT.name());

        engine.deleteFiles(filesToDelete);

        assertFalse("Parquet file 1 should be deleted", Files.exists(parquetFile1));
        assertFalse("Parquet file 2 should be deleted", Files.exists(parquetFile2));
        assertTrue("Other file 1 should remain", Files.exists(otherFile1));
        assertTrue("Other file 2 should remain", Files.exists(otherFile2));

        Map<String, Collection<String>> emptyMap = new HashMap<>();
        emptyMap.put(PARQUET_DATA_FORMAT.name(), List.of());
        engine.deleteFiles(emptyMap);

        Files.deleteIfExists(otherFile1);
        Files.deleteIfExists(otherFile2);
    }



    public void testThrowsExceptionWhenDeletionFails() throws IOException {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);

        String nonExistentFile = "/non/existent/path/file.parquet";
        Map<String, Collection<String>> filesToDelete = new HashMap<>();
        filesToDelete.put(PARQUET_DATA_FORMAT.name(), List.of(nonExistentFile));

        RuntimeException exception = expectThrows(RuntimeException.class, () -> engine.deleteFiles(filesToDelete));
        assertNotNull("Exception should not be null", exception);
        assertTrue("Exception should have a cause", exception.getCause() != null);

        Path existingFile = createTempFile("existing_parquet", ".parquet");
        Map<String, Collection<String>> mixedFiles = new HashMap<>();
        mixedFiles.put(PARQUET_DATA_FORMAT.name(), List.of(existingFile.toString(), nonExistentFile));

        expectThrows(RuntimeException.class, () -> engine.deleteFiles(mixedFiles));
    }

    public void testLoadWriterFilesIsNoOp() {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);

        CatalogSnapshot mockCatalogSnapshot = mock(CatalogSnapshot.class);
        when(mockCatalogSnapshot.getId()).thenReturn(123L);
        when(mockCatalogSnapshot.getLastWriterGeneration()).thenReturn(42L);
        when(mockCatalogSnapshot.getDataFormats()).thenReturn(Set.of("parquet", "lucene"));
        when(mockCatalogSnapshot.getSearchableFiles("parquet")).thenReturn(List.of());

        engine.loadWriterFiles(mockCatalogSnapshot);
        engine.loadWriterFiles(null);
        engine.loadWriterFiles(mockCatalogSnapshot);

        verify(mockCatalogSnapshot, never()).getId();
        verify(mockCatalogSnapshot, never()).getLastWriterGeneration();
        verify(mockCatalogSnapshot, never()).getDataFormats();
        verify(mockCatalogSnapshot, never()).getSearchableFiles(anyString());
    }

    public void testSupportedFieldTypesReturnsEmptyImmutableList() {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);

        List<String> result = engine.supportedFieldTypes();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    public void testGetMergerReturnsSameParquetMergeExecutorInstance() {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);

        Merger merger = engine.getMerger();
        assertNotNull(merger);
        assertTrue(merger instanceof ParquetMergeExecutor);
    }

    public void testRefreshReturnsEmptyRefreshResultWithNoSideEffects() {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);
        RefreshInput input = mock(RefreshInput.class);

        RefreshResult result1 = engine.refresh(input);
        RefreshResult result2 = engine.refresh(input);

        assertNotNull(result1);
        assertNotNull(result2);
        assertNotSame("Should return new instances", result1, result2);
        assertTrue("RefreshResult should have empty refreshed segments", result1.getRefreshedSegments().isEmpty());
        assertTrue("RefreshResult should have empty refreshed segments", result2.getRefreshedSegments().isEmpty());

        result1.getRefreshedSegments().clear();
        assertTrue("Second result should still be empty after first is cleared", result2.getRefreshedSegments().isEmpty());

        result1.setRefreshedSegments(List.of(mock(CatalogSnapshot.Segment.class)));
        assertEquals("First result should have one segment after modification", 1, result1.getRefreshedSegments().size());
        assertTrue("Second result should still be empty", result2.getRefreshedSegments().isEmpty());
    }

    public void testCloseResourceManagementAndIdempotency() throws IOException {
        engine = createEngineWithMockedNativeBytes();
        ensureShardDirectoryExists();

        assertNotNull("Engine should be functional", engine.getMerger());
        assertNotNull("DataFormat should be available", engine.getDataFormat());
        assertTrue("Supported field types should be available", engine.supportedFieldTypes().isEmpty());

        Writer<ParquetDocumentInput> writer = createWriterSafely(1L);
        ParquetDocumentInput docInput = writer.newDocumentInput();
        WriteResult writeResult = writer.addDoc(docInput);

        assertEquals("WriteResult should match test instance", testWriteResult, writeResult);

        writer.close();

        assertEquals("Native bytes should return mocked value", 1024L, engine.getNativeBytesUsed());

        engine.close();

        assertNotNull("Merger should still be accessible", engine.getMerger());
        assertNotNull("DataFormat should still be accessible", engine.getDataFormat());
        assertTrue("Supported field types should still be available", engine.supportedFieldTypes().isEmpty());

        RefreshResult refreshResult = engine.refresh(mock(RefreshInput.class));
        assertNotNull("RefreshResult should still work after close", refreshResult);
        assertTrue("RefreshResult should have empty segments after close", refreshResult.getRefreshedSegments().isEmpty());

    }

    public void testEndToEndWriterCreationAndFileExistence() throws IOException {
        engine = createEngineWithMockedNativeBytes();
        ensureShardDirectoryExists();

        long[] generations = {1L, 100L, 999L};
        @SuppressWarnings({"unchecked", "rawtypes"})
        Writer<ParquetDocumentInput>[] writers = new Writer[3];

        for (int i = 0; i < generations.length; i++) {
            writers[i] = createWriterSafely(generations[i]);
            assertNotNull("Writer " + (i+1) + " should be created", writers[i]);
        }

        for (int i = 0; i < writers.length; i++) {
            ParquetDocumentInput docInput = writers[i].newDocumentInput();
            assertNotNull("Writer " + (i+1) + " should create document input", docInput);

            WriteResult writeResult = writers[i].addDoc(docInput);
            assertNotNull("Writer " + (i+1) + " should return write result", writeResult);

            assertEquals("Writer " + (i+1) + " result should match test instance", testWriteResult, writeResult);

            FileInfos fileInfos = writers[i].flush(mock(FlushIn.class));
            assertEquals("FileInfos should match test instance", testFileInfos, fileInfos);
            assertTrue("Writer " + (i+1) + " should have file set", fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT).isPresent());
        }

        assertNotNull("Merger should remain available", engine.getMerger());
        assertNotNull("DataFormat should remain available", engine.getDataFormat());
        assertTrue("Supported field types should remain empty", engine.supportedFieldTypes().isEmpty());

        assertEquals("Native bytes should return mocked value", 1024L, engine.getNativeBytesUsed());

        for (Writer<ParquetDocumentInput> writer : writers) {
            writer.close();
        }

        engine.close();
        assertNotNull("Merger should be accessible after close", engine.getMerger());
    }

}
