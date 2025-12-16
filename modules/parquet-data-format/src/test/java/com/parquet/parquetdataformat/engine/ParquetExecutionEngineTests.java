/*
 * SPDX-License-Identifier: Apache-2.0
 */

package com.parquet.parquetdataformat.engine;

import com.parquet.parquetdataformat.merge.ParquetMergeExecutor;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import com.parquet.parquetdataformat.writer.ParquetWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.*;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.function.Supplier;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;

/**
 * Unit Tests for ParquetExecutionEngine covering all must-have scenarios.
 */

public class ParquetExecutionEngineTests extends OpenSearchTestCase {

    private Settings settings;
    private Supplier<Schema> schemaSupplier;
    private ShardPath shardPath;
    private Schema realSchema;
    private ParquetExecutionEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.builder().build();
        realSchema = createRealSchema();
        schemaSupplier = () -> realSchema;

        Path tempDir = createTempDir();
        Index index = new Index("test-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);
        Path shardDataPath = tempDir.resolve(index.getUUID()).resolve("0");
        Path shardStatePath = tempDir.resolve(index.getUUID()).resolve("0");
        shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);

        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);
    }

    @Override
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
            engine = null;
        }
        super.tearDown();
    }

    private Schema createRealSchema() {
        List<Field> fields = Arrays.asList(
            Field.nullable("id", Types.MinorType.BIGINT.getType()),
            Field.nullable("name", Types.MinorType.VARCHAR.getType()),
            Field.nullable("active", Types.MinorType.BIT.getType())
        );
        return new Schema(fields);
    }



    public void testDeleteFilesOnlyDeletesParquetFiles() throws IOException {
        Path parquetFile = createTempFile("test_parquet", ".parquet");
        Path otherFile = createTempFile("test_other", ".txt");

        Map<String, Collection<String>> filesToDelete = new HashMap<>();
        filesToDelete.put(PARQUET_DATA_FORMAT.name(), List.of(parquetFile.toString()));
        filesToDelete.put("OTHER_FORMAT", List.of(otherFile.toString()));

        engine.deleteFiles(filesToDelete);

        assertFalse(Files.exists(parquetFile));
        assertTrue(Files.exists(otherFile));

        Files.deleteIfExists(otherFile);
    }

    public void testDeleteFilesThrowsWhenFileDoesNotExist() {
        String nonExistentFile = "/non/existent/file.parquet";

        Map<String, Collection<String>> filesToDelete = Map.of(
            PARQUET_DATA_FORMAT.name(),
            List.of(nonExistentFile)
        );

        RuntimeException ex = expectThrows(RuntimeException.class, () -> engine.deleteFiles(filesToDelete));
        assertNotNull(ex.getCause());
    }

    public void testSupportedFieldTypesReturnsEmptyList() {
        List<String> supportedTypes = engine.supportedFieldTypes();
        assertNotNull(supportedTypes);
        assertTrue(supportedTypes.isEmpty());
    }

    public void testGetMergerReturnsParquetMergeExecutorInstance() {
        engine = new ParquetExecutionEngine(settings, schemaSupplier, shardPath);
        
        Merger merger = engine.getMerger();
        assertNotNull("Merger should not be null", merger);
        assertTrue("Merger should be ParquetMergeExecutor instance", merger instanceof ParquetMergeExecutor);
        
        Merger merger2 = engine.getMerger();
        assertSame("getMerger should return the same instance", merger, merger2);
        
        assertNotNull("Merger should have a valid toString", merger.toString());
    }

    public void testGetDataFormatReturnsParquetDataFormat() {
        DataFormat dataFormat = engine.getDataFormat();
        assertNotNull(dataFormat);
        assertEquals(PARQUET_DATA_FORMAT.name(), dataFormat.name());
    }

    public void testRefreshReturnsEmptyResult() {
        RefreshResult result = engine.refresh(new RefreshInput() {});
        assertNotNull(result);
        assertTrue(result.getRefreshedSegments().isEmpty());
    }

    public void testGetNativeBytesUsedReturnsNonNegative() {
        long nativeBytes = engine.getNativeBytesUsed();
        assertTrue(nativeBytes >= 0);
    }

    public void testCloseIsIdempotentAndDoesNotThrow() throws IOException {
        engine.close();
        // calling close again should not throw
        engine.close();
    }

    public void testWriterLifecycleAndBasicOperations() throws IOException {
        long generation = 10L;
        Writer<ParquetDocumentInput> writer = engine.createWriter(generation);
        assertNotNull(writer);

        ParquetDocumentInput input = writer.newDocumentInput();
        assertNotNull(input);
        assertNotNull(input.getFinalInput());

        WriteResult writeResult = writer.addDoc(input);
        assertNotNull(writeResult);
        assertTrue(writeResult.success());

        FlushIn flushIn = new FlushIn() {};
        FileInfos fileInfos = writer.flush(flushIn);
        assertNotNull(fileInfos);

        try {
            input.close();
        } catch (Exception e) {
            // Ignore close exceptions in tests
        }
        writer.close();
    }
}
