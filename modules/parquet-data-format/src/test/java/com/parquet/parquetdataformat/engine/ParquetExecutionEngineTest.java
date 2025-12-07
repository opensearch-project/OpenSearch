/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine;

import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Unit tests for ParquetExecutionEngine covering engine lifecycle, writer creation,
 * file loading and tracking, refresh operations, and resource management.
 */
public class ParquetExecutionEngineTest extends OpenSearchTestCase {

    private Path tempDir;
    private ShardPath shardPath;
    private Schema testSchema;
    private Settings testSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        // Create temporary directory for test files
        tempDir = Files.createTempDirectory("parquet-engine-test");
        
        // Create test schema
        List<Field> fields = Arrays.asList(
            new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("value", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
        );
        testSchema = new Schema(fields);
        
        // Create test settings
        testSettings = Settings.builder().build();
        
        // Create ShardPath
        Index index = new Index("test-index", "test-uuid");
        ShardId shardId = new ShardId(index, 0);
        shardPath = new ShardPath(false, tempDir, tempDir, shardId);
    }

    @Override
    public void tearDown() throws Exception {
        // Clean up temp directory
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // Ignore cleanup errors
                    }
                });
        }
        super.tearDown();
    }

    // ===== Basic Engine Functionality Tests =====

    public void testEngineCreationAndInitialization() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        assertNotNull("Engine should be created", engine);
        
        engine.close();
    }

    public void testGetDataFormat() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        DataFormat dataFormat = engine.getDataFormat();
        assertNotNull("Data format should not be null", dataFormat);
        assertTrue("Data format should be ParquetDataFormat", dataFormat instanceof ParquetDataFormat);
        
        engine.close();
    }

    public void testGetMerger() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        Merger merger = engine.getMerger();
        assertNotNull("Merger should not be null", merger);
        
        engine.close();
    }

    public void testSupportedFieldTypes() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        List<String> supportedTypes = engine.supportedFieldTypes();
        assertNotNull("Supported field types should not be null", supportedTypes);
        assertTrue("Supported field types should be a list", supportedTypes instanceof List);
        
        engine.close();
    }

    // ===== Writer Creation Tests =====

    public void testCreateWriter() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        long generation = 1L;
        Writer<ParquetDocumentInput> writer = engine.createWriter(generation);
        
        assertNotNull("Writer should be created", writer);
        
        writer.close();
        engine.close();
    }

    public void testCreateWriterWithDifferentGenerations() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        long[] generations = {1L, 10L, 100L, 1000L};
        
        for (long gen : generations) {
            Writer<ParquetDocumentInput> writer = engine.createWriter(gen);
            assertNotNull("Writer should be created for generation " + gen, writer);
            writer.close();
        }
        
        engine.close();
    }

    public void testWriterGenerationZero() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        Writer<ParquetDocumentInput> writer = engine.createWriter(0L);
        assertNotNull("Writer should be created with generation 0", writer);
        
        writer.close();
        engine.close();
    }

    public void testWriterGenerationMaxValue() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        Writer<ParquetDocumentInput> writer = engine.createWriter(Long.MAX_VALUE);
        assertNotNull("Writer should be created with max generation", writer);
        
        writer.close();
        engine.close();
    }

    // ===== File Loading Tests =====

    public void testLoadWriterFilesWithEmptyDirectory() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        engine.loadWriterFiles(null);
        // Should complete without error (no-op implementation)
        
        engine.close();
    }

    // ===== Refresh Operation Tests =====

    public void testRefreshWithEmptyInput() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        RefreshInput refreshInput = new RefreshInput();
        RefreshResult result = engine.refresh(refreshInput);
        
        assertNotNull("Refresh result should not be null", result);
        
        engine.close();
    }

    public void testRefreshWithWriterFiles() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(tempDir)
            .writerGeneration(1L)
            .addFile("_parquet_file_generation_1.parquet")
            .build();
        
        RefreshInput refreshInput = new RefreshInput();
        refreshInput.add(fileSet);
        
        RefreshResult result = engine.refresh(refreshInput);
        assertNotNull("Refresh result should not be null", result);
        
        engine.close();
    }

    public void testRefreshMultipleTimes() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        RefreshInput refreshInput = new RefreshInput();
        
        RefreshResult result1 = engine.refresh(refreshInput);
        RefreshResult result2 = engine.refresh(refreshInput);
        RefreshResult result3 = engine.refresh(refreshInput);
        
        assertNotNull("First refresh result should not be null", result1);
        assertNotNull("Second refresh result should not be null", result2);
        assertNotNull("Third refresh result should not be null", result3);
        
        engine.close();
    }

    // ===== Resource Management Tests =====

    public void testEngineClose() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        engine.close();
        // Verify close can be called multiple times without error
        engine.close();
    }

    public void testEngineCloseAfterOperations() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        // Perform various operations
        Writer<ParquetDocumentInput> writer = engine.createWriter(1L);
        writer.close();
        
        engine.refresh(new RefreshInput());
        engine.getNativeBytesUsed();
        
        // Close should clean up all resources
        engine.close();
    }

    public void testGetNativeBytesUsed() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        long bytesUsed = engine.getNativeBytesUsed();
        assertTrue("Native bytes used should be non-negative", bytesUsed >= 0);
        
        engine.close();
    }

    public void testGetNativeBytesUsedMultipleTimes() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        long bytes1 = engine.getNativeBytesUsed();
        long bytes2 = engine.getNativeBytesUsed();
        long bytes3 = engine.getNativeBytesUsed();
        
        assertTrue("First call should return non-negative value", bytes1 >= 0);
        assertTrue("Second call should return non-negative value", bytes2 >= 0);
        assertTrue("Third call should return non-negative value", bytes3 >= 0);
        
        engine.close();
    }

    // ===== Schema Handling Tests =====

    public void testSchemaSupplierIsCalled() throws IOException {
        final boolean[] schemaCalled = {false};
        
        Supplier<Schema> trackingSupplier = () -> {
            schemaCalled[0] = true;
            return testSchema;
        };
        
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, trackingSupplier, shardPath);
        
        Writer<ParquetDocumentInput> writer = engine.createWriter(1L);
        assertTrue("Schema supplier should be called", schemaCalled[0]);
        
        writer.close();
        engine.close();
    }

    public void testEngineWithComplexSchema() throws IOException {
        List<Field> complexFields = Arrays.asList(
            new Field("int_field", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("long_field", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("float_field", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
            new Field("double_field", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("string_field", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("bool_field", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("binary_field", FieldType.nullable(new ArrowType.Binary()), null)
        );
        Schema complexSchema = new Schema(complexFields);
        
        Supplier<Schema> schemaSupplier = () -> complexSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        Writer<ParquetDocumentInput> writer = engine.createWriter(1L);
        assertNotNull("Writer should be created with complex schema", writer);
        
        writer.close();
        engine.close();
    }

    public void testEngineWithEmptySchema() throws IOException {
        Schema emptySchema = new Schema(Arrays.asList());
        Supplier<Schema> schemaSupplier = () -> emptySchema;
        
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        Writer<ParquetDocumentInput> writer = engine.createWriter(1L);
        assertNotNull("Writer should be created with empty schema", writer);
        
        writer.close();
        engine.close();
    }

    // ===== Settings Tests =====

    public void testEngineWithDifferentSettings() throws IOException {
        Settings customSettings = Settings.builder()
            .put("test.setting", "value")
            .put("another.setting", 42)
            .build();
        
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(customSettings, schemaSupplier, shardPath);
        
        assertNotNull("Engine should be created with custom settings", engine);
        
        engine.close();
    }

    public void testEngineWithEmptySettings() throws IOException {
        Settings emptySettings = Settings.EMPTY;
        Supplier<Schema> schemaSupplier = () -> testSchema;
        
        ParquetExecutionEngine engine = new ParquetExecutionEngine(emptySettings, schemaSupplier, shardPath);
        
        assertNotNull("Engine should be created with empty settings", engine);
        
        engine.close();
    }

    // ===== Error Handling Tests =====

    public void testConstructorWithNullSettings() {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new ParquetExecutionEngine(null, schemaSupplier, shardPath);
        });
        assertNotNull("Should throw NullPointerException for null settings", exception);
    }

    public void testConstructorWithNullSchema() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new ParquetExecutionEngine(testSettings, null, shardPath);
        });
        assertNotNull("Should throw NullPointerException for null schema", exception);
    }

    public void testConstructorWithNullShardPath() {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new ParquetExecutionEngine(testSettings, schemaSupplier, null);
        });
        assertNotNull("Should throw NullPointerException for null shard path", exception);
    }

    // ===== Integration Pattern Tests =====

    public void testEngineLifecyclePattern() throws IOException {
        Supplier<Schema> schemaSupplier = () -> testSchema;
        ParquetExecutionEngine engine = new ParquetExecutionEngine(testSettings, schemaSupplier, shardPath);
        
        
        // Create writers
        Writer<ParquetDocumentInput> writer1 = engine.createWriter(1L);
        Writer<ParquetDocumentInput> writer2 = engine.createWriter(2L);
        
        // Refresh with file sets
        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(tempDir)
            .writerGeneration(1L)
            .addFile("file.parquet")
            .build();
        
        RefreshInput refreshInput = new RefreshInput();
        refreshInput.add(fileSet);
        RefreshResult result = engine.refresh(refreshInput);
        assertNotNull("Refresh should return result", result);
        
        // Check memory usage
        long bytesUsed = engine.getNativeBytesUsed();
        assertTrue("Should track memory usage", bytesUsed >= 0);
        
         // Close writers
        writer1.close();
        writer2.close();

        // Close engine
        engine.close();
    }
}
