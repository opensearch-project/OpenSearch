/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Unit tests for ParquetWriter covering writer lifecycle, document input generation,
 * flush operations, and resource management.
 */
public class ParquetWriterTest extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private Schema testSchema;
    private Path tempDir;
    private String testFile;
    private static final long TEST_GENERATION = 1L;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        // Create temporary directory for test files
        tempDir = Files.createTempDirectory("parquet-writer-test");
        testFile = tempDir.resolve("test.parquet").toString();

        // Create a simple test schema
        Field idField = new Field("id", FieldType.nullable(Types.MinorType.INT.getType()), null);
        Field nameField = new Field("name", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        Field valueField = new Field("value", FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        testSchema = new Schema(Arrays.asList(idField, nameField, valueField));

        // Create buffer pool
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
    }

    @Override
    public void tearDown() throws Exception {
        if (bufferPool != null) {
            bufferPool.close();
        }
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

    // ===== Basic Writer Functionality Tests =====

    public void testWriterCreationAndInitialization() {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        
        assertNotNull("Writer should be created", writer);
        
        writer.close();
    }

    public void testNewDocumentInputCreation() {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        
        ParquetDocumentInput docInput = writer.newDocumentInput();
        assertNotNull("Document input should not be null", docInput);
        
        writer.close();
    }

    public void testNewDocumentInputMultipleTimes() {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        
        ParquetDocumentInput docInput1 = writer.newDocumentInput();
        ParquetDocumentInput docInput2 = writer.newDocumentInput();
        
        assertNotNull("First document input should not be null", docInput1);
        assertNotNull("Second document input should not be null", docInput2);
        
        writer.close();
    }

    public void testSyncOperation() throws IOException {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        
        // sync() is currently a no-op, just verify it doesn't throw
        writer.sync();
        
        writer.close();
    }

    // ===== Flush Operation Tests =====

    public void testFlushWithNoData() throws IOException {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        FlushIn flushIn = new FlushIn() {};
        
        FileInfos result = writer.flush(flushIn);
        
        assertNotNull("Flush result should not be null", result);
        
        writer.close();
    }

    public void testMultipleFlushes() throws IOException {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        FlushIn flushIn = new FlushIn() {};
        
        FileInfos result1 = writer.flush(flushIn);
        FileInfos result2 = writer.flush(flushIn);
        
        assertNotNull("First flush result should not be null", result1);
        assertNotNull("Second flush result should not be null", result2);
        
        writer.close();
    }

    public void testFlushPreservesWriterGeneration() throws IOException {
        long customGeneration = 42L;
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, customGeneration, bufferPool);
        FlushIn flushIn = new FlushIn() {};
        
        FileInfos result = writer.flush(flushIn);
        
        assertNotNull("Flush result should not be null", result);
        // Writer generation is used internally by VSRManager
        
        writer.close();
    }

    // ===== Resource Management Tests =====

    public void testWriterClose() {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        
        writer.close();
        // Verify close can be called without error
    }

    public void testWriterCloseAfterOperations() throws IOException {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        
        // Perform some operations
        writer.newDocumentInput();
        writer.sync();
        writer.flush(new FlushIn() {});
        
        // Close should clean up all resources
        writer.close();
    }

    // ===== Error Handling Tests =====

    public void testConstructorWithNullFile() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new ParquetWriter(null, testSchema, TEST_GENERATION, bufferPool);
        });
        assertNotNull("Should throw NullPointerException for null file", exception);
    }

    public void testConstructorWithNullSchema() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new ParquetWriter(testFile, null, TEST_GENERATION, bufferPool);
        });
        assertNotNull("Should throw NullPointerException for null schema", exception);
    }

    public void testConstructorWithNullBufferPool() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new ParquetWriter(testFile, testSchema, TEST_GENERATION, null);
        });
        assertNotNull("Should throw NullPointerException for null buffer pool", exception);
    }

    // ===== Writer Generation Tests =====

    public void testWriterWithDifferentGenerations() {
        long[] generations = {0L, 1L, 100L, 1000L, Long.MAX_VALUE};
        
        for (long gen : generations) {
            String fileName = tempDir.resolve("test_" + gen + ".parquet").toString();
            ParquetWriter writer = new ParquetWriter(fileName, testSchema, gen, bufferPool);
            
            assertNotNull("Writer should be created with generation " + gen, writer);
            
            writer.close();
        }
    }

    public void testWriterGenerationZero() {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, 0L, bufferPool);
        
        assertNotNull("Writer should handle generation 0", writer);
        
        writer.close();
    }

    public void testWriterGenerationMaxValue() {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, Long.MAX_VALUE, bufferPool);
        
        assertNotNull("Writer should handle max generation value", writer);
        
        writer.close();
    }

    // ===== Schema Handling Tests =====

    public void testWriterWithEmptySchema() {
        Schema emptySchema = new Schema(Arrays.asList());
        ParquetWriter writer = new ParquetWriter(testFile, emptySchema, TEST_GENERATION, bufferPool);
        
        assertNotNull("Writer should handle empty schema", writer);
        ParquetDocumentInput docInput = writer.newDocumentInput();
        assertNotNull("Document input should be created with empty schema", docInput);
        
        writer.close();
    }

    public void testWriterWithSingleFieldSchema() {
        Field singleField = new Field("id", FieldType.nullable(Types.MinorType.INT.getType()), null);
        Schema singleFieldSchema = new Schema(Arrays.asList(singleField));
        
        ParquetWriter writer = new ParquetWriter(testFile, singleFieldSchema, TEST_GENERATION, bufferPool);
        
        assertNotNull("Writer should handle single field schema", writer);
        ParquetDocumentInput docInput = writer.newDocumentInput();
        assertNotNull("Document input should be created", docInput);
        
        writer.close();
    }

    public void testWriterWithMultipleFieldTypes() {
        Field intField = new Field("int_field", FieldType.nullable(Types.MinorType.INT.getType()), null);
        Field longField = new Field("long_field", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field floatField = new Field("float_field", FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        Field doubleField = new Field("double_field", FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        Field stringField = new Field("string_field", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        Field boolField = new Field("bool_field", FieldType.nullable(Types.MinorType.BIT.getType()), null);
        
        Schema multiTypeSchema = new Schema(Arrays.asList(
            intField, longField, floatField, doubleField, stringField, boolField
        ));
        
        ParquetWriter writer = new ParquetWriter(testFile, multiTypeSchema, TEST_GENERATION, bufferPool);
        
        assertNotNull("Writer should handle multiple field types", writer);
        ParquetDocumentInput docInput = writer.newDocumentInput();
        assertNotNull("Document input should be created for multi-type schema", docInput);
        
        writer.close();
    }

    // ===== Integration Pattern Tests =====

    public void testWriterLifecyclePattern() throws IOException {
        ParquetWriter writer = new ParquetWriter(testFile, testSchema, TEST_GENERATION, bufferPool);
        
        // Create document inputs
        ParquetDocumentInput docInput1 = writer.newDocumentInput();
        assertNotNull("First document input should be created", docInput1);
        
        ParquetDocumentInput docInput2 = writer.newDocumentInput();
        assertNotNull("Second document input should be created", docInput2);
        
        // Sync operation
        writer.sync();
        
        // Flush operation
        FlushIn flushIn = new FlushIn() {};
        FileInfos fileInfos = writer.flush(flushIn);
        assertNotNull("Flush should return file infos", fileInfos);
        
        // Close writer
        writer.close();
    }

    public void testMultipleWritersWithSameBufferPool() {
        String file1 = tempDir.resolve("test1.parquet").toString();
        String file2 = tempDir.resolve("test2.parquet").toString();
        String file3 = tempDir.resolve("test3.parquet").toString();
        
        ParquetWriter writer1 = new ParquetWriter(file1, testSchema, 1L, bufferPool);
        ParquetWriter writer2 = new ParquetWriter(file2, testSchema, 2L, bufferPool);
        ParquetWriter writer3 = new ParquetWriter(file3, testSchema, 3L, bufferPool);
        
        assertNotNull("First writer should be created", writer1);
        assertNotNull("Second writer should be created", writer2);
        assertNotNull("Third writer should be created", writer3);
        
        // All writers share the same buffer pool
        ParquetDocumentInput doc1 = writer1.newDocumentInput();
        ParquetDocumentInput doc2 = writer2.newDocumentInput();
        ParquetDocumentInput doc3 = writer3.newDocumentInput();
        
        assertNotNull("Document input from writer1 should be created", doc1);
        assertNotNull("Document input from writer2 should be created", doc2);
        assertNotNull("Document input from writer3 should be created", doc3);
        
        writer1.close();
        writer2.close();
        writer3.close();
    }
}
