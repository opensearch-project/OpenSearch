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
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;

/**
 * Unit Tests for ParquetExecutionEngine covering all must-have scenarios.
 */

public class ParquetExecutionEngineTests extends OpenSearchTestCase {

    private static final String TEST_INDEX_NAME = "test-index";
    private static final String TEST_INDEX_UUID = "test-uuid";
    private static final int TEST_SHARD_ID = 0;
    private static final String PRIMARY_TERM_FIELD_NAME = "_primary";
    private static final String ROW_ID_FIELD_NAME = "_id";
    
    private static final String USER_ID_FIELD_NAME = "user_id";
    private static final String COUNT_FIELD_NAME = "count";
    private static final String ENABLED_FIELD_NAME = "enabled";
    private static final String ID_FIELD_NAME = "id";
    private static final String NAME_FIELD_NAME = "name";
    private static final String ACTIVE_FIELD_NAME = "active";
    private static final String MESSAGE_FIELD_NAME = "message";
    private static final String STATUS_FIELD_NAME = "status";
    private static final String PRICE_FIELD_NAME = "price";
    
    private static final long TEST_GENERATION = 42L;
    private static final long PRIMARY_TERM_VALUE = 1L;
    private static final long FIRST_ROW_ID = 1001L;
    private static final long SECOND_ROW_ID = 1002L;
    private static final long THIRD_ROW_ID = 1003L;
    private static final long USER_ID_VALUE = 12345L;
    private static final int COUNT_VALUE = 42;
    private static final boolean ENABLED_VALUE = true;
    
    private static final String PARQUET_FILE_PREFIX = "test_parquet";
    private static final String PARQUET_FILE_EXTENSION = ".parquet";
    private static final String OTHER_FILE_PREFIX = "test_other";
    private static final String OTHER_FILE_EXTENSION = ".txt";
    private static final String OTHER_FORMAT_NAME = "OTHER_FORMAT";
    private static final String NON_EXISTENT_FILE_PATH = "/non/existent/file.parquet";
    private static final String PARQUET_FILE_GENERATION_PATTERN = "_parquet_file_generation_";
    
    private static final int EXPECTED_TEST_DATA_COUNT = 3;

    private Settings settings;
    private Supplier<Schema> schemaSupplier;
    private ShardPath shardPath;
    private Schema testSchema;
    private ParquetExecutionEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().build();
        testSchema = createTestSchema();
        schemaSupplier = () -> testSchema;
        Path tempDir = createTempDir();
        Index index = new Index(TEST_INDEX_NAME, TEST_INDEX_UUID);
        ShardId shardId = new ShardId(index, TEST_SHARD_ID);
        Path shardDataPath = tempDir.resolve(index.getUUID()).resolve(String.valueOf(TEST_SHARD_ID));
        Path shardStatePath = tempDir.resolve(index.getUUID()).resolve(String.valueOf(TEST_SHARD_ID));
        shardPath = new ShardPath(false, shardDataPath, shardStatePath, shardId);
        
        Path dataFormatDir = shardDataPath.resolve(PARQUET_DATA_FORMAT.name());
        Files.createDirectories(dataFormatDir);
        
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

    private Schema createTestSchema() {
        List<Field> fields = Arrays.asList(
            Field.nullable(CompositeDataFormatWriter.ROW_ID, Types.MinorType.BIGINT.getType()),
            Field.nullable(PRIMARY_TERM_FIELD_NAME, Types.MinorType.BIGINT.getType()),
            Field.nullable(ID_FIELD_NAME, Types.MinorType.BIGINT.getType()),
            Field.nullable(NAME_FIELD_NAME, Types.MinorType.VARCHAR.getType()),
            Field.nullable(ACTIVE_FIELD_NAME, Types.MinorType.BIT.getType()),
            Field.nullable(USER_ID_FIELD_NAME, Types.MinorType.BIGINT.getType()),
            Field.nullable(MESSAGE_FIELD_NAME, Types.MinorType.VARCHAR.getType()),
            Field.nullable(COUNT_FIELD_NAME, Types.MinorType.INT.getType()),
            Field.nullable(STATUS_FIELD_NAME, Types.MinorType.VARCHAR.getType()),
            Field.nullable(PRICE_FIELD_NAME, Types.MinorType.FLOAT8.getType()),
            Field.nullable(ENABLED_FIELD_NAME, Types.MinorType.BIT.getType())
        );
        return new Schema(fields);
    }



    public void testDeleteFilesOnlyDeletesParquetFiles() throws IOException {
        Path parquetFile = createTempFile(PARQUET_FILE_PREFIX, PARQUET_FILE_EXTENSION);
        Path otherFile = createTempFile(OTHER_FILE_PREFIX, OTHER_FILE_EXTENSION);
        Map<String, Collection<String>> filesToDelete = new HashMap<>();
        filesToDelete.put(PARQUET_DATA_FORMAT.name(), List.of(parquetFile.toString()));
        filesToDelete.put(OTHER_FORMAT_NAME, List.of(otherFile.toString()));
        engine.deleteFiles(filesToDelete);
        assertFalse(Files.exists(parquetFile));
        assertTrue(Files.exists(otherFile));
        Files.deleteIfExists(otherFile);
    }

    public void testDeleteFilesThrowsWhenFileDoesNotExist() {
        Map<String, Collection<String>> filesToDelete = Map.of(
            PARQUET_DATA_FORMAT.name(),
            List.of(NON_EXISTENT_FILE_PATH)
        );
        RuntimeException ex = expectThrows(RuntimeException.class, () -> engine.deleteFiles(filesToDelete));
        assertNotNull(ex.getCause());
    }

    public void testGetDataFormatReturnsParquetDataFormat() {
        DataFormat dataFormat = engine.getDataFormat();
        assertNotNull(dataFormat);
        assertEquals(PARQUET_DATA_FORMAT.name(), dataFormat.name());
    }

    public void testGetNativeBytesUsedReturnsNonNegative() {
        long nativeBytes = engine.getNativeBytesUsed();
        assertTrue(nativeBytes >= 0);
    }

    /**
     * Tests complete writer workflow with multiple documents.
     */
    public void testCompleteWriterWorkflowWithMultipleDocuments() throws Exception {
        Writer<ParquetDocumentInput> writer = engine.createWriter(TEST_GENERATION);

        Object[][] testData = {
            {FIRST_ROW_ID, new NumberFieldMapper.NumberFieldType(USER_ID_FIELD_NAME, NumberFieldMapper.NumberType.LONG), USER_ID_FIELD_NAME, USER_ID_VALUE},
            {SECOND_ROW_ID, new NumberFieldMapper.NumberFieldType(COUNT_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER), COUNT_FIELD_NAME, COUNT_VALUE},
            {THIRD_ROW_ID, new BooleanFieldMapper.BooleanFieldType(ENABLED_FIELD_NAME), ENABLED_FIELD_NAME, ENABLED_VALUE}
        };

        List<WriteResult> writeResults = new ArrayList<>();
        List<ParquetDocumentInput> documentInputs = new ArrayList<>();

        for (Object[] data : testData) {
            ParquetDocumentInput doc = writer.newDocumentInput();
            doc.addRowIdField(ROW_ID_FIELD_NAME, (Long) data[0]);
            doc.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, PRIMARY_TERM_VALUE);
            doc.addField((MappedFieldType) data[1], data[3]);
            WriteResult result = doc.addToWriter();
            assertTrue(result.success());
            writeResults.add(result);
            documentInputs.add(doc);
        }

        FileInfos fileInfos = writer.flush(new FlushIn() {});
        WriterFileSet parquetFileSet = fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT).orElse(null);

        assertNotNull(parquetFileSet);
        assertFalse(parquetFileSet.getFiles().isEmpty());

        boolean hasParquetFile = parquetFileSet.getFiles().stream()
            .anyMatch(f -> f.endsWith(PARQUET_FILE_EXTENSION) && f.contains(PARQUET_FILE_GENERATION_PATTERN + TEST_GENERATION));
        assertTrue(hasParquetFile);

        assertEquals(TEST_GENERATION, parquetFileSet.getWriterGeneration());
        assertEquals(EXPECTED_TEST_DATA_COUNT, writeResults.size());

        String parquetFileName = parquetFileSet.getFiles().stream()
            .filter(f -> f.endsWith(PARQUET_FILE_EXTENSION))
            .findFirst()
            .orElse(null);
        assertNotNull("Parquet file name should be present in file set", parquetFileName);
        
        Path parquetFilePath = Path.of(parquetFileSet.getDirectory(), parquetFileName);
        assertTrue("Parquet file should exist on disk: " + parquetFilePath, Files.exists(parquetFilePath));
        assertTrue("Parquet file should be a regular file: " + parquetFilePath, Files.isRegularFile(parquetFilePath));
        assertTrue("Parquet file should have content (size > 0): " + parquetFilePath, Files.size(parquetFilePath) > 0);

        writer.close();
    }
}
