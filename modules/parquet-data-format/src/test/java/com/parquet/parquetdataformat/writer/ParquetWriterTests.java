/*
 * SPDX-License-Identifier: Apache-2.0
 */

package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;

/**
 * Unit tests for ParquetWriter testing actual behavior through public interface.
 */
public class ParquetWriterTests extends OpenSearchTestCase {

    private static final String PRIMARY_TERM_FIELD_NAME = "_primary";
    private static final String ROW_ID_FIELD_NAME = "_id";
    private static final String ID_FIELD_NAME = "id";
    private static final String NAME_FIELD_NAME = "name";
    private static final String ACTIVE_FIELD_NAME = "active";
    
    private static final String TEST_WRITER_FILENAME = "test-writer.parquet";
    private static final String PARQUET_FILE_EXTENSION = ".parquet";
    
    private static final long TEST_WRITER_GENERATION = 42L;
    private static final long FIRST_PRIMARY_TERM = 1L;
    private static final long SECOND_PRIMARY_TERM = 2L;
    private static final long THIRD_PRIMARY_TERM = 3L;
    
    private static final long FIRST_ROW_ID = 123L;
    private static final long SECOND_ROW_ID = 456L;
    private static final long THIRD_ROW_ID = 100L;
    private static final long FOURTH_ROW_ID = 200L;
    private static final long FIFTH_ROW_ID = 1001L;
    private static final long SIXTH_ROW_ID = 1002L;
    private static final long SEVENTH_ROW_ID = 1003L;

    private ParquetWriter writer;
    private ArrowBufferPool bufferPool;
    private Path tempFile;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Schema schema = new Schema(Arrays.asList(
            Field.nullable(CompositeDataFormatWriter.ROW_ID, Types.MinorType.BIGINT.getType()),
            Field.nullable(PRIMARY_TERM_FIELD_NAME, Types.MinorType.BIGINT.getType()),
            Field.nullable(ID_FIELD_NAME, Types.MinorType.BIGINT.getType()),
            Field.nullable(NAME_FIELD_NAME, Types.MinorType.VARCHAR.getType()),
            Field.nullable(ACTIVE_FIELD_NAME, Types.MinorType.BIT.getType())
        ));
        Path tempDir = createTempDir();
        tempFile = tempDir.resolve(TEST_WRITER_FILENAME);
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        writer = new ParquetWriter(tempFile.toString(), schema, TEST_WRITER_GENERATION, bufferPool);
    }

    @Override
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (bufferPool != null) {
            bufferPool.close();
        }
        super.tearDown();
    }

    public void shouldDelegateAddDocToVSRManagerAndReturnResult() throws IOException {
        ParquetDocumentInput input = writer.newDocumentInput();
        input.addRowIdField(ROW_ID_FIELD_NAME, FIRST_ROW_ID);
        input.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, FIRST_PRIMARY_TERM);
        WriteResult actualResult = writer.addDoc(input);
        assertNotNull(actualResult);
        assertTrue(actualResult.success());
        assertNull(actualResult.e());
    }

    public void shouldHandleAddDocWithClosedWriter() throws IOException {
        ParquetDocumentInput input = writer.newDocumentInput();
        input.addRowIdField(ROW_ID_FIELD_NAME, FIRST_ROW_ID);
        input.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, FIRST_PRIMARY_TERM);
        writer.close();
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> writer.addDoc(input));
        assertNotNull(thrown);
    }

    public void shouldReturnEmptyFileInfosWhenFlushWithNoData() throws IOException {
        FileInfos fileInfos = writer.flush(new FlushIn() {});
        assertNotNull(fileInfos);
        assertTrue(fileInfos.getWriterFilesMap().isEmpty());
    }

    public void shouldBuildCorrectWriterFileSetOnFlush() throws IOException {
        ParquetDocumentInput input = writer.newDocumentInput();
        input.addRowIdField(ROW_ID_FIELD_NAME, SECOND_ROW_ID);
        input.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, FIRST_PRIMARY_TERM);
        writer.addDoc(input);
        FileInfos fileInfos = writer.flush(new FlushIn() {});
        Optional<WriterFileSet> optional = fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT);
        assertTrue(optional.isPresent());
        WriterFileSet fileSet = optional.get();
        assertEquals(TEST_WRITER_GENERATION, fileSet.getWriterGeneration());
        assertEquals(tempFile.getParent().toString(), fileSet.getDirectory());
        assertTrue(fileSet.getFiles().stream().anyMatch(f -> f.endsWith(PARQUET_FILE_EXTENSION)));
        
        String parquetFileName = fileSet.getFiles().stream()
            .filter(f -> f.endsWith(PARQUET_FILE_EXTENSION))
            .findFirst()
            .orElse(null);
        assertNotNull(parquetFileName);
        
        Path parquetFilePath = Path.of(fileSet.getDirectory(), parquetFileName);
        assertTrue("Parquet file should exist: " + parquetFilePath, Files.exists(parquetFilePath));
        assertTrue("Parquet file should be regular file: " + parquetFilePath, Files.isRegularFile(parquetFilePath));
    }

    public void shouldCreateNewDocumentInputSuccessfully() throws IOException {
        ParquetDocumentInput input1 = writer.newDocumentInput();
        ParquetDocumentInput input2 = writer.newDocumentInput();
        assertNotNull(input1);
        assertNotNull(input2);
        assertNotNull(input1.getFinalInput());
        assertNotSame(input1, input2);
        input1.addRowIdField(ROW_ID_FIELD_NAME, THIRD_ROW_ID);
        input1.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, FIRST_PRIMARY_TERM);
        input2.addRowIdField(ROW_ID_FIELD_NAME, FOURTH_ROW_ID);
        input2.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, SECOND_PRIMARY_TERM);
        assertTrue(writer.addDoc(input1).success());
        assertTrue(writer.addDoc(input2).success());
    }

    public void shouldExecuteCompleteLifecycleSuccessfully() throws IOException {
        ParquetDocumentInput input1 = writer.newDocumentInput();
        input1.addRowIdField(ROW_ID_FIELD_NAME, THIRD_ROW_ID);
        input1.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, FIRST_PRIMARY_TERM);
        ParquetDocumentInput input2 = writer.newDocumentInput();
        input2.addRowIdField(ROW_ID_FIELD_NAME, FOURTH_ROW_ID);
        input2.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, FIRST_PRIMARY_TERM);
        WriteResult result1 = input1.addToWriter();
        WriteResult result2 = input2.addToWriter();
        assertTrue(result1.success());
        assertTrue(result2.success());
        FileInfos fileInfos = writer.flush(new FlushIn() {});
        assertNotNull(fileInfos);
        
        Optional<WriterFileSet> writerFileSet = fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT);
        assertTrue(writerFileSet.isPresent());
        WriterFileSet fileSet = writerFileSet.get();
        String parquetFileName = fileSet.getFiles().stream()
            .filter(f -> f.endsWith(PARQUET_FILE_EXTENSION))
            .findFirst()
            .orElse(null);
        assertNotNull(parquetFileName);
        
        Path parquetFilePath = Path.of(fileSet.getDirectory(), parquetFileName);
        assertTrue("Parquet file should exist: " + parquetFilePath, Files.exists(parquetFilePath));
        assertTrue("Parquet file should be regular file: " + parquetFilePath, Files.isRegularFile(parquetFilePath));
        
        writer.close();
    }

    public void testCompleteEndToEndWorkflow() throws IOException {
        ParquetDocumentInput doc1 = writer.newDocumentInput();
        doc1.addRowIdField(ROW_ID_FIELD_NAME, FIFTH_ROW_ID);
        doc1.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, FIRST_PRIMARY_TERM);
        ParquetDocumentInput doc2 = writer.newDocumentInput();
        doc2.addRowIdField(ROW_ID_FIELD_NAME, SIXTH_ROW_ID);
        doc2.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, SECOND_PRIMARY_TERM);
        ParquetDocumentInput doc3 = writer.newDocumentInput();
        doc3.addRowIdField(ROW_ID_FIELD_NAME, SEVENTH_ROW_ID);
        doc3.setPrimaryTerm(PRIMARY_TERM_FIELD_NAME, THIRD_PRIMARY_TERM);
        WriteResult result1 = doc1.addToWriter();
        WriteResult result2 = doc2.addToWriter();
        WriteResult result3 = doc3.addToWriter();
        assertTrue(result1.success() && result2.success() && result3.success());
        FileInfos fileInfos = writer.flush(new FlushIn() {});
        assertNotNull(fileInfos);
        Optional<WriterFileSet> writerFileSet = fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT);
        if (writerFileSet.isPresent()) {
            WriterFileSet fileSet = writerFileSet.get();
            assertEquals(TEST_WRITER_GENERATION, fileSet.getWriterGeneration());
            assertFalse(fileSet.getFiles().isEmpty());
            boolean hasParquetFile = fileSet.getFiles().stream().anyMatch(f -> f.endsWith(PARQUET_FILE_EXTENSION));
            assertTrue(hasParquetFile);
            
            String parquetFileName = fileSet.getFiles().stream()
                .filter(f -> f.endsWith(PARQUET_FILE_EXTENSION))
                .findFirst()
                .orElse(null);
            assertNotNull(parquetFileName);
            
            Path parquetFilePath = Path.of(fileSet.getDirectory(), parquetFileName);
            assertTrue("Parquet file should exist: " + parquetFilePath, Files.exists(parquetFilePath));
            assertTrue("Parquet file should be regular file: " + parquetFilePath, Files.isRegularFile(parquetFilePath));
            assertTrue("Parquet file should have content: " + parquetFilePath, Files.size(parquetFilePath) > 0);
        }
        writer.close();
    }
}
