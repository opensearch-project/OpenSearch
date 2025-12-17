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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;

/**
 * Unit tests for ParquetWriter testing actual behavior through public interface.
 */
public class ParquetWriterTests extends OpenSearchTestCase {

    private ParquetWriter writer;
    private ArrowBufferPool bufferPool;
    private Path tempFile;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Schema schema = new Schema(Arrays.asList(
            Field.nullable(CompositeDataFormatWriter.ROW_ID, Types.MinorType.BIGINT.getType()),
            Field.nullable("_primary", Types.MinorType.BIGINT.getType()),
            Field.nullable("id", Types.MinorType.BIGINT.getType()),
            Field.nullable("name", Types.MinorType.VARCHAR.getType()),
            Field.nullable("active", Types.MinorType.BIT.getType())
        ));

        Path tempDir = createTempDir();
        tempFile = tempDir.resolve("test-writer.parquet");

        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        writer = new ParquetWriter(tempFile.toString(), schema, 42L, bufferPool);
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
        input.addRowIdField("_id", 123L);
        input.setPrimaryTerm("_primary", 1L);

        WriteResult actualResult = writer.addDoc(input);

        assertNotNull("AddDoc should return WriteResult", actualResult);
        assertTrue("WriteResult should indicate success", actualResult.success());
        assertNull("WriteResult should have no exception", actualResult.e());
    }

    public void shouldHandleAddDocWithClosedWriter() throws IOException {
        ParquetDocumentInput input = writer.newDocumentInput();
        input.addRowIdField("_id", 123L);
        input.setPrimaryTerm("_primary", 1L);

        writer.close();

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> writer.addDoc(input));
        assertNotNull("Exception should be thrown when adding to closed writer", thrown);
    }

    public void shouldReturnEmptyFileInfosWhenFlushWithNoData() throws IOException {
        FileInfos fileInfos = writer.flush(new FlushIn() {});

        assertNotNull(fileInfos);
        assertTrue("FileInfos should be empty when no data to flush", fileInfos.getWriterFilesMap().isEmpty());
    }

    public void shouldBuildCorrectWriterFileSetOnFlush() throws IOException {
        ParquetDocumentInput input = writer.newDocumentInput();
        input.addRowIdField("_id", 456L);
        input.setPrimaryTerm("_primary", 1L);
        
        writer.addDoc(input);
        FileInfos fileInfos = writer.flush(new FlushIn() {});

        Optional<WriterFileSet> optional = fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT);
        assertTrue("WriterFileSet should be present after flush", optional.isPresent());

        WriterFileSet fileSet = optional.get();
        assertEquals("Writer generation should match", 42L, fileSet.getWriterGeneration());
        assertEquals("Directory should match", tempFile.getParent().toString(), fileSet.getDirectory());
        assertTrue("Files should contain parquet filename", 
                  fileSet.getFiles().stream().anyMatch(f -> f.endsWith(".parquet")));
    }

    public void shouldCreateNewDocumentInputSuccessfully() throws IOException {
        ParquetDocumentInput input1 = writer.newDocumentInput();
        ParquetDocumentInput input2 = writer.newDocumentInput();

        assertNotNull(input1);
        assertNotNull(input2);
        assertNotNull(input1.getFinalInput());
        assertNotSame(input1, input2);
        
        input1.addRowIdField("_id", 100L);
        input1.setPrimaryTerm("_primary", 1L);
        input2.addRowIdField("_id", 200L);
        input2.setPrimaryTerm("_primary", 2L);
        
        assertTrue(writer.addDoc(input1).success());
        assertTrue(writer.addDoc(input2).success());
    }

    public void shouldCloseSuccessfully() {
        writer.close();
        writer.close();
    }

    public void shouldExecuteCompleteLifecycleSuccessfully() throws IOException {
        ParquetDocumentInput input1 = writer.newDocumentInput();
        input1.addRowIdField("_id", 100L);
        input1.setPrimaryTerm("_primary", 1L);
        
        ParquetDocumentInput input2 = writer.newDocumentInput();
        input2.addRowIdField("_id", 200L);
        input2.setPrimaryTerm("_primary", 1L);

        WriteResult result1 = writer.addDoc(input1);
        WriteResult result2 = writer.addDoc(input2);
        
        assertTrue("First write should succeed", result1.success());
        assertTrue("Second write should succeed", result2.success());
        
        FileInfos fileInfos = writer.flush(new FlushIn() {});
        assertNotNull("FileInfos should not be null", fileInfos);
        
        writer.close();
    }

    public void shouldAllowMultipleSyncCallsWithoutSideEffects() throws IOException {
        writer.sync();
        writer.sync();
    }

    public void testCompleteEndToEndWorkflow() throws IOException {
        ParquetDocumentInput doc1 = writer.newDocumentInput();
        doc1.addRowIdField("_id", 1001L);
        doc1.setPrimaryTerm("_primary", 1L);
        
        ParquetDocumentInput doc2 = writer.newDocumentInput();
        doc2.addRowIdField("_id", 1002L);
        doc2.setPrimaryTerm("_primary", 2L);
        
        ParquetDocumentInput doc3 = writer.newDocumentInput();
        doc3.addRowIdField("_id", 1003L);
        doc3.setPrimaryTerm("_primary", 3L);

        WriteResult result1 = writer.addDoc(doc1);
        WriteResult result2 = writer.addDoc(doc2);
        WriteResult result3 = writer.addDoc(doc3);

        assertTrue("All writes should succeed", result1.success() && result2.success() && result3.success());

        FileInfos fileInfos = writer.flush(new FlushIn() {});
        
        assertNotNull("FileInfos should not be null", fileInfos);
        Optional<WriterFileSet> writerFileSet = fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT);
        
        if (writerFileSet.isPresent()) {
            WriterFileSet fileSet = writerFileSet.get();
            assertEquals("Generation should match", 42L, fileSet.getWriterGeneration());
            assertFalse("Files should not be empty", fileSet.getFiles().isEmpty());
            
            boolean hasParquetFile = fileSet.getFiles().stream().anyMatch(f -> f.endsWith(".parquet"));
            assertTrue("Should contain parquet file", hasParquetFile);
        }

        writer.close();
    }
}
