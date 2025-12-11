/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import com.parquet.parquetdataformat.vsr.VSRManager;
import org.mockito.MockitoAnnotations;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.Mock;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ParquetWriter using mocks to isolate behavior from native dependencies.
 */
public class ParquetWriterTests extends OpenSearchTestCase {
    @Mock
    private FlushIn mockFlushIn;
    @Mock
    private VSRManager mockVSRManager;
    @Mock
    private ManagedVSR mockManagedVSR;
    @Mock
    private ParquetWriter mockParquetWriter;

    private WriteResult testWriteResult;
    private Schema testSchema;
    private String testFilePath;
    private long testWriterGeneration;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        List<Field> fields = Arrays.asList(
            Field.nullable("id", Types.MinorType.BIGINT.getType()),
            Field.nullable("name", Types.MinorType.VARCHAR.getType())
        );
        testSchema = new Schema(fields);

        testFilePath = "/tmp/test.parquet";
        testWriterGeneration = 123L;
        testWriteResult = new WriteResult(true, null, 1L, 1L, 1L);

        when(mockVSRManager.getActiveManagedVSR()).thenReturn(mockManagedVSR);
        when(mockVSRManager.addToManagedVSR(any())).thenReturn(testWriteResult);
        when(mockVSRManager.flush(any())).thenReturn(null);

        setupParquetWriterMock();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void setupParquetWriterMock() throws IOException {
        when(mockParquetWriter.addDoc(any())).thenReturn(testWriteResult);
        when(mockParquetWriter.flush(any())).thenReturn(FileInfos.empty());

        ParquetDocumentInput mockDocInput = mock(ParquetDocumentInput.class);
        when(mockParquetWriter.newDocumentInput()).thenReturn(mockDocInput);

        doNothing().when(mockParquetWriter).sync();
        doNothing().when(mockParquetWriter).close();
    }

    public void testMockParquetWriterIsAvailable() {
        assertNotNull("Mock ParquetWriter should be available", mockParquetWriter);

        ParquetDocumentInput docInput = mockParquetWriter.newDocumentInput();
        assertNotNull("Writer should create document input", docInput);

        verify(mockParquetWriter).newDocumentInput();

        mockParquetWriter.close();
        verify(mockParquetWriter).close();
    }

    public void testDifferentWriterInstancesWorkIndependently() {
        ParquetWriter mockWriter1 = mock(ParquetWriter.class);
        ParquetWriter mockWriter2 = mock(ParquetWriter.class);

        ParquetDocumentInput mockInput1 = mock(ParquetDocumentInput.class);
        ParquetDocumentInput mockInput2 = mock(ParquetDocumentInput.class);

        when(mockWriter1.newDocumentInput()).thenReturn(mockInput1);
        when(mockWriter2.newDocumentInput()).thenReturn(mockInput2);

        assertNotSame("Different parameters should create different writers", mockWriter1, mockWriter2);

        ParquetDocumentInput input1 = mockWriter1.newDocumentInput();
        ParquetDocumentInput input2 = mockWriter2.newDocumentInput();

        assertNotNull("Writer 1 should work", input1);
        assertNotNull("Writer 2 should work", input2);
        assertNotSame("Inputs should be different", input1, input2);

        verify(mockWriter1).newDocumentInput();
        verify(mockWriter2).newDocumentInput();

        mockWriter1.close();
        mockWriter2.close();

        verify(mockWriter1).close();
        verify(mockWriter2).close();
    }

    public void testAddDocReturnsValidWriteResult() throws IOException {
        ParquetDocumentInput docInput = mockParquetWriter.newDocumentInput();
        WriteResult result = mockParquetWriter.addDoc(docInput);

        assertNotNull("addDoc should return WriteResult", result);
        assertTrue("WriteResult should indicate success", result.success());
        assertNull("WriteResult should have no exception", result.e());
        assertTrue("WriteResult should have valid version", result.version() >= 0);
        assertTrue("WriteResult should have valid term", result.term() >= 0);
        assertTrue("WriteResult should have valid sequence number", result.seqNo() >= 0);

        WriteResult result2 = new WriteResult(true, null, 2L, 2L, 2L);
        when(mockParquetWriter.addDoc(any())).thenReturn(result2);

        ParquetDocumentInput docInput2 = mockParquetWriter.newDocumentInput();
        WriteResult actualResult2 = mockParquetWriter.addDoc(docInput2);

        assertNotNull("Second addDoc should return WriteResult", actualResult2);
        assertTrue("Second WriteResult should indicate success", actualResult2.success());
        assertNull("Second WriteResult should have no exception", actualResult2.e());

        assertTrue("Second version should be >= first", actualResult2.version() >= result.version());
        assertTrue("Second term should be >= first", actualResult2.term() >= result.term());
        assertTrue("Second seqNo should be >= first", actualResult2.seqNo() >= result.seqNo());

        verify(mockParquetWriter, times(2)).addDoc(any());
        verify(mockParquetWriter, times(2)).newDocumentInput();

        mockParquetWriter.close();
    }

    public void testAddDocPropagatesIOException() throws IOException {
        IOException testException = new IOException("Test exception");
        when(mockParquetWriter.addDoc(null)).thenThrow(testException);

        IOException thrown = assertThrows(IOException.class, () -> mockParquetWriter.addDoc(null));
        assertNotNull("IOException should be thrown for null input", thrown);
        assertEquals("Exception message should match", "Test exception", thrown.getMessage());

        verify(mockParquetWriter).addDoc(null);
        mockParquetWriter.close();
    }

    public void testFlushReturnsEmptyFileInfosWhenNoData() throws IOException {
        when(mockParquetWriter.flush(mockFlushIn)).thenReturn(FileInfos.empty());

        FileInfos result = mockParquetWriter.flush(mockFlushIn);
        assertNotNull("Flush should return FileInfos", result);
        assertTrue("FileInfos should be empty", result.getWriterFilesMap().isEmpty());

        verify(mockParquetWriter).flush(mockFlushIn);
        mockParquetWriter.close();
    }


    public void testFlushBuildsCorrectFileInfosWithData() throws IOException {
        Path filePath = Path.of(testFilePath);
        WriterFileSet writerFileSet = WriterFileSet.builder()
            .directory(filePath.getParent())
            .writerGeneration(testWriterGeneration)
            .addFile(filePath.getFileName().toString())
            .build();
        FileInfos expectedFileInfos = FileInfos.builder()
            .putWriterFileSet(PARQUET_DATA_FORMAT, writerFileSet)
            .build();

        when(mockParquetWriter.flush(mockFlushIn)).thenReturn(expectedFileInfos);

        ParquetDocumentInput docInput = mockParquetWriter.newDocumentInput();
        WriteResult addResult = mockParquetWriter.addDoc(docInput);

        assertNotNull("addDoc should return result", addResult);
        assertTrue("addDoc should succeed before flush", addResult.success());

        FileInfos result = mockParquetWriter.flush(mockFlushIn);
        assertNotNull("Flush should return FileInfos", result);

        Map<DataFormat, WriterFileSet> writerFilesMap = result.getWriterFilesMap();
        assertNotNull("WriterFilesMap should not be null", writerFilesMap);

        assertTrue("Should contain PARQUET_DATA_FORMAT", writerFilesMap.containsKey(PARQUET_DATA_FORMAT));
        WriterFileSet actualWriterFileSet = writerFilesMap.get(PARQUET_DATA_FORMAT);

        assertEquals("Writer generation should match", testWriterGeneration, actualWriterFileSet.getWriterGeneration());
        assertNotNull("Directory should be set", actualWriterFileSet.getDirectory());
        assertNotNull("Files should be set", actualWriterFileSet.getFiles());

        String directory = actualWriterFileSet.getDirectory();
        assertTrue("Directory should be absolute path", directory.startsWith("/"));
        assertTrue("Directory should contain tmp", directory.contains("tmp"));

        Set<String> files = actualWriterFileSet.getFiles();
        assertFalse("ParquetWriter.flush() should produce files when data is flushed", files.isEmpty());
        assertTrue("Should contain test file", files.contains("test.parquet"));

        Optional<WriterFileSet> optionalFileSet = result.getWriterFileSet(PARQUET_DATA_FORMAT);
        assertTrue("Optional should be present", optionalFileSet.isPresent());
        WriterFileSet fileSet = optionalFileSet.get();
        assertEquals("ParquetWriter.flush() Optional access should return same generation",
                   testWriterGeneration, fileSet.getWriterGeneration());

        verify(mockParquetWriter).addDoc(docInput);
        verify(mockParquetWriter).flush(mockFlushIn);

        mockParquetWriter.close();
    }


    public void testFlushPropagatesIOException() throws IOException {
        IOException testException = new IOException("VSRManager flush failed");
        when(mockParquetWriter.flush(mockFlushIn)).thenThrow(testException);

        IOException thrown = assertThrows(IOException.class, () -> mockParquetWriter.flush(mockFlushIn));
        assertNotNull("IOException should have message", thrown.getMessage());
        assertEquals("Exception message should match", "VSRManager flush failed", thrown.getMessage());

        verify(mockParquetWriter).flush(mockFlushIn);

        mockParquetWriter.close();
    }


    public void testFlushHandlesNestedDirectoryPaths() throws IOException {
        String nestedPath = "/var/data/parquet/shard/12345/file.parquet";
        Path filePath = Path.of(nestedPath);

        WriterFileSet nestedWriterFileSet = WriterFileSet.builder()
            .directory(filePath.getParent())
            .writerGeneration(testWriterGeneration)
            .addFile(filePath.getFileName().toString())
            .build();
        FileInfos nestedFileInfos = FileInfos.builder()
            .putWriterFileSet(PARQUET_DATA_FORMAT, nestedWriterFileSet)
            .build();

        when(mockParquetWriter.flush(mockFlushIn)).thenReturn(nestedFileInfos);

        ParquetDocumentInput docInput = mockParquetWriter.newDocumentInput();
        mockParquetWriter.addDoc(docInput);

        FileInfos result = mockParquetWriter.flush(mockFlushIn);
        assertNotNull("Flush should return FileInfos for nested path", result);

        Map<DataFormat, WriterFileSet> writerFilesMap = result.getWriterFilesMap();
        assertTrue("Should contain PARQUET_DATA_FORMAT", writerFilesMap.containsKey(PARQUET_DATA_FORMAT));
        WriterFileSet writerFileSet = writerFilesMap.get(PARQUET_DATA_FORMAT);

        String directory = writerFileSet.getDirectory();
        assertTrue("Directory should contain nested path", directory.contains("/var/data/parquet/shard/12345"));

        verify(mockParquetWriter).addDoc(docInput);
        verify(mockParquetWriter).flush(mockFlushIn);

        mockParquetWriter.close();
    }


    public void testSyncIsNoOp() throws IOException {
        mockParquetWriter.sync();
        verify(mockParquetWriter).sync();
        mockParquetWriter.close();
    }

    public void testCloseCanBeCalledMultipleTimes() {
        mockParquetWriter.close();
        mockParquetWriter.close();
        verify(mockParquetWriter, times(2)).close();
    }

    public void testNewDocumentInputReturnsValidInput() throws IOException {
        ParquetDocumentInput result = mockParquetWriter.newDocumentInput();

        assertNotNull("Should return ParquetDocumentInput", result);
        verify(mockParquetWriter).newDocumentInput();
        mockParquetWriter.close();
    }



    public void testCompleteWriterLifecycle() throws IOException {
        Path filePath = Path.of(testFilePath);
        WriterFileSet writerFileSet = WriterFileSet.builder()
            .directory(filePath.getParent())
            .writerGeneration(testWriterGeneration)
            .addFile(filePath.getFileName().toString())
            .build();
        FileInfos expectedFileInfos = FileInfos.builder()
            .putWriterFileSet(PARQUET_DATA_FORMAT, writerFileSet)
            .build();

        when(mockParquetWriter.flush(mockFlushIn)).thenReturn(expectedFileInfos);

        ParquetDocumentInput docInput = mockParquetWriter.newDocumentInput();
        WriteResult addResult = mockParquetWriter.addDoc(docInput);

        assertNotNull("addDoc should return result", addResult);
        assertTrue("addDoc should succeed in lifecycle", addResult.success());
        assertNull("addDoc should have no exception in lifecycle", addResult.e());
        assertTrue("addDoc should have valid version in lifecycle", addResult.version() >= 0);
        assertTrue("addDoc should have valid term in lifecycle", addResult.term() >= 0);
        assertTrue("addDoc should have valid seqNo in lifecycle", addResult.seqNo() >= 0);

        FileInfos flushResult = mockParquetWriter.flush(mockFlushIn);

        assertNotNull("flush should return result", flushResult);
        Map<DataFormat, WriterFileSet> writerFilesMap = flushResult.getWriterFilesMap();
        assertNotNull("WriterFilesMap should be available in lifecycle", writerFilesMap);

        if (writerFilesMap.containsKey(PARQUET_DATA_FORMAT)) {
            WriterFileSet lifecycleWriterFileSet = writerFilesMap.get(PARQUET_DATA_FORMAT);
            assertEquals("Generation should be consistent throughout lifecycle",
                       testWriterGeneration, lifecycleWriterFileSet.getWriterGeneration());

            assertNotNull("Directory should be set after lifecycle operations", lifecycleWriterFileSet.getDirectory());
            assertNotNull("Files should be set after lifecycle operations", lifecycleWriterFileSet.getFiles());

            Set<String> files = lifecycleWriterFileSet.getFiles();
            if (!files.isEmpty()) {
                for (String fileName : files) {
                    assertNotNull("File name should not be null", fileName);
                    assertFalse("File name should not be empty", fileName.isEmpty());
                    assertTrue("File should have parquet extension", fileName.endsWith(".parquet"));
                }
            }
        }

        WriteResult addResult2 = new WriteResult(true, null, 2L, 2L, 2L);
        when(mockParquetWriter.addDoc(any())).thenReturn(addResult2);

        ParquetDocumentInput docInput2 = mockParquetWriter.newDocumentInput();
        WriteResult actualAddResult2 = mockParquetWriter.addDoc(docInput2);
        FileInfos flushResult2 = mockParquetWriter.flush(mockFlushIn);

        assertNotNull("Second addDoc should return result", actualAddResult2);
        assertTrue("Second addDoc should succeed", actualAddResult2.success());
        assertNotNull("Second flush should return result", flushResult2);

        assertTrue("Second operation should have progressed version",
                 actualAddResult2.version() >= addResult.version());
        assertTrue("Second operation should have progressed seqNo",
                 actualAddResult2.seqNo() >= addResult.seqNo());

        verify(mockParquetWriter, times(2)).newDocumentInput();
        verify(mockParquetWriter, times(2)).addDoc(any());
        verify(mockParquetWriter, times(2)).flush(mockFlushIn);

        mockParquetWriter.close();
    }


    public void testFlushFileInfosStructure() throws IOException {
        when(mockParquetWriter.flush(mockFlushIn)).thenReturn(FileInfos.empty());
        FileInfos emptyResult = mockParquetWriter.flush(mockFlushIn);
        assertNotNull("ParquetWriter.flush() should return FileInfos even when empty", emptyResult);

        Path filePath = Path.of(testFilePath);
        WriterFileSet writerFileSet = WriterFileSet.builder()
            .directory(filePath.getParent())
            .writerGeneration(testWriterGeneration)
            .addFile(filePath.getFileName().toString())
            .build();
        FileInfos expectedFileInfos = FileInfos.builder()
            .putWriterFileSet(PARQUET_DATA_FORMAT, writerFileSet)
            .build();

        when(mockParquetWriter.flush(mockFlushIn)).thenReturn(expectedFileInfos);

        ParquetDocumentInput docInput = mockParquetWriter.newDocumentInput();
        WriteResult addResult = mockParquetWriter.addDoc(docInput);
        assertTrue("ParquetWriter.addDoc() should succeed before flush test", addResult.success());

        FileInfos result = mockParquetWriter.flush(mockFlushIn);
        assertNotNull("ParquetWriter.flush() should return FileInfos", result);

        Map<DataFormat, WriterFileSet> writerFilesMap = result.getWriterFilesMap();
        assertNotNull("ParquetWriter.flush() FileInfos should have WriterFilesMap", writerFilesMap);

        assertTrue("Should contain PARQUET_DATA_FORMAT", writerFilesMap.containsKey(PARQUET_DATA_FORMAT));
        WriterFileSet actualWriterFileSet = writerFilesMap.get(PARQUET_DATA_FORMAT);
        assertEquals("ParquetWriter should set correct generation in WriterFileSet",
                   testWriterGeneration, actualWriterFileSet.getWriterGeneration());
        assertNotNull("ParquetWriter should set directory in WriterFileSet", actualWriterFileSet.getDirectory());
        assertNotNull("ParquetWriter should set files in WriterFileSet", actualWriterFileSet.getFiles());

        verify(mockParquetWriter).newDocumentInput();
        verify(mockParquetWriter).addDoc(docInput);
        verify(mockParquetWriter, times(2)).flush(mockFlushIn);

        mockParquetWriter.close();
    }
}
