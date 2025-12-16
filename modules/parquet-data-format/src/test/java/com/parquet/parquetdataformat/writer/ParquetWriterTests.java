/*
 * SPDX-License-Identifier: Apache-2.0
 */

package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import com.parquet.parquetdataformat.vsr.VSRManager;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit Tests for ParquetWriter covering all must-have scenarios.
 */
public class ParquetWriterTests extends OpenSearchTestCase {

    private ParquetWriter writer;

    @Mock
    private VSRManager vsrManager;

    @Mock
    private FlushIn flushIn;

    @Override
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        Schema schema = new Schema(List.of(
            Field.nullable("id", Types.MinorType.BIGINT.getType()),
            Field.nullable("name", Types.MinorType.VARCHAR.getType()),
            Field.nullable("email", Types.MinorType.VARCHAR.getType()),
            Field.nullable("age", Types.MinorType.INT.getType()),
            Field.nullable("active", Types.MinorType.BIT.getType()),
            Field.nullable("created_at", Types.MinorType.BIGINT.getType())
        ));

        Path tempDir = createTempDir();
        Path parquetFile = tempDir.resolve("unit-test.parquet");

        ArrowBufferPool bufferPool = new ArrowBufferPool(Settings.EMPTY);
        writer = new ParquetWriter(
            parquetFile.toString(),
            schema,
            42L,
            bufferPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        super.tearDown();
    }

    public void shouldDelegateAddDocToVSRManagerAndReturnResult() throws IOException {
        ParquetDocumentInput input = mock(ParquetDocumentInput.class);
        WriteResult expectedResult = new WriteResult(true, null, 1L, 1L, 1L);

        when(vsrManager.addToManagedVSR(input)).thenReturn(expectedResult);

        WriteResult actualResult = writer.addDoc(input);

        assertSame("AddDoc should return WriteResult from VSRManager", expectedResult, actualResult);
        verify(vsrManager).addToManagedVSR(input);
        verifyNoMoreInteractions(vsrManager);
    }

    public void shouldPropagateIOExceptionThrownByVSRManagerOnAddDoc() throws IOException {
        ParquetDocumentInput input = mock(ParquetDocumentInput.class);
        IOException expectedException = new IOException("Simulated add failure");

        when(vsrManager.addToManagedVSR(input)).thenThrow(expectedException);

        IOException thrown = assertThrows(IOException.class, () -> writer.addDoc(input));
        assertEquals("Simulated add failure", thrown.getMessage());

        verify(vsrManager).addToManagedVSR(input);
        verifyNoMoreInteractions(vsrManager);
    }

    public void shouldReturnEmptyFileInfosWhenFlushReturnsNull() throws IOException {
        when(vsrManager.flush(flushIn)).thenReturn(null);

        FileInfos fileInfos = writer.flush(flushIn);

        assertNotNull(fileInfos);
        assertTrue("FileInfos should be empty when flush returns null", fileInfos.getWriterFilesMap().isEmpty());
        verify(vsrManager).flush(flushIn);
        verifyNoMoreInteractions(vsrManager);
    }

    public void shouldBuildCorrectWriterFileSetOnFlush() throws IOException {
        String flushFilePath = "/tmp/output/data.parquet";
        when(vsrManager.flush(flushIn)).thenReturn(flushFilePath);

        FileInfos fileInfos = writer.flush(flushIn);

        Optional<WriterFileSet> optional = fileInfos.getWriterFileSet(PARQUET_DATA_FORMAT);
        assertTrue("WriterFileSet should be present after flush", optional.isPresent());

        WriterFileSet fileSet = optional.get();
        assertEquals("Writer generation should match", 42L, fileSet.getWriterGeneration());
        assertEquals("Directory should match", Path.of("/tmp/output"), fileSet.getDirectory());
        assertTrue("Files should contain parquet filename", fileSet.getFiles().contains("data.parquet"));

        verify(vsrManager).flush(flushIn);
        verifyNoMoreInteractions(vsrManager);
    }

    public void shouldCreateNewDocumentInputAndHandleRotationFailureGracefully() throws IOException {
        ManagedVSR managedVSR = mock(ManagedVSR.class);

        doThrow(new IOException("Rotation failed")).when(vsrManager).maybeRotateActiveVSR();
        when(vsrManager.getActiveManagedVSR()).thenReturn(managedVSR);

        ParquetDocumentInput input = writer.newDocumentInput();

        assertNotNull("New document input should not be null even on rotation failure", input);
        verify(vsrManager).maybeRotateActiveVSR();
        verify(vsrManager).getActiveManagedVSR();
        verifyNoMoreInteractions(vsrManager);
    }

    public void shouldDelegateCloseToVSRManager() {
        writer.close();
        verify(vsrManager).close();
        verifyNoMoreInteractions(vsrManager);
    }

    public void shouldInvokeMethodsInProperLifecycleOrder() throws IOException {
        ManagedVSR managedVSR = mock(ManagedVSR.class);
        when(vsrManager.getActiveManagedVSR()).thenReturn(managedVSR);
        when(vsrManager.addToManagedVSR(any())).thenReturn(new WriteResult(true, null, 1L, 1L, 1L));
        when(vsrManager.flush(any())).thenReturn(null);

        ParquetDocumentInput input = writer.newDocumentInput();
        writer.addDoc(input);
        writer.flush(flushIn);
        writer.close();

        InOrder inOrder = inOrder(vsrManager);
        inOrder.verify(vsrManager).maybeRotateActiveVSR();
        inOrder.verify(vsrManager).getActiveManagedVSR();
        inOrder.verify(vsrManager).addToManagedVSR(any());
        inOrder.verify(vsrManager).flush(flushIn);
        inOrder.verify(vsrManager).close();

        verifyNoMoreInteractions(vsrManager);
    }

    public void shouldAllowMultipleSyncCallsWithoutSideEffects() throws IOException {
        // sync is a no-op in ParquetWriter, so just call multiple times and verify no exception
        writer.sync();
        writer.sync();

        // No interactions expected with vsrManager on sync
        verifyNoInteractions(vsrManager);
    }
}
