/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RowIdAwareWriter}.
 */
public class RowIdAwareWriterTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testDocCountStartsAtZero() {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        assertEquals(0L, writer.docCount());
    }

    @SuppressWarnings("unchecked")
    public void testDocCountIncrementsOnAddDoc() throws IOException {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        DocumentInput<?> input = mock(DocumentInput.class);
        when(delegate.addDoc(input)).thenReturn(new WriteResult.Success(1L, 1L, 0L));

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        writer.addDoc(input);
        assertEquals(1L, writer.docCount());

        when(delegate.addDoc(input)).thenReturn(new WriteResult.Success(1L, 1L, 1L));
        writer.addDoc(input);
        assertEquals(2L, writer.docCount());
    }

    @SuppressWarnings("unchecked")
    public void testAddDocSetsRowId() throws IOException {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        DocumentInput<?> input = mock(DocumentInput.class);
        when(delegate.addDoc(input)).thenReturn(new WriteResult.Success(1L, 1L, 0L));

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        writer.addDoc(input);

        verify(input).setRowId(DocumentInput.ROW_ID_FIELD, 0L);
    }

    @SuppressWarnings("unchecked")
    public void testFlushDelegatesToUnderlying() throws IOException {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        FileInfos expected = FileInfos.empty();
        when(delegate.flush(FlushInput.EMPTY)).thenReturn(expected);

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        FileInfos result = writer.flush(FlushInput.EMPTY);

        assertEquals(expected, result);
        verify(delegate).flush(FlushInput.EMPTY);
    }

    @SuppressWarnings("unchecked")
    public void testFlushWithRowIdMapping() throws IOException {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        RowIdMapping mapping = new PackedRowIdMapping(new long[] { 1, 0 }, true);
        FlushInput flushInput = new FlushInput(mapping);
        FileInfos expected = FileInfos.empty();
        when(delegate.flush(flushInput)).thenReturn(expected);

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        FileInfos result = writer.flush(flushInput);

        assertEquals(expected, result);
        verify(delegate).flush(flushInput);
    }

    @SuppressWarnings("unchecked")
    public void testGenerationDelegates() {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        when(delegate.generation()).thenReturn(42L);

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        assertEquals(42L, writer.generation());
    }

    @SuppressWarnings("unchecked")
    public void testGetWriterForFormatDelegates() {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        // Default implementation returns empty
        assertFalse(writer.getWriterForFormat("test").isPresent());
    }

    @SuppressWarnings("unchecked")
    public void testCloseDelegates() throws IOException {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        writer.close();
        verify(delegate).close();
    }
}
