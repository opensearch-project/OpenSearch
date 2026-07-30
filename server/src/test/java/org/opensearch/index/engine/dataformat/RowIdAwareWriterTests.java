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
    private static Writer<DocumentInput<?>> activeDelegate() {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        when(delegate.state()).thenReturn(WriterState.ACTIVE);
        return delegate;
    }

    public void testDocCountStartsAtZero() {
        Writer<DocumentInput<?>> delegate = activeDelegate();
        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        assertEquals(0L, writer.docCount());
    }

    @SuppressWarnings("unchecked")
    public void testDocCountIncrementsOnAddDoc() throws IOException {
        Writer<DocumentInput<?>> delegate = activeDelegate();
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
    public void testAddDocSetsRowIdInOrder() throws IOException {
        Writer<DocumentInput<?>> delegate = activeDelegate();
        DocumentInput<?> input = mock(DocumentInput.class);
        when(delegate.addDoc(input)).thenReturn(new WriteResult.Success(1L, 1L, 0L));

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        writer.addDoc(input);
        writer.addDoc(input);
        writer.addDoc(input);

        verify(input).setRowId(DocumentInput.ROW_ID_FIELD, 0L);
        verify(input).setRowId(DocumentInput.ROW_ID_FIELD, 1L);
        verify(input).setRowId(DocumentInput.ROW_ID_FIELD, 2L);
    }

    @SuppressWarnings("unchecked")
    public void testFlushDelegatesToUnderlying() throws IOException {
        Writer<DocumentInput<?>> delegate = activeDelegate();
        FileInfos expected = FileInfos.empty();
        when(delegate.flush(FlushInput.EMPTY)).thenReturn(expected);

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        FileInfos result = writer.flush(FlushInput.EMPTY);

        assertEquals(expected, result);
        verify(delegate).flush(FlushInput.EMPTY);
    }

    @SuppressWarnings("unchecked")
    public void testFlushWithRowIdMapping() throws IOException {
        Writer<DocumentInput<?>> delegate = activeDelegate();
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

    // ==============================================================================
    // Failure handling: rowId counter behavior under different leaf failure modes
    // ==============================================================================

    /**
     * Parquet-style: leaf throws but recovers (rolls back internally) and stays ACTIVE.
     * The consumed rowId must be returned so the next addDoc reuses it — otherwise we
     * leave a permanent gap in the rowId sequence.
     */
    /**
     * Cross-doc rowId monotonicity: across many addDoc calls (all successful), the
     * sequence of rowIds set on inputs must be 0, 1, 2, ... contiguous and ordered.
     */
    @SuppressWarnings("unchecked")
    public void testRowIdMonotonicityAcrossManyDocs() throws IOException {
        Writer<DocumentInput<?>> delegate = activeDelegate();
        when(delegate.addDoc(org.mockito.ArgumentMatchers.any())).thenReturn(new WriteResult.Success(1L, 1L, 0L));

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        long[] observed = new long[100];
        for (int i = 0; i < 100; i++) {
            DocumentInput<?> input = mock(DocumentInput.class);
            final int idx = i;
            org.mockito.Mockito.doAnswer(inv -> {
                observed[idx] = inv.getArgument(1);
                return null;
            }).when(input).setRowId(org.mockito.ArgumentMatchers.eq(DocumentInput.ROW_ID_FIELD), org.mockito.ArgumentMatchers.anyLong());
            writer.addDoc(input);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals("rowId at position " + i + " must equal " + i, (long) i, observed[i]);
        }
    }

    /**
     * Direct invariant: ensureActive() throws on a non-ACTIVE delegate (was an assert,
     * now a runtime check per review feedback).
     */
    @SuppressWarnings("unchecked")
    public void testAddDocOnRetiredWriterThrowsRuntime() {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        when(delegate.state()).thenReturn(WriterState.RETIRED_FLUSHABLE);
        DocumentInput<?> input = mock(DocumentInput.class);

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        expectThrows(IllegalStateException.class, () -> writer.addDoc(input));
    }

    @SuppressWarnings("unchecked")
    public void testRollbackOnRetiredWriterThrowsRuntime() {
        Writer<DocumentInput<?>> delegate = mock(Writer.class);
        when(delegate.state()).thenReturn(WriterState.RETIRED_FLUSHABLE);

        RowIdAwareWriter<DocumentInput<?>> writer = new RowIdAwareWriter<>(delegate);
        expectThrows(UnsupportedOperationException.class, () -> writer.rollbackTo(0));
    }
}
