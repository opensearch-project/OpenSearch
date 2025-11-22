/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Before;
import org.opensearch.datafusion.search.RecordBatchIterator;
import org.opensearch.test.OpenSearchTestCase;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

public class RecordBatchIteratorTests extends OpenSearchTestCase {

    private RecordBatchStream mockStream;
    private VectorSchemaRoot mockRoot;

    @Before
    public void setup() {
        mockStream = mock(RecordBatchStream.class);
        mockRoot = mock(VectorSchemaRoot.class);
    }

    public void testHasNextReturnsTrueWhenBatchAvailable() {
        when(mockStream.loadNextBatch()).thenReturn(CompletableFuture.completedFuture(true));

        RecordBatchIterator iterator = new RecordBatchIterator(mockStream);

        assertTrue(iterator.hasNext());
        verify(mockStream, times(1)).loadNextBatch();
    }

    public void testHasNextReturnsFalseWhenNoMoreBatches() {
        when(mockStream.loadNextBatch()).thenReturn(CompletableFuture.completedFuture(false));

        RecordBatchIterator iterator = new RecordBatchIterator(mockStream);

        assertFalse(iterator.hasNext());
        verify(mockStream, times(1)).loadNextBatch();
    }

    public void testHasNextCachesResult() {
        when(mockStream.loadNextBatch()).thenReturn(CompletableFuture.completedFuture(true));

        RecordBatchIterator iterator = new RecordBatchIterator(mockStream);

        iterator.hasNext();
        iterator.hasNext();

        verify(mockStream, times(1)).loadNextBatch();
    }

    public void testNextReturnsVectorSchemaRoot() {
        when(mockStream.loadNextBatch()).thenReturn(CompletableFuture.completedFuture(true));
        when(mockStream.getVectorSchemaRoot()).thenReturn(mockRoot);

        RecordBatchIterator iterator = new RecordBatchIterator(mockStream);

        VectorSchemaRoot result = iterator.next();

        assertSame(mockRoot, result);
        verify(mockStream).getVectorSchemaRoot();
    }

    public void testNextThrowsWhenNoMoreElements() {
        when(mockStream.loadNextBatch()).thenReturn(CompletableFuture.completedFuture(false));

        RecordBatchIterator iterator = new RecordBatchIterator(mockStream);

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    public void testIterateMultipleBatches() {
        when(mockStream.loadNextBatch())
            .thenReturn(CompletableFuture.completedFuture(true))
            .thenReturn(CompletableFuture.completedFuture(true))
            .thenReturn(CompletableFuture.completedFuture(false));
        when(mockStream.getVectorSchemaRoot()).thenReturn(mockRoot);

        RecordBatchIterator iterator = new RecordBatchIterator(mockStream);

        int count = 0;
        while (iterator.hasNext()) {
            assertNotNull(iterator.next());
            count++;
        }

        assertEquals(2, count);
        verify(mockStream, times(3)).loadNextBatch();
        verify(mockStream, times(2)).getVectorSchemaRoot();
    }
}
