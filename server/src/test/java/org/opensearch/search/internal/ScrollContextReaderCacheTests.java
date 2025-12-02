/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link ScrollContext}'s StoredFieldsReader caching functionality.
 *
 * The cache stores merge instances of StoredFieldsReader per segment to optimize
 * sequential document access during scroll queries by avoiding repeated decompression
 * of stored field blocks.
 */
public class ScrollContextReaderCacheTests extends OpenSearchTestCase {

    public void testCachePutAndGet() throws IOException {
        ScrollContext scrollContext = new ScrollContext();
        Object segmentKey1 = new Object();
        Object segmentKey2 = new Object();
        StoredFieldsReader reader1 = mock(StoredFieldsReader.class);
        StoredFieldsReader reader2 = mock(StoredFieldsReader.class);
        // Initially no cached readers
        assertNull(scrollContext.getCachedSequentialReader(segmentKey1));
        assertNull(scrollContext.getCachedSequentialReader(segmentKey2));
        // Cache reader for segment 1
        scrollContext.cacheSequentialReader(segmentKey1, reader1);
        assertSame(reader1, scrollContext.getCachedSequentialReader(segmentKey1));
        assertNull(scrollContext.getCachedSequentialReader(segmentKey2));
        // Cache reader for segment 2
        scrollContext.cacheSequentialReader(segmentKey2, reader2);
        assertSame(reader1, scrollContext.getCachedSequentialReader(segmentKey1));
        assertSame(reader2, scrollContext.getCachedSequentialReader(segmentKey2));
        scrollContext.close();
        // Verify both readers were closed
        verify(reader1, times(1)).close();
        verify(reader2, times(1)).close();
        // After close, cache should be cleared
        assertNull(scrollContext.getCachedSequentialReader(segmentKey1));
        assertNull(scrollContext.getCachedSequentialReader(segmentKey2));
    }

    public void testCloseWithEmptyCache() {
        ScrollContext scrollContext = new ScrollContext();
        // Should not throw when closing with no cached readers
        scrollContext.close();
        assertNull(scrollContext.getCachedSequentialReader(new Object()));
    }

    public void testCloseHandlesReaderException() throws IOException {
        ScrollContext scrollContext = new ScrollContext();
        Object segmentKey1 = new Object();
        Object segmentKey2 = new Object();
        StoredFieldsReader reader1 = mock(StoredFieldsReader.class);
        StoredFieldsReader reader2 = mock(StoredFieldsReader.class);
        // Make reader1 throw on close
        doThrow(new IOException("test exception")).when(reader1).close();
        scrollContext.cacheSequentialReader(segmentKey1, reader1);
        scrollContext.cacheSequentialReader(segmentKey2, reader2);
        // Should not throw, should continue closing other readers
        scrollContext.close();
        // Both readers should have been attempted to close
        verify(reader1, times(1)).close();
        verify(reader2, times(1)).close();
    }

    public void testMultipleSegmentsCached() throws IOException {
        ScrollContext scrollContext = new ScrollContext();
        int numSegments = randomIntBetween(3, 10);
        Object[] segmentKeys = new Object[numSegments];
        StoredFieldsReader[] readers = new StoredFieldsReader[numSegments];
        // Cache readers for all segments (simulates first batch per segment)
        for (int i = 0; i < numSegments; i++) {
            segmentKeys[i] = new Object();
            readers[i] = mock(StoredFieldsReader.class);
            assertNull(scrollContext.getCachedSequentialReader(segmentKeys[i]));
            scrollContext.cacheSequentialReader(segmentKeys[i], readers[i]);
        }
        // Simulate multiple scroll batches - all should hit cache
        for (int batch = 2; batch <= 5; batch++) {
            for (int i = 0; i < numSegments; i++) {
                assertSame(
                    "Batch " + batch + ", segment " + i + " should hit cache",
                    readers[i],
                    scrollContext.getCachedSequentialReader(segmentKeys[i])
                );
            }
        }
        scrollContext.close();
        // Verify all readers were closed
        for (int i = 0; i < numSegments; i++) {
            verify(readers[i], times(1)).close();
        }
    }
}
