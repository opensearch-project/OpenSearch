/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Tests the Java-side FFM callback dispatch via {@link FilterTreeCallbacks}
 * routing to a {@link FilterDelegationHandle} without going through the full
 * substrait → native pipeline.
 */
public class IndexFilterCallbackTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FilterTreeCallbacks.setHandle(null);
    }

    @Override
    public void tearDown() throws Exception {
        FilterTreeCallbacks.setHandle(null);
        super.tearDown();
    }

    public void testFullRoundTrip() {
        long[] cannedWords = new long[] { 0x5L, 0x3L };
        MockHandle handle = new MockHandle(cannedWords);
        FilterTreeCallbacks.setHandle(handle);

        // createProvider
        int providerKey = FilterTreeCallbacks.createProvider(42);
        assertTrue("providerKey >= 0", providerKey >= 0);
        assertEquals("handle received annotationId", 42, handle.lastAnnotationId);

        // createCollector
        int collectorKey = FilterTreeCallbacks.createCollector(providerKey, 2L, 0, 128);
        assertTrue("collectorKey >= 0", collectorKey >= 0);
        assertEquals("handle received providerKey", providerKey, handle.lastProviderKey);
        assertEquals("handle received writerGeneration", 2L, handle.lastWriterGeneration);
        assertEquals("handle received minDoc", 0, handle.lastMinDoc);
        assertEquals("handle received maxDoc", 128, handle.lastMaxDoc);

        // collectDocs
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(Long.BYTES * 2);
            long wordsWritten = FilterTreeCallbacks.collectDocs(collectorKey, 0, 128, buf, 2);
            assertEquals("wordsWritten matches canned length", 2L, wordsWritten);
            assertEquals(0x5L, buf.getAtIndex(ValueLayout.JAVA_LONG, 0));
            assertEquals(0x3L, buf.getAtIndex(ValueLayout.JAVA_LONG, 1));
        }

        // releaseCollector
        FilterTreeCallbacks.releaseCollector(collectorKey);
        assertEquals("handle received collectorKey for release", collectorKey, handle.lastReleasedCollectorKey);

        // releaseProvider
        FilterTreeCallbacks.releaseProvider(providerKey);
        assertEquals("handle received providerKey for release", providerKey, handle.lastReleasedProviderKey);
    }

    public void testNoHandleReturnsNegativeOne() {
        FilterTreeCallbacks.setHandle(null);
        assertEquals(-1, FilterTreeCallbacks.createProvider(1));
        assertEquals(-1, FilterTreeCallbacks.createCollector(1, 0L, 0, 64));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(Long.BYTES);
            assertEquals(-1L, FilterTreeCallbacks.collectDocs(1, 0, 64, buf, 1));
        }
    }

    public void testReleaseWithNoHandleIsSafe() {
        FilterTreeCallbacks.setHandle(null);
        FilterTreeCallbacks.releaseCollector(Integer.MAX_VALUE);
        FilterTreeCallbacks.releaseProvider(Integer.MAX_VALUE);
    }

    public void testHandleReturningNegativeOnePropagates() {
        FilterDelegationHandle failingHandle = new FilterDelegationHandle() {
            @Override
            public int createProvider(int annotationId) {
                return -1;
            }

            @Override
            public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
                return -1;
            }

            @Override
            public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
                return -1;
            }

            @Override
            public void releaseCollector(int collectorKey) {}

            @Override
            public void releaseProvider(int providerKey) {}

            @Override
            public void close() {}
        };
        FilterTreeCallbacks.setHandle(failingHandle);

        assertEquals(-1, FilterTreeCallbacks.createProvider(1));
        assertEquals(-1, FilterTreeCallbacks.createCollector(1, 0L, 0, 64));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(Long.BYTES);
            assertEquals(-1L, FilterTreeCallbacks.collectDocs(1, 0, 64, buf, 1));
        }
    }

    /** Mock handle that records arguments and returns canned bitset words. */
    private static final class MockHandle implements FilterDelegationHandle {
        private final long[] cannedWords;
        private int nextKey = 1;

        int lastAnnotationId = -1;
        int lastProviderKey = -1;
        long lastWriterGeneration = -1L;
        int lastMinDoc = -1;
        int lastMaxDoc = -1;
        int lastCollectorKey = -1;
        int lastReleasedCollectorKey = -1;
        int lastReleasedProviderKey = -1;

        MockHandle(long[] cannedWords) {
            this.cannedWords = cannedWords;
        }

        @Override
        public int createProvider(int annotationId) {
            this.lastAnnotationId = annotationId;
            return nextKey++;
        }

        @Override
        public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
            this.lastProviderKey = providerKey;
            this.lastWriterGeneration = writerGeneration;
            this.lastMinDoc = minDoc;
            this.lastMaxDoc = maxDoc;
            return nextKey++;
        }

        @Override
        public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
            this.lastCollectorKey = collectorKey;
            int wordCount = Math.min(cannedWords.length, (int) (out.byteSize() / Long.BYTES));
            for (int i = 0; i < wordCount; i++) {
                out.setAtIndex(ValueLayout.JAVA_LONG, i, cannedWords[i]);
            }
            return wordCount;
        }

        @Override
        public void releaseCollector(int collectorKey) {
            this.lastReleasedCollectorKey = collectorKey;
        }

        @Override
        public void releaseProvider(int providerKey) {
            this.lastReleasedProviderKey = providerKey;
        }

        @Override
        public void close() {}
    }
}
