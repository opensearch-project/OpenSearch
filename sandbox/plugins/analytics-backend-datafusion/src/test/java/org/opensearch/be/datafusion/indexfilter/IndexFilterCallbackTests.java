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
 *
 * <p>All callbacks now receive a {@code contextId} as their first argument.
 * Tests use {@code contextId=0} via {@link FilterTreeCallbacks#register}.
 */
public class IndexFilterCallbackTests extends OpenSearchTestCase {

    private static final long CTX = 0L;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FilterTreeCallbacks.unregister(CTX);
    }

    @Override
    public void tearDown() throws Exception {
        FilterTreeCallbacks.unregister(CTX);
        super.tearDown();
    }

    public void testFullRoundTrip() {
        long[] cannedWords = new long[] { 0x5L, 0x3L };
        MockHandle handle = new MockHandle(cannedWords);
        FilterTreeCallbacks.register(CTX, handle, null);

        // createProvider
        int providerKey = FilterTreeCallbacks.createProvider(CTX, 42);
        assertTrue("providerKey >= 0", providerKey >= 0);
        assertEquals("handle received annotationId", 42, handle.lastAnnotationId);

        // createCollector
        int collectorKey = FilterTreeCallbacks.createCollector(CTX, providerKey, 2L, 0, 128);
        assertTrue("collectorKey >= 0", collectorKey >= 0);
        assertEquals("handle received providerKey", providerKey, handle.lastProviderKey);
        assertEquals("handle received writerGeneration", 2L, handle.lastWriterGeneration);
        assertEquals("handle received minDoc", 0, handle.lastMinDoc);
        assertEquals("handle received maxDoc", 128, handle.lastMaxDoc);

        // collectDocs
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(Long.BYTES * 2);
            long wordsWritten = FilterTreeCallbacks.collectDocs(CTX, collectorKey, 0, 128, buf, 2);
            assertEquals("wordsWritten matches canned length", 2L, wordsWritten);
            assertEquals(0x5L, buf.getAtIndex(ValueLayout.JAVA_LONG, 0));
            assertEquals(0x3L, buf.getAtIndex(ValueLayout.JAVA_LONG, 1));
        }

        // releaseCollector
        FilterTreeCallbacks.releaseCollector(CTX, collectorKey);
        assertEquals("handle received collectorKey for release", collectorKey, handle.lastReleasedCollectorKey);

        // releaseProvider
        FilterTreeCallbacks.releaseProvider(CTX, providerKey);
        assertEquals("handle received providerKey for release", providerKey, handle.lastReleasedProviderKey);
    }

    /**
     * Lifecycle assertion: invoking an upcall on an unregistered contextId trips
     * {@code assert binding != null}. With {@code -ea} on (test default), this throws
     * AssertionError rather than silently returning -1 — surfacing missing-register
     * or premature-unregister bugs.
     */
    public void testUnregisteredContextIdAsserts() {
        FilterTreeCallbacks.unregister(CTX);
        expectThrows(AssertionError.class, () -> FilterTreeCallbacks.createProvider(CTX, 1));
        expectThrows(AssertionError.class, () -> FilterTreeCallbacks.createCollector(CTX, 1, 0L, 0, 64));
        expectThrows(AssertionError.class, () -> {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                FilterTreeCallbacks.collectDocs(CTX, 1, 0, 64, buf, 1);
            }
        });
        expectThrows(AssertionError.class, () -> FilterTreeCallbacks.releaseCollector(CTX, Integer.MAX_VALUE));
        expectThrows(AssertionError.class, () -> FilterTreeCallbacks.releaseProvider(CTX, Integer.MAX_VALUE));
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
            public long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
                return -1;
            }

            @Override
            public void releaseCollector(int collectorKey) {}

            @Override
            public void releaseProvider(int providerKey) {}

            @Override
            public void close() {}
        };
        FilterTreeCallbacks.register(CTX, failingHandle, null);

        assertEquals(-1, FilterTreeCallbacks.createProvider(CTX, 1));
        assertEquals(-1, FilterTreeCallbacks.createCollector(CTX, 1, 0L, 0, 64));
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(Long.BYTES);
            assertEquals(-1L, FilterTreeCallbacks.collectDocs(CTX, 1, 0, 64, buf, 1));
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
        public long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
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
