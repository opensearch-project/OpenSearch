/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.opensearch.analytics.spi.IndexFilterProvider;
import org.opensearch.analytics.spi.IndexFilterProviderFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Tests the Java-side provider registry + FFM callback dispatch glue without
 * going through the full substrait → native pipeline. The provider-creation
 * callback (which takes a {@code MemorySegment}) is covered separately by
 * tests that exercise the whole FFM entry point.
 */
public class IndexFilterCallbackTests extends OpenSearchTestCase {

    public void testCreateCollectReleaseRoundTrip() {
        MockProvider provider = new MockProvider(new long[] { 0x5L, 0x0L });
        // Register directly, bypassing the factory upcall for test purposes.
        int providerKey = ProviderRegistry.registerProvider(provider);
        try {
            int collectorKey = FilterTreeCallbacks.createCollector(providerKey, 0, 0, 64);
            assertTrue("collectorKey >= 0", collectorKey >= 0);

            long[] buf = new long[1];
            int n = provider.collectDocs(
                ProviderRegistry.collector(collectorKey).innerCollectorKey(),
                0,
                64,
                buf
            );
            assertEquals(1, n);
            assertEquals(0x5L, buf[0]);

            FilterTreeCallbacks.releaseCollector(collectorKey);
            assertNull("collector removed from registry", ProviderRegistry.collector(collectorKey));
        } finally {
            // releaseProvider also closes + unregisters.
            FilterTreeCallbacks.releaseProvider(providerKey);
            assertNull("provider removed from registry", ProviderRegistry.provider(providerKey));
        }
    }

    public void testCreateWithUnknownProviderReturnsError() {
        int missingKey = Integer.MAX_VALUE;
        assertEquals(-1, FilterTreeCallbacks.createCollector(missingKey, 0, 0, 16));
    }

    public void testReleaseWithUnknownCollectorIsSafe() {
        FilterTreeCallbacks.releaseCollector(Integer.MAX_VALUE);
    }

    public void testReleaseWithUnknownProviderIsSafe() {
        FilterTreeCallbacks.releaseProvider(Integer.MAX_VALUE);
    }

    public void testCreateProviderDispatchesToRegisteredFactory() throws IOException {
        // Install a stub factory, call createProvider via the FFM upcall
        // target, and assert the factory was invoked with the expected bytes
        // and the returned key resolves to the stub provider.
        byte[] expected = new byte[] { 1, 2, 3, 4 };
        StubFactory factory = new StubFactory(expected);
        ProviderRegistry.resetFactoryForTesting();
        try {
            ProviderRegistry.setFactory(factory);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment seg = arena.allocate(expected.length);
                MemorySegment.copy(expected, 0, seg, ValueLayout.JAVA_BYTE, 0, expected.length);

                int key = FilterTreeCallbacks.createProvider(seg, expected.length);
                assertTrue("providerKey >= 0", key >= 0);
                assertEquals("factory invoked exactly once", 1, factory.callCount);

                IndexFilterProvider registered = ProviderRegistry.provider(key);
                assertNotNull("provider registered under returned key", registered);
                assertSame("registered provider is the one factory produced", factory.lastProvider, registered);

                FilterTreeCallbacks.releaseProvider(key);
                assertNull(ProviderRegistry.provider(key));
            }
        } finally {
            ProviderRegistry.resetFactoryForTesting();
        }
    }

    public void testCreateProviderWithNoFactoryReturnsError() {
        ProviderRegistry.resetFactoryForTesting();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocate(1);
            int key = FilterTreeCallbacks.createProvider(seg, 1);
            assertEquals("no factory → -1", -1, key);
        }
    }

    /** Stub factory that records its input and emits a MockProvider. */
    private static final class StubFactory implements IndexFilterProviderFactory {
        private final byte[] expectedBytes;
        int callCount = 0;
        IndexFilterProvider lastProvider;

        StubFactory(byte[] expectedBytes) {
            this.expectedBytes = expectedBytes;
        }

        @Override
        public IndexFilterProvider create(byte[] queryBytes) {
            callCount++;
            assertArrayEquals("factory receives the exact bytes from upcall", expectedBytes, queryBytes);
            lastProvider = new MockProvider(new long[] { 0xAL });
            return lastProvider;
        }
    }

    /** In-memory provider that returns canned bitset words. */
    private static final class MockProvider implements IndexFilterProvider {
        private final long[] cannedWords;
        private int nextCollector = 1;

        MockProvider(long[] cannedWords) {
            this.cannedWords = cannedWords;
        }

        @Override
        public int createCollector(int segmentOrd, int minDoc, int maxDoc) {
            return nextCollector++;
        }

        @Override
        public int collectDocs(int collectorKey, int minDoc, int maxDoc, long[] out) {
            int n = Math.min(cannedWords.length, out.length);
            System.arraycopy(cannedWords, 0, out, 0, n);
            return n;
        }

        @Override
        public void releaseCollector(int collectorKey) {}

        @Override
        public void close() throws IOException {}
    }
}
