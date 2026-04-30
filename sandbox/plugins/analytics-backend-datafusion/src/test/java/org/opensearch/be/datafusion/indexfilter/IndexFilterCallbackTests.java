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
 * Tests the Java-side provider/collector registries + FFM callback dispatch
 * glue without going through the full substrait → native pipeline.
 */
public class IndexFilterCallbackTests extends OpenSearchTestCase {

    private FilterProviderRegistry providers;
    private CollectorRegistry collectors;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        collectors = new CollectorRegistry();
        providers = new FilterProviderRegistry(collectors);
        FilterTreeCallbacks.setRegistries(providers, collectors);
    }

    @Override
    public void tearDown() throws Exception {
        FilterTreeCallbacks.setRegistries(null, null);
        super.tearDown();
    }

    public void testCreateCollectReleaseRoundTrip() {
        MockProvider provider = new MockProvider(new long[] { 0x5L, 0x0L });
        // Register provider directly via lifecycle, bypassing factory upcall.
        int providerKey = providers.createProvider(new byte[0]);
        // That returns -1 because no factory is set. Register manually instead.
        providerKey = registerProviderDirectly(provider);

        try (Arena arena = Arena.ofConfined()) {
            int collectorKey = FilterTreeCallbacks.createCollector(providerKey, 0, 0, 64);
            assertTrue("collectorKey >= 0", collectorKey >= 0);

            MemorySegment buf = arena.allocate(Long.BYTES);
            int n = provider.collectDocs(
                collectors.collector(collectorKey).innerCollectorKey(),
                0,
                64,
                buf
            );
            assertEquals(1, n);
            assertEquals(0x5L, buf.getAtIndex(ValueLayout.JAVA_LONG, 0));

            FilterTreeCallbacks.releaseCollector(collectorKey);
            assertNull("collector removed from registry", collectors.collector(collectorKey));
        } finally {
            FilterTreeCallbacks.releaseProvider(providerKey);
            assertNull("provider removed from registry", providers.provider(providerKey));
        }
    }

    public void testCreateWithUnknownProviderReturnsError() {
        assertEquals(-1, FilterTreeCallbacks.createCollector(Integer.MAX_VALUE, 0, 0, 16));
    }

    public void testReleaseWithUnknownCollectorIsSafe() {
        FilterTreeCallbacks.releaseCollector(Integer.MAX_VALUE);
    }

    public void testReleaseWithUnknownProviderIsSafe() {
        FilterTreeCallbacks.releaseProvider(Integer.MAX_VALUE);
    }

    public void testCreateProviderDispatchesToRegisteredFactory() throws IOException {
        byte[] expected = new byte[] { 1, 2, 3, 4 };
        StubFactory factory = new StubFactory(expected);
        providers.setFactory(factory);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocate(expected.length);
            MemorySegment.copy(expected, 0, seg, ValueLayout.JAVA_BYTE, 0, expected.length);

            int key = FilterTreeCallbacks.createProvider(seg, expected.length);
            assertTrue("providerKey >= 0", key >= 0);
            assertEquals("factory invoked exactly once", 1, factory.callCount);

            IndexFilterProvider registered = providers.provider(key);
            assertNotNull("provider registered under returned key", registered);
            assertSame("registered provider is the one factory produced", factory.lastProvider, registered);

            FilterTreeCallbacks.releaseProvider(key);
            assertNull(providers.provider(key));
        }
    }

    public void testCreateProviderWithNoFactoryReturnsError() {
        // Fresh lifecycle with no factory set.
        CollectorRegistry emptyColl = new CollectorRegistry();
        FilterProviderRegistry empty = new FilterProviderRegistry(emptyColl);
        FilterTreeCallbacks.setRegistries(empty, emptyColl);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocate(1);
            int key = FilterTreeCallbacks.createProvider(seg, 1);
            assertEquals("no factory → -1", -1, key);
        }
    }

    /**
     * Helper: register a provider directly into the lifecycle's internal map
     * for tests that bypass the factory. Uses reflection-free approach by
     * setting a factory that returns the given provider, calling createProvider,
     * then the factory is consumed.
     */
    private int registerProviderDirectly(IndexFilterProvider provider) {
        // Use a one-shot factory that returns the given provider.
        FilterProviderRegistry directLifecycle = new FilterProviderRegistry(collectors);
        directLifecycle.setFactory(bytes -> provider);
        int key = directLifecycle.createProvider(new byte[0]);
        // Swap the lifecycle so FilterTreeCallbacks sees this provider.
        // We need to keep the collectors registry.
        this.providers = directLifecycle;
        FilterTreeCallbacks.setRegistries(directLifecycle, collectors);
        return key;
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
        public int collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
            int n = Math.min(cannedWords.length, (int) (out.byteSize() / Long.BYTES));
            for (int i = 0; i < n; i++) {
                out.setAtIndex(ValueLayout.JAVA_LONG, i, cannedWords[i]);
            }
            return n;
        }

        @Override
        public void releaseCollector(int collectorKey) {}

        @Override
        public void close() throws IOException {}
    }
}
