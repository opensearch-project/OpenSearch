/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.index.engine.IndexFilterTreeNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for {@link FilterTreeCallbackBridge}.
 *
 * Covers: context creation, provider registration, JNI callback routing,
 * sentinel values for unregistered contexts, unregister cleanup, and
 * thread-safety under concurrent access.
 */
public class FilterTreeCallbackBridgeTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    // ── Context creation ────────────────────────────────────────────

    public void testCreateContextReturnsUniqueIds() {
        long id1 = FilterTreeCallbackBridge.createContext();
        long id2 = FilterTreeCallbackBridge.createContext();
        long id3 = FilterTreeCallbackBridge.createContext();

        assertNotEquals(id1, id2);
        assertNotEquals(id2, id3);
        assertNotEquals(id1, id3);

        // Cleanup
        FilterTreeCallbackBridge.unregister(id1);
        FilterTreeCallbackBridge.unregister(id2);
        FilterTreeCallbackBridge.unregister(id3);
    }

    // ── Registration and callback routing ───────────────────────────

    public void testRegisterProviderAndRouteCallbacks() throws IOException {
        long contextId = FilterTreeCallbackBridge.createContext();
        try {
            MockIndexFilterTreeProvider provider = new MockIndexFilterTreeProvider(3, 1000);
            IndexFilterTreeContext<MockIndexFilterContext> treeContext = createMockTreeContext(provider, 2);

            FilterTreeCallbackBridge.registerProvider(contextId, 0, provider, treeContext);

            // getSegmentCount should route to the provider's context
            int segCount = FilterTreeCallbackBridge.getSegmentCount(contextId, 0, 0);
            assertEquals(3, segCount);

            // getSegmentMaxDoc should route correctly
            int maxDoc = FilterTreeCallbackBridge.getSegmentMaxDoc(contextId, 0, 0, 0);
            assertEquals(1000, maxDoc);

            // createCollector should delegate to provider
            int collectorKey = FilterTreeCallbackBridge.createCollector(contextId, 0, 0, 0, 0, 500);
            assertTrue(collectorKey >= 0);

            // collectDocs should return data
            long[] docs = FilterTreeCallbackBridge.collectDocs(contextId, 0, 0, collectorKey, 0, 500);
            assertNotNull(docs);

            // releaseCollector should not throw
            FilterTreeCallbackBridge.releaseCollector(contextId, 0, 0, collectorKey);
        } finally {
            FilterTreeCallbackBridge.unregister(contextId);
        }
    }

    // ── Multi-provider routing ──────────────────────────────────────

    public void testMultipleProvidersUnderOneContext() throws IOException {
        long contextId = FilterTreeCallbackBridge.createContext();
        try {
            MockIndexFilterTreeProvider provider0 = new MockIndexFilterTreeProvider(2, 500);
            MockIndexFilterTreeProvider provider1 = new MockIndexFilterTreeProvider(4, 2000);

            IndexFilterTreeContext<MockIndexFilterContext> ctx0 = createMockTreeContext(provider0, 1);
            IndexFilterTreeContext<MockIndexFilterContext> ctx1 = createMockTreeContext(provider1, 1);

            FilterTreeCallbackBridge.registerProvider(contextId, 0, provider0, ctx0);
            FilterTreeCallbackBridge.registerProvider(contextId, 1, provider1, ctx1);

            // Provider 0: 2 segments, maxDoc 500
            assertEquals(2, FilterTreeCallbackBridge.getSegmentCount(contextId, 0, 0));
            assertEquals(500, FilterTreeCallbackBridge.getSegmentMaxDoc(contextId, 0, 0, 0));

            // Provider 1: 4 segments, maxDoc 2000
            assertEquals(4, FilterTreeCallbackBridge.getSegmentCount(contextId, 1, 0));
            assertEquals(2000, FilterTreeCallbackBridge.getSegmentMaxDoc(contextId, 1, 0, 0));
        } finally {
            FilterTreeCallbackBridge.unregister(contextId);
        }
    }

    // ── Sentinel values for unregistered contexts ───────────────────

    public void testSentinelValuesForUnregisteredContextId() {
        long bogusContextId = Long.MAX_VALUE - 999;

        assertEquals(-1, FilterTreeCallbackBridge.getSegmentCount(bogusContextId, 0, 0));
        assertEquals(-1, FilterTreeCallbackBridge.getSegmentMaxDoc(bogusContextId, 0, 0, 0));
        assertEquals(-1, FilterTreeCallbackBridge.createCollector(bogusContextId, 0, 0, 0, 0, 100));
        assertArrayEquals(new long[0], FilterTreeCallbackBridge.collectDocs(bogusContextId, 0, 0, 0, 0, 100));
        // releaseCollector should not throw for unregistered context
        FilterTreeCallbackBridge.releaseCollector(bogusContextId, 0, 0, 0);
    }

    public void testSentinelValuesForUnregisteredProviderId() throws IOException {
        long contextId = FilterTreeCallbackBridge.createContext();
        try {
            MockIndexFilterTreeProvider provider = new MockIndexFilterTreeProvider(2, 500);
            IndexFilterTreeContext<MockIndexFilterContext> ctx = createMockTreeContext(provider, 1);
            FilterTreeCallbackBridge.registerProvider(contextId, 0, provider, ctx);

            // Provider 0 exists, provider 99 does not
            assertEquals(2, FilterTreeCallbackBridge.getSegmentCount(contextId, 0, 0));
            assertEquals(-1, FilterTreeCallbackBridge.getSegmentCount(contextId, 99, 0));
            assertEquals(-1, FilterTreeCallbackBridge.getSegmentMaxDoc(contextId, 99, 0, 0));
            assertEquals(-1, FilterTreeCallbackBridge.createCollector(contextId, 99, 0, 0, 0, 100));
            assertArrayEquals(new long[0], FilterTreeCallbackBridge.collectDocs(contextId, 99, 0, 0, 0, 100));
        } finally {
            FilterTreeCallbackBridge.unregister(contextId);
        }
    }

    // ── Unregister cleanup ──────────────────────────────────────────

    public void testUnregisterRemovesAllProviders() throws IOException {
        long contextId = FilterTreeCallbackBridge.createContext();

        MockIndexFilterTreeProvider provider = new MockIndexFilterTreeProvider(3, 1000);
        IndexFilterTreeContext<MockIndexFilterContext> ctx = createMockTreeContext(provider, 1);
        FilterTreeCallbackBridge.registerProvider(contextId, 0, provider, ctx);

        // Before unregister: works
        assertEquals(3, FilterTreeCallbackBridge.getSegmentCount(contextId, 0, 0));

        // Unregister
        FilterTreeCallbackBridge.unregister(contextId);

        // After unregister: sentinel values
        assertEquals(-1, FilterTreeCallbackBridge.getSegmentCount(contextId, 0, 0));
        assertEquals(-1, FilterTreeCallbackBridge.getSegmentMaxDoc(contextId, 0, 0, 0));
    }

    public void testUnregisterNonexistentContextDoesNotThrow() {
        // Should be a no-op
        FilterTreeCallbackBridge.unregister(Long.MAX_VALUE - 12345);
    }

    // ── Register without createContext throws ───────────────────────

    public void testRegisterProviderWithoutCreateContextThrows() {
        long bogusContextId = Long.MAX_VALUE - 54321;
        MockIndexFilterTreeProvider provider = new MockIndexFilterTreeProvider(1, 100);

        expectThrows(IllegalStateException.class, () -> FilterTreeCallbackBridge.registerProvider(
            bogusContextId, 0, provider, createMockTreeContext(provider, 1)
        ));
    }

    // ── Concurrent access ───────────────────────────────────────────

    public void testConcurrentCreateAndUnregister() throws InterruptedException {
        int threadCount = 10;
        int opsPerThread = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong errorCount = new AtomicLong(0);

        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        long id = FilterTreeCallbackBridge.createContext();
                        MockIndexFilterTreeProvider provider = new MockIndexFilterTreeProvider(1, 100);
                        FilterTreeCallbackBridge.registerProvider(id, 0, provider, createMockTreeContext(provider, 1));

                        // Verify routing works
                        int seg = FilterTreeCallbackBridge.getSegmentCount(id, 0, 0);
                        if (seg != 1) {
                            errorCount.incrementAndGet();
                        }

                        FilterTreeCallbackBridge.unregister(id);

                        // Verify sentinel after unregister
                        int segAfter = FilterTreeCallbackBridge.getSegmentCount(id, 0, 0);
                        if (segAfter != -1) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        startLatch.countDown();
        doneLatch.await();
        assertEquals("Expected no errors during concurrent access", 0, errorCount.get());
    }

    // ── Mock implementations ────────────────────────────────────────

    private IndexFilterTreeContext<MockIndexFilterContext> createMockTreeContext(
        MockIndexFilterTreeProvider provider, int leafCount
    ) {
        IndexFilterTreeNode root = leafCount >= 2
            ? IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.collectorLeaf(0, 1)
            )
            : IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.predicateLeaf(0)
            );
        IndexFilterTree tree = new IndexFilterTree(root, leafCount);

        List<MockIndexFilterContext> leafContexts = new ArrayList<>();
        for (int i = 0; i < leafCount; i++) {
            leafContexts.add(new MockIndexFilterContext(provider.segmentCount, provider.maxDoc));
        }
        return new IndexFilterTreeContext<>(tree, leafContexts);
    }

    /**
     * Mock IndexFilterContext that returns configurable segment metadata.
     */
    static class MockIndexFilterContext implements IndexFilterContext {
        private final int segmentCount;
        private final int maxDoc;

        MockIndexFilterContext(int segmentCount, int maxDoc) {
            this.segmentCount = segmentCount;
            this.maxDoc = maxDoc;
        }

        @Override
        public int segmentCount() {
            return segmentCount;
        }

        @Override
        public int segmentMaxDoc(int segmentOrd) {
            return maxDoc;
        }

        @Override
        public void close() {}
    }

    /**
     * Mock IndexFilterTreeProvider that creates MockIndexFilterContexts
     * and tracks collector operations.
     */
    @SuppressWarnings("rawtypes")
    static class MockIndexFilterTreeProvider implements IndexFilterTreeProvider {
        final int segmentCount;
        final int maxDoc;
        private int nextCollectorKey = 1;

        MockIndexFilterTreeProvider(int segmentCount, int maxDoc) {
            this.segmentCount = segmentCount;
            this.maxDoc = maxDoc;
        }

        @Override
        public IndexFilterTreeContext createTreeContext(Object[] queries, Object reader, IndexFilterTree tree) {
            List<MockIndexFilterContext> contexts = new ArrayList<>();
            for (int i = 0; i < tree.collectorLeafCount(); i++) {
                contexts.add(new MockIndexFilterContext(segmentCount, maxDoc));
            }
            return new IndexFilterTreeContext<>(tree, contexts);
        }

        @Override
        public int createCollector(IndexFilterTreeContext treeContext, int leafIndex, int segmentOrd, int minDoc, int maxDoc) {
            return nextCollectorKey++;
        }

        @Override
        public long[] collectDocs(IndexFilterTreeContext treeContext, int leafIndex, int collectorKey, int minDoc, int maxDoc) {
            // Return a simple bitset with first bit set
            return new long[] { 1L };
        }

        @Override
        public void releaseCollector(IndexFilterTreeContext treeContext, int leafIndex, int collectorKey) {
            // no-op
        }

        @Override
        public void close() {}
    }
}
