/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.IndexFilterTree;
import org.opensearch.index.engine.IndexFilterTreeNode;
import org.opensearch.index.engine.exec.FilterTreeCallbackBridge;
import org.opensearch.index.engine.exec.IndexFilterContext;
import org.opensearch.index.engine.exec.IndexFilterTreeContext;
import org.opensearch.index.engine.exec.IndexFilterTreeProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * End-to-end tests for boolean tree query execution through JNI.
 * Verifies: tree build → serialize → register with FilterTreeCallbackBridge →
 * call executeTreeQueryAsync via NativeBridge → Rust parses the tree.
 */
public class TreeQueryExecutionTests extends OpenSearchTestCase {

    private long runtimePtr;
    private long readerPtr;

    private static boolean runtimeInitialized = false;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (runtimeInitialized == false) {
            NativeBridge.initTokioRuntimeManager(2);
            runtimeInitialized = true;
        }
        Path spillDir = createTempDir("datafusion-spill");
        runtimePtr = NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024);

        Path dataDir = createTempDir("datafusion-data");
        Path testParquet = Path.of(getClass().getClassLoader().getResource("test.parquet").toURI());
        Files.copy(testParquet, dataDir.resolve("test.parquet"));
        readerPtr = NativeBridge.createDatafusionReader(dataDir.toString(), new String[] { "test.parquet" });
    }

    @Override
    public void tearDown() throws Exception {
        NativeBridge.closeDatafusionReader(readerPtr);
        NativeBridge.closeGlobalRuntime(runtimePtr);
        super.tearDown();
    }

    // ── End-to-end: tree → serialize → JNI → Rust parses ───────────

    public void testTreeQueryAsyncRustParsesTree() throws Exception {
        // Build a simple tree: AND(CollectorLeaf(0, 0), PredicateLeaf(0))
        IndexFilterTreeNode root = IndexFilterTreeNode.and(
            IndexFilterTreeNode.collectorLeaf(0, 0),
            IndexFilterTreeNode.predicateLeaf(0)
        );
        IndexFilterTree tree = new IndexFilterTree(root, 1);
        byte[] treeBytes = tree.serialize();

        // Create mock provider and register with bridge
        MockIndexFilterContext mockLeafCtx = new MockIndexFilterContext(1, 2);
        IndexFilterTree dummyTree = new IndexFilterTree(IndexFilterTreeNode.predicateLeaf(0), 0);
        IndexFilterTreeContext<MockIndexFilterContext> mockTreeCtx = new IndexFilterTreeContext<>(
            dummyTree,
            Collections.singletonList(mockLeafCtx)
        );
        MockIndexFilterTreeProvider mockProvider = new MockIndexFilterTreeProvider();

        long contextId = FilterTreeCallbackBridge.createContext();
        FilterTreeCallbackBridge.registerProvider(contextId, 0, mockProvider, mockTreeCtx);

        try {
            // Generate substrait bytes
            byte[] substraitBytes = NativeBridge.sqlToSubstrait(
                readerPtr, "test_table", "SELECT message FROM test_table", runtimePtr
            );
            assertNotNull(substraitBytes);
            assertTrue(substraitBytes.length > 0);

            // Call executeTreeQueryAsync — Rust should parse the tree successfully
            // and return a NotImplemented error with "tree parsed OK" in the message
            CompletableFuture<Long> future = new CompletableFuture<>();
            NativeBridge.executeTreeQueryAsync(
                treeBytes,
                contextId,
                new long[0],
                new String[0],
                "test_table",
                substraitBytes,
                1,
                1,
                false,
                runtimePtr,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Long v) {
                        future.complete(v);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            );

            // The Rust side currently returns NotImplemented with "tree parsed OK"
            CompletionException ce = expectThrows(CompletionException.class, future::join);
            assertNotNull(ce.getCause());
            String errorMsg = ce.getCause().getMessage();
            assertTrue(
                "Expected 'tree parsed OK' in error message but got: " + errorMsg,
                errorMsg.contains("tree parsed OK")
            );
        } finally {
            FilterTreeCallbackBridge.unregister(contextId);
        }
    }

    // ── Bridge registration and cleanup lifecycle ───────────────────

    public void testBridgeRegistrationAndCleanup() {
        // Create context
        long contextId = FilterTreeCallbackBridge.createContext();
        assertTrue("contextId should be positive", contextId > 0);

        // Create mock provider and context
        MockIndexFilterContext mockLeafCtx = new MockIndexFilterContext(2, 5);
        IndexFilterTree dummyTree = new IndexFilterTree(IndexFilterTreeNode.predicateLeaf(0), 0);
        IndexFilterTreeContext<MockIndexFilterContext> mockTreeCtx = new IndexFilterTreeContext<>(
            dummyTree,
            Collections.singletonList(mockLeafCtx)
        );
        MockIndexFilterTreeProvider mockProvider = new MockIndexFilterTreeProvider();

        // Register provider
        FilterTreeCallbackBridge.registerProvider(contextId, 0, mockProvider, mockTreeCtx);

        // Verify callbacks route correctly
        int segCount = FilterTreeCallbackBridge.getSegmentCount(contextId, 0, 0);
        assertEquals(2, segCount);

        int maxDoc = FilterTreeCallbackBridge.getSegmentMaxDoc(contextId, 0, 0, 0);
        assertEquals(5, maxDoc);

        // Unregister
        FilterTreeCallbackBridge.unregister(contextId);

        // After unregister, callbacks should return sentinel values
        int sentinelSegCount = FilterTreeCallbackBridge.getSegmentCount(contextId, 0, 0);
        assertEquals(-1, sentinelSegCount);

        int sentinelMaxDoc = FilterTreeCallbackBridge.getSegmentMaxDoc(contextId, 0, 0, 0);
        assertEquals(-1, sentinelMaxDoc);

        long[] sentinelDocs = FilterTreeCallbackBridge.collectDocs(contextId, 0, 0, 0, 0, 10);
        assertEquals(0, sentinelDocs.length);
    }

    // ── Tree serialization round-trip through JNI ───────────────────

    public void testTreeSerializationPassedToRust() throws Exception {
        byte[] substraitBytes = NativeBridge.sqlToSubstrait(
            readerPtr, "test_table", "SELECT message FROM test_table", runtimePtr
        );

        // Test various tree shapes
        IndexFilterTreeNode[] trees = new IndexFilterTreeNode[] {
            // Simple AND
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.predicateLeaf(0)
            ),
            // Simple OR
            IndexFilterTreeNode.or(
                IndexFilterTreeNode.collectorLeaf(0, 0),
                IndexFilterTreeNode.predicateLeaf(0)
            ),
            // NOT wrapping collector
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.not(IndexFilterTreeNode.collectorLeaf(0, 0)),
                IndexFilterTreeNode.predicateLeaf(0)
            ),
            // Nested: AND(OR(Collector, Predicate), Collector)
            IndexFilterTreeNode.and(
                IndexFilterTreeNode.or(
                    IndexFilterTreeNode.collectorLeaf(0, 0),
                    IndexFilterTreeNode.predicateLeaf(0)
                ),
                IndexFilterTreeNode.collectorLeaf(0, 1)
            ),
        };

        for (int i = 0; i < trees.length; i++) {
            IndexFilterTree tree = new IndexFilterTree(trees[i], 1);
            byte[] treeBytes = tree.serialize();

            long contextId = FilterTreeCallbackBridge.createContext();
            MockIndexFilterContext mockLeafCtx = new MockIndexFilterContext(1, 2);
            IndexFilterTree dummyTree = new IndexFilterTree(IndexFilterTreeNode.predicateLeaf(0), 0);
            IndexFilterTreeContext<MockIndexFilterContext> mockTreeCtx = new IndexFilterTreeContext<>(
                dummyTree,
                Collections.singletonList(mockLeafCtx)
            );
            FilterTreeCallbackBridge.registerProvider(contextId, 0, new MockIndexFilterTreeProvider(), mockTreeCtx);

            try {
                CompletableFuture<Long> future = new CompletableFuture<>();
                NativeBridge.executeTreeQueryAsync(
                    treeBytes,
                    contextId,
                    new long[0],
                    new String[0],
                    "test_table",
                    substraitBytes,
                    1,
                    1,
                    false,
                    runtimePtr,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Long v) {
                            future.complete(v);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            future.completeExceptionally(e);
                        }
                    }
                );

                // Rust should parse the tree OK — error message should NOT say "Failed to deserialize"
                CompletionException ce = expectThrows(CompletionException.class, future::join);
                String errorMsg = ce.getCause().getMessage();
                assertFalse(
                    "Tree " + i + " should deserialize OK but got: " + errorMsg,
                    errorMsg.contains("Failed to deserialize")
                );
                assertTrue(
                    "Tree " + i + " should contain 'tree parsed OK' but got: " + errorMsg,
                    errorMsg.contains("tree parsed OK")
                );
            } finally {
                FilterTreeCallbackBridge.unregister(contextId);
            }
        }
    }

    // ── Async helper ────────────────────────────────────────────────

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) {
                future.complete(v);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future.join();
    }

    // ── Mock implementations ────────────────────────────────────────

    /** Simple mock IndexFilterContext with fixed segment count and max doc. */
    private static class MockIndexFilterContext implements IndexFilterContext {
        private final int segmentCount;
        private final int segmentMaxDoc;

        MockIndexFilterContext(int segmentCount, int segmentMaxDoc) {
            this.segmentCount = segmentCount;
            this.segmentMaxDoc = segmentMaxDoc;
        }

        @Override
        public int segmentCount() {
            return segmentCount;
        }

        @Override
        public int segmentMaxDoc(int segmentOrd) {
            return segmentMaxDoc;
        }

        @Override
        public void close() throws IOException {}
    }

    /** Mock IndexFilterTreeProvider that returns no-op results. */
    @SuppressWarnings("rawtypes")
    private static class MockIndexFilterTreeProvider implements IndexFilterTreeProvider {

        @Override
        public IndexFilterTreeContext createTreeContext(Object[] queries, Object reader, IndexFilterTree tree) {
            return null; // not used in these tests
        }

        @Override
        public int createCollector(IndexFilterTreeContext treeContext, int leafIndex, int segmentOrd, int minDoc, int maxDoc) {
            return 0;
        }

        @Override
        public long[] collectDocs(IndexFilterTreeContext treeContext, int leafIndex, int collectorKey, int minDoc, int maxDoc) {
            return new long[0];
        }

        @Override
        public void releaseCollector(IndexFilterTreeContext treeContext, int leafIndex, int collectorKey) {}

        @Override
        public void close() throws IOException {}
    }
}
