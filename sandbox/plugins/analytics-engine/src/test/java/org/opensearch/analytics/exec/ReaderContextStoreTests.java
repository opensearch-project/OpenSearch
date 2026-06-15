/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

public class ReaderContextStoreTests extends OpenSearchTestCase {

    private static final ShardId SHARD_0 = new ShardId(new Index("idx", "uuid"), 0);
    private static final ShardId SHARD_1 = new ShardId(new Index("idx", "uuid"), 1);

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    private GatedCloseable<Reader> mockGatedReader(AtomicBoolean closedFlag) {
        Reader reader = mock(Reader.class);
        return new GatedCloseable<>(reader, () -> closedFlag.set(true));
    }

    public void testCreateAndAcquireContext() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        GatedCloseable<Reader> gated = mockGatedReader(closed);
        ReaderContext ctx = store.createContext("q1", SHARD_0, gated);

        assertNotNull(ctx);
        assertEquals(1, store.activeCount());

        // Release from query phase
        store.releaseContext("q1", SHARD_0);

        // Acquire for fetch phase
        ReaderContext fetched = store.acquireContext("q1", SHARD_0);
        assertNotNull("Should acquire for fetch", fetched);
        assertSame(ctx.getReader(), fetched.getReader());
    }

    public void testAcquireNonExistentReturnsNull() {
        ReaderContextStore store = new ReaderContextStore(threadPool);

        assertNull(store.acquireContext("no-such-query", SHARD_0));
    }

    public void testAcquireWhileInUseSucceeds() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        ReaderContext created = store.createContext("q1", SHARD_0, mockGatedReader(closed));
        // Already in use by the query phase; the fetch phase must still be able to acquire it.
        ReaderContext acquired = store.acquireContext("q1", SHARD_0);
        assertNotNull("Should acquire even while query phase still in-use", acquired);
        assertSame(created.getReader(), acquired.getReader());
    }

    public void testFreeContextClosesReader() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        store.releaseContext("q1", SHARD_0);
        store.freeContext("q1", SHARD_0);

        assertTrue("Reader should be closed after freeContext", closed.get());
        assertEquals(0, store.activeCount());
    }

    public void testReleaseAndFreeClosesReaderInUse() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        // createContext leaves the context in-use (refcount = base + this phase). releaseAndFree
        // must drop both refs and close the reader in one call.
        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        store.releaseAndFree("q1", SHARD_0);

        assertTrue("Reader should be closed after releaseAndFree", closed.get());
        assertEquals(0, store.activeCount());
    }

    public void testFreeContextRemovesFromStore() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        store.releaseContext("q1", SHARD_0);
        store.freeContext("q1", SHARD_0);

        assertNull("Should not find after free", store.getContext("q1", SHARD_0));
    }

    public void testReaperCleansExpiredContexts() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 50); // 50ms keepAlive

        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        store.releaseContext("q1", SHARD_0);

        // Wait for expiry + reaper cycle
        assertBusy(() -> {
            assertTrue("Reader should be closed by reaper", closed.get());
            assertEquals(0, store.activeCount());
        });
    }

    public void testReaperDoesNotCleanInUseContext() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 1); // 1ms keepAlive

        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        // Still in-use (not released)

        Thread.sleep(50);
        assertFalse("Reaper should not close in-use context", closed.get());
        assertEquals(1, store.activeCount());

        // Cleanup
        store.releaseContext("q1", SHARD_0);
        store.freeContext("q1", SHARD_0);
    }

    /**
     * Query and fetch hold the context at the same time; the reaper must wait for both to finish
     * before closing the reader, even with a tiny keepAlive.
     */
    public void testReaperWaitsForOverlappingQueryAndFetch() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 1); // 1ms keepAlive

        // Query in use; fetch acquires while query is still in use.
        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        assertNotNull("Fetch must acquire while query still in-use", store.acquireContext("q1", SHARD_0));

        // Query finishes, fetch still running: reader must stay open past keepAlive.
        store.releaseContext("q1", SHARD_0);
        Thread.sleep(50);
        assertFalse("Reader must stay open while fetch still in-use", closed.get());
        assertEquals(1, store.activeCount());

        // Fetch finishes: now reapable.
        store.releaseContext("q1", SHARD_0);
        assertBusy(() -> {
            assertTrue("Reader closed once both phases released", closed.get());
            assertEquals(0, store.activeCount());
        });
    }

    /**
     * Freeing a context while a phase is still using it must not close the reader immediately;
     * the close is deferred until that phase finishes.
     */
    public void testFreeContextDefersCloseUntilLastPhaseDone() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        ReaderContext ctx = store.createContext("q1", SHARD_0, mockGatedReader(closed));
        assertNotNull(store.acquireContext("q1", SHARD_0));
        store.releaseContext("q1", SHARD_0);

        store.freeContext("q1", SHARD_0);
        assertFalse("Reader must stay open while a phase is still using the context", closed.get());
        assertEquals("Context removed from store on free", 0, store.activeCount());

        ctx.markDone();
        assertTrue("Reader closed once the last phase finishes", closed.get());
    }

    /**
     * A query-phase failure releases the query's use-reference but leaves the context registered;
     * the refcount returns to the base ref, so the reaper closes it once keepAlive elapses.
     */
    public void testReaperClosesContextAfterQueryPhaseFailure() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 50); // 50ms keepAlive

        // Query phase started (createContext marks in-use), then failed -> release without fetch.
        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        store.releaseContext("q1", SHARD_0);

        assertBusy(() -> {
            assertTrue("Reaper must close the reader after a query-phase failure", closed.get());
            assertEquals(0, store.activeCount());
        });
    }

    /**
     * A fetch-phase failure that releases both the fetch and the base reference (eager free) closes
     * the reader immediately rather than leaking it to the reaper.
     */
    public void testFetchPhaseFailureFreesReaderImmediately() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", SHARD_0, mockGatedReader(closed)); // query phase
        store.releaseContext("q1", SHARD_0);                         // query done
        assertNotNull(store.acquireContext("q1", SHARD_0));          // fetch acquires

        // Fetch fails: release this acquire, then free the base ref (mirrors drainFetchByRowIds error paths).
        store.releaseContext("q1", SHARD_0);
        store.freeContext("q1", SHARD_0);

        assertTrue("Reader must be closed immediately on fetch-phase failure", closed.get());
        assertEquals(0, store.activeCount());
    }

    public void testMultipleContexts() {
        AtomicBoolean closed1 = new AtomicBoolean(false);
        AtomicBoolean closed2 = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", SHARD_0, mockGatedReader(closed1));
        store.createContext("q2", SHARD_1, mockGatedReader(closed2));
        assertEquals(2, store.activeCount());

        store.releaseContext("q1", SHARD_0);
        store.freeContext("q1", SHARD_0);
        assertEquals(1, store.activeCount());
        assertTrue(closed1.get());
        assertFalse(closed2.get());

        store.releaseContext("q2", SHARD_1);
        store.freeContext("q2", SHARD_1);
        assertEquals(0, store.activeCount());
        assertTrue(closed2.get());
    }

    public void testQueryThenFetchLifecycle() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        // Query phase: create + use
        store.createContext("q1", SHARD_0, mockGatedReader(closed));
        ReaderContext ctx = store.getContext("q1", SHARD_0);
        assertNotNull(ctx);
        assertNotNull(ctx.getReader());

        // Query done
        store.releaseContext("q1", SHARD_0);
        assertFalse("Reader stays open between phases", closed.get());

        // Fetch phase: acquire + use
        ReaderContext fetchCtx = store.acquireContext("q1", SHARD_0);
        assertNotNull(fetchCtx);
        assertSame(ctx.getReader(), fetchCtx.getReader());

        // Fetch done: free
        fetchCtx.markDone();
        store.freeContext("q1", SHARD_0);
        assertTrue("Reader closed after fetch", closed.get());
    }
}
