/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

public class ReaderContextStoreTests extends OpenSearchTestCase {

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
        ReaderContext ctx = store.createContext("q1", gated);

        assertNotNull(ctx);
        assertEquals(1, store.activeCount());

        // Release from query phase
        store.releaseContext("q1");

        // Acquire for fetch phase
        ReaderContext fetched = store.acquireContext("q1");
        assertNotNull("Should acquire for fetch", fetched);
        assertSame(ctx.getReader(), fetched.getReader());
    }

    public void testAcquireNonExistentReturnsNull() {
        ReaderContextStore store = new ReaderContextStore(threadPool);

        assertNull(store.acquireContext("no-such-query"));
    }

    public void testAcquireWhileInUseReturnsNull() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", mockGatedReader(closed));
        // Context is already in-use from createContext

        assertNull("Should not acquire while in-use", store.acquireContext("q1"));
    }

    public void testFreeContextClosesReader() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", mockGatedReader(closed));
        store.releaseContext("q1");
        store.freeContext("q1");

        assertTrue("Reader should be closed after freeContext", closed.get());
        assertEquals(0, store.activeCount());
    }

    public void testFreeContextRemovesFromStore() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", mockGatedReader(closed));
        store.releaseContext("q1");
        store.freeContext("q1");

        assertNull("Should not find after free", store.getContext("q1"));
    }

    public void testReaperCleansExpiredContexts() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 50); // 50ms keepAlive

        store.createContext("q1", mockGatedReader(closed));
        store.releaseContext("q1");

        // Wait for expiry + reaper cycle
        assertBusy(() -> {
            assertTrue("Reader should be closed by reaper", closed.get());
            assertEquals(0, store.activeCount());
        });
    }

    public void testReaperDoesNotCleanInUseContext() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 1); // 1ms keepAlive

        store.createContext("q1", mockGatedReader(closed));
        // Still in-use (not released)

        Thread.sleep(50);
        assertFalse("Reaper should not close in-use context", closed.get());
        assertEquals(1, store.activeCount());

        // Cleanup
        store.releaseContext("q1");
        store.freeContext("q1");
    }

    public void testMultipleContexts() {
        AtomicBoolean closed1 = new AtomicBoolean(false);
        AtomicBoolean closed2 = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("q1", mockGatedReader(closed1));
        store.createContext("q2", mockGatedReader(closed2));
        assertEquals(2, store.activeCount());

        store.releaseContext("q1");
        store.freeContext("q1");
        assertEquals(1, store.activeCount());
        assertTrue(closed1.get());
        assertFalse(closed2.get());

        store.releaseContext("q2");
        store.freeContext("q2");
        assertEquals(0, store.activeCount());
        assertTrue(closed2.get());
    }

    public void testQueryThenFetchLifecycle() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        // Query phase: create + use
        store.createContext("q1", mockGatedReader(closed));
        ReaderContext ctx = store.getContext("q1");
        assertNotNull(ctx);
        assertNotNull(ctx.getReader());

        // Query done
        store.releaseContext("q1");
        assertFalse("Reader stays open between phases", closed.get());

        // Fetch phase: acquire + use
        ReaderContext fetchCtx = store.acquireContext("q1");
        assertNotNull(fetchCtx);
        assertSame(ctx.getReader(), fetchCtx.getReader());

        // Fetch done: free
        fetchCtx.markDone();
        store.freeContext("q1");
        assertTrue("Reader closed after fetch", closed.get());
    }
}
