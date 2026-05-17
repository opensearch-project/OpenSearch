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

/**
 * Tests the full data-node QTF lifecycle: query phase stores reader in context,
 * fetch phase reuses the same reader, and context is freed after fetch.
 */
public class QueryThenFetchDataNodeTests extends OpenSearchTestCase {

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

    /**
     * Happy path: query phase stores reader -> fetch phase gets same reader -> free closes it.
     */
    public void testFullQueryThenFetchLifecycle() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        // Simulate query phase: acquire reader and store in context
        GatedCloseable<Reader> gatedReader = mockGatedReader(closed);
        Reader originalReader = gatedReader.get();
        ReaderContext ctx = store.createContext("query-1", gatedReader);
        assertNotNull(ctx);
        assertSame(originalReader, ctx.getReader());

        // Query phase completes, release context (reader stays alive for fetch)
        store.releaseContext("query-1");
        assertFalse("Reader must not be closed between phases", closed.get());

        // Simulate fetch phase: acquire the same context
        ReaderContext fetchCtx = store.acquireContext("query-1");
        assertNotNull("Fetch phase must find the context", fetchCtx);
        assertSame("Fetch must get the SAME reader instance", originalReader, fetchCtx.getReader());

        // Fetch completes
        fetchCtx.markDone();
        store.freeContext("query-1");
        assertTrue("Reader must be closed after completeFetch", closed.get());
        assertEquals(0, store.activeCount());
    }

    /**
     * Fetch arrives after the keep-alive expires and reaper cleans up -> acquireContext returns null.
     */
    public void testFetchWithExpiredContext() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 50); // 50ms keepAlive

        store.createContext("query-expired", mockGatedReader(closed));
        store.releaseContext("query-expired");

        // Wait for reaper to clean up the expired context
        assertBusy(() -> {
            assertTrue("Reader should be closed by reaper", closed.get());
            assertEquals(0, store.activeCount());
        });

        // Fetch arrives too late
        ReaderContext fetchCtx = store.acquireContext("query-expired");
        assertNull("Expired context must not be acquirable", fetchCtx);
    }

    /**
     * Fetch for an unknown queryId returns null (no prior query phase).
     */
    public void testFetchWithoutPriorQuery() {
        ReaderContextStore store = new ReaderContextStore(threadPool);

        ReaderContext ctx = store.acquireContext("nonexistent-query");
        assertNull("Unknown queryId must return null", ctx);
    }

    /**
     * Verify reader's close callback is NOT invoked between query and fetch phases.
     */
    public void testReaderNotClosedBetweenPhases() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("query-2", mockGatedReader(closed));
        // Query phase done
        store.releaseContext("query-2");

        // Between phases: reader must still be alive
        assertFalse("Reader must remain open while awaiting fetch", closed.get());
        assertEquals(1, store.activeCount());

        // Cleanup
        store.freeContext("query-2");
    }

    /**
     * Verify reader's close callback IS invoked after completeFetch (freeContext).
     */
    public void testReaderClosedAfterCompleteFetch() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        store.createContext("query-3", mockGatedReader(closed));
        store.releaseContext("query-3");

        ReaderContext fetchCtx = store.acquireContext("query-3");
        assertNotNull(fetchCtx);
        assertFalse("Reader must not be closed during fetch", closed.get());

        // Simulate completeFetch
        fetchCtx.markDone();
        store.freeContext("query-3");

        assertTrue("Reader must be closed after freeContext", closed.get());
        assertEquals(0, store.activeCount());
        assertNull("Context must be removed from store", store.getContext("query-3"));
    }


    /**
     * Multiple queries have independent contexts with different readers and independent lifecycles.
     */
    public void testMultipleQueriesIndependentContexts() {
        AtomicBoolean closed1 = new AtomicBoolean(false);
        AtomicBoolean closed2 = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);

        GatedCloseable<Reader> gated1 = mockGatedReader(closed1);
        GatedCloseable<Reader> gated2 = mockGatedReader(closed2);
        Reader reader1 = gated1.get();
        Reader reader2 = gated2.get();

        store.createContext("q1", gated1);
        store.createContext("q2", gated2);
        assertEquals(2, store.activeCount());

        // Release both query phases
        store.releaseContext("q1");
        store.releaseContext("q2");

        // Fetch q1
        ReaderContext fetch1 = store.acquireContext("q1");
        assertNotNull(fetch1);
        assertSame(reader1, fetch1.getReader());
        assertNotSame("Different queries must have different readers", reader1, reader2);

        // Free q1 while q2 still alive
        fetch1.markDone();
        store.freeContext("q1");
        assertTrue("q1 reader closed", closed1.get());
        assertFalse("q2 reader still alive", closed2.get());
        assertEquals(1, store.activeCount());

        // Fetch q2
        ReaderContext fetch2 = store.acquireContext("q2");
        assertNotNull(fetch2);
        assertSame(reader2, fetch2.getReader());
        fetch2.markDone();
        store.freeContext("q2");
        assertTrue("q2 reader closed", closed2.get());
        assertEquals(0, store.activeCount());
    }

    /**
     * Even if query "fails" (we just release without a successful result), the context remains
     * available for potential fetch. The reaper cleans it if fetch never arrives.
     */
    public void testQueryContextSurvivesQueryFailure() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool, 50); // short keepAlive for reaper test

        store.createContext("query-failed", mockGatedReader(closed));

        // Simulate "query failure": release the context without explicit cleanup
        store.releaseContext("query-failed");

        // Context still exists immediately after failure
        assertFalse("Reader not yet closed", closed.get());
        assertEquals(1, store.activeCount());

        // A fetch could still arrive in time
        ReaderContext fetchCtx = store.acquireContext("query-failed");
        assertNotNull("Context survives query failure for potential fetch", fetchCtx);
        fetchCtx.markDone();

        // If fetch never comes, reaper eventually cleans up
        // (release again so reaper can reap it)
        assertBusy(() -> {
            assertTrue("Reaper eventually closes reader", closed.get());
            assertEquals(0, store.activeCount());
        });
    }
}
