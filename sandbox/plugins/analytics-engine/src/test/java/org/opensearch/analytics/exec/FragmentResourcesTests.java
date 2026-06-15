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

/**
 * Verifies how {@link FragmentResources#close()} disposes of the reader context depending on
 * whether the reader is single-session (freed eagerly) or multi-session (kept for the fetch).
 */
public class FragmentResourcesTests extends OpenSearchTestCase {

    private static final ShardId SHARD_0 = new ShardId(new Index("idx", "uuid"), 0);

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

    public void testNoFetchClosesReaderEagerly() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);
        ReaderContext ctx = store.createContext("q1", SHARD_0, mockGatedReader(closed));

        new FragmentResources(store, ctx, null, null, null, false).close(); // fetchFollows=false

        assertTrue("Reader with no following fetch must be closed on close()", closed.get());
        assertEquals("Context must be removed from the store", 0, store.activeCount());
    }

    public void testFetchFollowsKeepsReaderForFetch() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        ReaderContextStore store = new ReaderContextStore(threadPool);
        ReaderContext ctx = store.createContext("q1", SHARD_0, mockGatedReader(closed));

        new FragmentResources(store, ctx, null, null, null, true).close(); // fetchFollows=true

        assertFalse("Reader must stay open for the following fetch phase", closed.get());
        assertEquals("Context must remain in the store", 1, store.activeCount());

        // Fetch reuses then frees it (terminal phase, fetchFollows=false).
        assertNotNull(store.acquireContext("q1", SHARD_0));
        new FragmentResources(store, ctx, null, null, null, false).close();
        assertTrue("Reader closed once the terminal fetch frees it", closed.get());
        assertEquals(0, store.activeCount());
    }
}
