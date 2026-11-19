/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.opensearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.opensearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.StreamSupport;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GlobalOrdinalsCancellationTests extends OpenSearchTestCase {

    private static final String KEYWORD = "keyword";
    private static final String TEXT = "text";
    private static final int SEGMENTS = 3;

    private Directory directory;
    private DirectoryReader reader;
    private IndicesFieldDataCache nodeCache;
    private IndexFieldDataCache keywordCache;
    private IndexFieldDataCache textCache;
    private CircuitBreakerService breakerService;
    private SortedSetOrdinalsIndexFieldData keywordFieldData;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int doc = 0; doc < 64; doc++) {
                    String value = "value_" + segment + "_" + doc;
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField(KEYWORD, new BytesRef(value)));
                    document.add(new StringField(TEXT, value, Field.Store.NO));
                    writer.addDocument(document);
                }
                writer.commit();
            }
        }
        reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId("index", "_na_", 0));
        assertEquals(SEGMENTS, reader.leaves().size());
        nodeCache = new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
        }, null, null);
        breakerService = new HierarchyCircuitBreakerService(
            Settings.builder().put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false).build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        keywordCache = nodeCache.buildIndexFieldDataCache(new IndexFieldDataCache.Listener() {
        }, new Index("index", "_na_"), KEYWORD);
        textCache = nodeCache.buildIndexFieldDataCache(new IndexFieldDataCache.Listener() {
        }, new Index("index", "_na_"), TEXT);
        keywordFieldData = keywordFieldData(keywordCache, breakerService);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        nodeCache.close();
        directory.close();
        super.tearDown();
    }

    public void testCancelledSearchStopsBuildAndLeavesNoState() throws Exception {
        AtomicBoolean cancelling = new AtomicBoolean(true);
        AtomicInteger checks = new AtomicInteger();
        // The first per-segment check passes and later checks cancel, so the build stops part way.
        ContextIndexSearcher searcher = newSearcher(() -> checks.incrementAndGet() >= 2 && cancelling.get());
        DirectoryReader searchReader = (DirectoryReader) searcher.getIndexReader();

        expectThrows(TaskCancelledException.class, () -> keywordFieldData.loadGlobal(searchReader));
        assertTrue(checks.get() >= 2);
        assertEquals(0, cachedGlobalOrdinals());
        assertEquals(0, breakerService.getBreaker(CircuitBreaker.FIELDDATA).getUsed());

        cancelling.set(false);
        assertNotNull(keywordFieldData.loadGlobal(searchReader).getOrdinalMap());
        assertEquals(1, cachedGlobalOrdinals());
        assertTrue(breakerService.getBreaker(CircuitBreaker.FIELDDATA).getUsed() > 0);
    }

    public void testExpiredRequestTimeoutDoesNotStopBuild() throws Exception {
        ContextIndexSearcher searcher = newSearcher(() -> false);
        searcher.addQueryCancellation(() -> { throw new QueryPhase.TimeExceededException(); });
        DirectoryReader searchReader = (DirectoryReader) searcher.getIndexReader();

        assertNotNull(keywordFieldData.loadGlobal(searchReader).getOrdinalMap());
        assertEquals(1, cachedGlobalOrdinals());
        long hits = nodeCache.getCache().stats().getHits();
        assertNotNull(keywordFieldData.loadGlobal(searchReader).getOrdinalMap());
        assertEquals(hits + 1, nodeCache.getCache().stats().getHits());
    }

    public void testExpiredRequestTimeoutDoesNotStopTextFieldBuild() throws Exception {
        ContextIndexSearcher searcher = newSearcher(() -> false);
        searcher.addQueryCancellation(() -> { throw new QueryPhase.TimeExceededException(); });
        DirectoryReader searchReader = (DirectoryReader) searcher.getIndexReader();
        PagedBytesIndexFieldData textFieldData = textFieldData(textCache);

        assertNotNull(textFieldData.loadGlobal(searchReader).getOrdinalMap());
        assertEquals(1, cachedGlobalOrdinals());
        long hits = nodeCache.getCache().stats().getHits();
        assertNotNull(textFieldData.loadGlobal(searchReader).getOrdinalMap());
        assertEquals(hits + 1, nodeCache.getCache().stats().getHits());
    }

    public void testSegmentLoadStoppedByOwnTimeoutThrowsTheTimeout() throws Exception {
        ContextIndexSearcher searcher = newSearcher(() -> false);
        searcher.addQueryCancellation(() -> { throw new QueryPhase.TimeExceededException(); });

        // The query phase reports this exception as timed_out with partial results, not as a shard failure.
        expectThrows(
            QueryPhase.TimeExceededException.class,
            () -> textFieldData(textCache).load(searcher.getIndexReader().leaves().get(0))
        );
    }

    public void testClosedSearchIsNotRetainedByCachedBuild() throws Exception {
        WeakReference<SearchContext> searchContext = buildThroughClosedSearcher();
        assertEquals(1, cachedGlobalOrdinals());
        assertBusy(() -> {
            System.gc(); // Do not rely on GC to be deterministic, hence the polling
            assertNull("the cached build must not keep the closed search context", searchContext.get());
        }, 10, TimeUnit.SECONDS);
        assertEquals(1, cachedGlobalOrdinals());
    }

    public void testBreakerTripDuringCancellationKeepsBreakerFailure() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean();
        CircuitBreaker breaker = mock(CircuitBreaker.class);
        // The breaker trips at the moment the task is cancelled.
        doAnswer(invocation -> {
            cancelled.set(true);
            throw new CircuitBreakingException("tripped", CircuitBreaker.Durability.PERMANENT);
        }).when(breaker).addEstimateBytesAndMaybeBreak(anyLong(), anyString());
        CircuitBreakerService trippingService = mock(CircuitBreakerService.class);
        when(trippingService.getBreaker(CircuitBreaker.FIELDDATA)).thenReturn(breaker);
        ContextIndexSearcher searcher = newSearcher(cancelled::get);

        Exception e = expectThrows(
            Exception.class,
            () -> keywordFieldData(keywordCache, trippingService).loadGlobal((DirectoryReader) searcher.getIndexReader())
        );

        assertTrue(cancelled.get());
        assertNotNull(ExceptionsHelper.unwrap(e, CircuitBreakingException.class));
        assertNull(ExceptionsHelper.unwrap(e, TaskCancelledException.class));
    }

    public void testSearchWaitingOnBuildStoppedForAnotherSearchBuildsAgain() throws Exception {
        ContextIndexSearcher cancelled = newSearcher(() -> true);
        AtomicInteger waitingChecks = new AtomicInteger();
        ContextIndexSearcher waiting = newSearcher(() -> {
            waitingChecks.incrementAndGet();
            return false;
        });
        WaiterCache cache = new WaiterCache(cancelled.getIndexReader());

        IndexOrdinalsFieldData global = keywordFieldData(cache, breakerService).loadGlobal((DirectoryReader) waiting.getIndexReader());

        assertNotNull(global.getOrdinalMap());
        assertEquals(2, cache.globalLoads.get());
        assertTrue("the second build runs the waiting search's own check", waitingChecks.get() >= SEGMENTS);
    }

    public void testSearchWaitingOnStoppedBuildFailsWithItsOwnCancellation() throws Exception {
        ContextIndexSearcher cancelled = newSearcher(() -> true);
        ContextIndexSearcher waiting = newSearcher(() -> true, "this search");
        WaiterCache cache = new WaiterCache(cancelled.getIndexReader());

        TaskCancelledException e = expectThrows(
            TaskCancelledException.class,
            () -> keywordFieldData(cache, breakerService).loadGlobal((DirectoryReader) waiting.getIndexReader())
        );

        assertEquals("cancelled task with reason: this search", e.getMessage());
        assertTrue(Arrays.stream(e.getSuppressed()).anyMatch(TaskCancelledException.class::isInstance));
        assertEquals(1, cache.globalLoads.get());
    }

    public void testSecondStoppedBuildIsReportedInsteadOfRetried() throws Exception {
        ContextIndexSearcher cancelled = newSearcher(() -> true);
        ContextIndexSearcher waiting = newSearcher(() -> false);
        WaiterCache cache = new WaiterCache(cancelled.getIndexReader(), 2);

        OpenSearchException e = expectThrows(
            OpenSearchException.class,
            () -> keywordFieldData(cache, breakerService).loadGlobal((DirectoryReader) waiting.getIndexReader())
        );

        assertEquals("[" + KEYWORD + "] fielddata load was stopped by other searches 2 times", e.getMessage());
        assertNotNull("the latest failure is the cause", ExceptionsHelper.unwrap(e.getCause(), TaskCancelledException.class));
        assertEquals(1, e.getSuppressed().length);
        assertNotNull("the first failure is suppressed", ExceptionsHelper.unwrap(e.getSuppressed()[0], TaskCancelledException.class));
        assertNotSame(e.getCause(), e.getSuppressed()[0]);
        assertEquals(2, cache.globalLoads.get());
    }

    public void testSearchPastItsTimeoutBuildsAgainAfterAnotherSearchIsCancelled() throws Exception {
        ContextIndexSearcher cancelled = newSearcher(() -> true);
        ContextIndexSearcher waiting = newSearcher(() -> false);
        waiting.addQueryCancellation(() -> { throw new QueryPhase.TimeExceededException(); });
        WaiterCache cache = new WaiterCache(cancelled.getIndexReader());

        // The build ignores the timeout; the query phase reports it where it handles TimeExceededException.
        IndexOrdinalsFieldData global = keywordFieldData(cache, breakerService).loadGlobal((DirectoryReader) waiting.getIndexReader());

        assertNotNull(global.getOrdinalMap());
        assertEquals(2, cache.globalLoads.get());
    }

    public void testSearchWaitingOnSegmentLoadStoppedForAnotherSearchLoadsAgain() throws Exception {
        ContextIndexSearcher cancelled = newSearcher(() -> false);
        cancelled.addQueryCancellation(() -> { throw new TaskCancelledException("other search"); });
        ContextIndexSearcher waiting = newSearcher(() -> false);
        AtomicInteger waitingChecks = new AtomicInteger();
        waiting.addQueryCancellation(waitingChecks::incrementAndGet);
        WaiterCache cache = new WaiterCache(cancelled.getIndexReader());
        PagedBytesIndexFieldData textFieldData = textFieldData(cache);
        LeafReaderContext waitingLeaf = waiting.getIndexReader().leaves().get(0);

        assertNotNull(textFieldData.load(waitingLeaf).getOrdinalsValues());
        assertEquals(2, cache.segmentLoads.get());
        assertTrue("the second load reads terms through the waiting search's reader", waitingChecks.get() > 0);
    }

    public void testSearchWaitingOnStoppedSegmentLoadFailsWithItsOwnStop() throws Exception {
        ContextIndexSearcher cancelled = newSearcher(() -> false);
        cancelled.addQueryCancellation(() -> { throw new TaskCancelledException("other search"); });
        ContextIndexSearcher waiting = newSearcher(() -> false);
        waiting.addQueryCancellation(() -> { throw new QueryPhase.TimeExceededException(); });
        WaiterCache cache = new WaiterCache(cancelled.getIndexReader());

        expectThrows(QueryPhase.TimeExceededException.class, () -> textFieldData(cache).load(waiting.getIndexReader().leaves().get(0)));
        assertEquals(1, cache.segmentLoads.get());
    }

    public void testSearchWaitingOnSharedCacheCompletesAfterOtherSearchIsCancelled() throws Exception {
        CountDownLatch building = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger checks = new AtomicInteger();
        ContextIndexSearcher cancelled = newSearcher(() -> {
            if (checks.incrementAndGet() == 1) {
                building.countDown();
                try {
                    assertTrue(release.await(30, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            return true;
        });
        ContextIndexSearcher waiting = newSearcher(() -> false);
        AtomicReference<Exception> cancelledFailure = new AtomicReference<>();
        AtomicReference<Exception> waitingFailure = new AtomicReference<>();
        AtomicReference<IndexOrdinalsFieldData> waitingResult = new AtomicReference<>();
        Thread cancelledThread = new Thread(() -> {
            try {
                keywordFieldData.loadGlobal((DirectoryReader) cancelled.getIndexReader());
            } catch (Exception e) {
                cancelledFailure.set(e);
            }
        });
        Thread waitingThread = new Thread(() -> {
            try {
                waitingResult.set(keywordFieldData.loadGlobal((DirectoryReader) waiting.getIndexReader()));
            } catch (Exception e) {
                waitingFailure.set(e);
            }
        });

        try {
            cancelledThread.start();
            assertTrue(building.await(30, TimeUnit.SECONDS));
            waitingThread.start();
            // The waiting search parks on the build that the cancelled search runs in the node cache.
            assertBusy(() -> assertEquals(Thread.State.WAITING, waitingThread.getState()));
        } finally {
            release.countDown();
            cancelledThread.join(TimeUnit.SECONDS.toMillis(30));
            waitingThread.join(TimeUnit.SECONDS.toMillis(30));
        }

        assertFalse("the cancelled search did not finish within 30 seconds", cancelledThread.isAlive());
        assertFalse("the waiting search did not finish within 30 seconds", waitingThread.isAlive());
        if (waitingFailure.get() != null) {
            throw new AssertionError("the waiting search failed", waitingFailure.get());
        }
        assertTrue(String.valueOf(cancelledFailure.get()), cancelledFailure.get() instanceof TaskCancelledException);
        assertNotNull(waitingResult.get().getOrdinalMap());
        assertEquals(1, cachedGlobalOrdinals());
    }

    public void testReaderWithoutCancellationBuildsNormally() throws Exception {
        assertNotNull(keywordFieldData.loadGlobal(reader).getOrdinalMap());
        assertEquals(1, cachedGlobalOrdinals());
    }

    private ContextIndexSearcher newSearcher(BooleanSupplier taskCancelled) throws Exception {
        return newSearcher(taskCancelled, "other search");
    }

    private ContextIndexSearcher newSearcher(BooleanSupplier taskCancelled, String reason) throws Exception {
        SearchShardTask task = mock(SearchShardTask.class);
        when(task.isCancelled()).thenAnswer(invocation -> taskCancelled.getAsBoolean());
        when(task.getReasonCancelled()).thenReturn(reason);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getTask()).thenReturn(task);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );
    }

    private WeakReference<SearchContext> buildThroughClosedSearcher() throws Exception {
        TestSearchContext searchContext = new TestSearchContext(null);
        searchContext.setTask(new SearchShardTask(1L, "", "", "", null, Collections.emptyMap()));
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );
        assertNotNull(keywordFieldData.loadGlobal((DirectoryReader) searcher.getIndexReader()).getOrdinalMap());
        searcher.close();
        return new WeakReference<>(searchContext);
    }

    private static SortedSetOrdinalsIndexFieldData keywordFieldData(IndexFieldDataCache cache, CircuitBreakerService breakers) {
        return new SortedSetOrdinalsIndexFieldData(
            cache,
            KEYWORD,
            CoreValuesSourceType.BYTES,
            breakers,
            AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
        );
    }

    private static PagedBytesIndexFieldData textFieldData(IndexFieldDataCache cache) {
        return new PagedBytesIndexFieldData(
            TEXT,
            CoreValuesSourceType.BYTES,
            cache,
            new NoneCircuitBreakerService(),
            TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
            TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
            TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE
        );
    }

    private long cachedGlobalOrdinals() {
        return StreamSupport.stream(nodeCache.getCache().values().spliterator(), false)
            .filter(GlobalOrdinalsIndexFieldData.class::isInstance)
            .count();
    }

    /**
     * Fails the first loads the way the node cache fails a search that waits inside the cache on another search's load:
     * it runs that load through the other search's reader and rethrows its failure. Later loads run normally.
     */
    private static final class WaiterCache implements IndexFieldDataCache {
        private final DirectoryReader otherSearchReader;
        private final AtomicInteger globalLoads = new AtomicInteger();
        private final AtomicInteger segmentLoads = new AtomicInteger();

        private final int stoppedLoads;

        private WaiterCache(IndexReader otherSearchReader) {
            this(otherSearchReader, 1);
        }

        private WaiterCache(IndexReader otherSearchReader, int stoppedLoads) {
            this.otherSearchReader = (DirectoryReader) otherSearchReader;
            this.stoppedLoads = stoppedLoads;
        }

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
            throws Exception {
            if (indexFieldData instanceof PagedBytesIndexFieldData && segmentLoads.incrementAndGet() <= stoppedLoads) {
                try {
                    indexFieldData.loadDirect(otherSearchReader.leaves().get(context.ord));
                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
                throw new AssertionError("the other search's load should stop");
            }
            return indexFieldData.loadDirect(context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData)
            throws Exception {
            if (globalLoads.incrementAndGet() <= stoppedLoads) {
                try {
                    indexFieldData.loadGlobalDirect(otherSearchReader);
                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
                throw new AssertionError("the other search's build should stop");
            }
            return (IFD) indexFieldData.loadGlobalDirect(indexReader);
        }

        @Override
        public void clear() {}

        @Override
        public void clear(String fieldName) {}
    }
}
