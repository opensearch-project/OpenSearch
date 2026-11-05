/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.arrow.search.ArrowSearchPlugin;
import org.opensearch.arrow.search.ArrowSearchSettings;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.Column.ColumnType;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperExtrasModulePlugin;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.search.query.QueryPhaseSearcherWrapper;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

/**
 * Serves shard streams through the handler, as a data node does. Checks how each stream ends, and that it releases what it
 * holds: its open-scan slot, its registration, its compute slots and its batches.
 */
public class ArrowShardStreamHandlerTests extends OpenSearchSingleNodeTestCase {

    private static final int SLICES = 4;

    /** Takes over the query phase when asked, as a plugin's query phase searcher does for its own queries. */
    public static class TakeOverPlugin extends Plugin implements SearchPlugin {
        static volatile boolean takeOver;

        @Override
        public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
            final QueryPhaseSearcher regular = new QueryPhaseSearcherWrapper();
            return Optional.of(new QueryPhaseSearcher() {
                @Override
                public boolean searchWith(
                    SearchContext searchContext,
                    ContextIndexSearcher searcher,
                    Query query,
                    LinkedList<QueryCollectorContext> collectors,
                    boolean hasFilterCollector,
                    boolean hasTimeout
                ) throws IOException {
                    if (takeOver == false) {
                        return regular.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
                    }
                    final TotalHits hits = new TotalHits(searcher.count(query), TotalHits.Relation.EQUAL_TO);
                    searchContext.queryResult()
                        .topDocs(new TopDocsAndMaxScore(new TopDocs(hits, new ScoreDoc[0]), Float.NaN), new DocValueFormat[0]);
                    return false;
                }

                @Override
                public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
                    return regular.aggregationProcessor(searchContext);
                }
            });
        }
    }

    private static final List<Column> COLUMNS = List.of(
        new Column("id", ColumnType.UTF8, Set.of("keyword")),
        new Column("l", ColumnType.INT64, Set.of("long")),
        new Column("ip", ColumnType.IP, Set.of("ip")),
        new Column("tags", ColumnType.UTF8, Set.of("keyword"), true)
    );

    private final AtomicLong taskIds = new AtomicLong();
    private BufferAllocator allocator;
    private BufferAllocator batchAllocator;
    private ArrowScanRegistry registry;
    private IndexService index;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ArrowShardScanTests.ScanCollectorPlugin.class, TakeOverPlugin.class, MapperExtrasModulePlugin.class);
    }

    @Before
    public void setUpScans() {
        allocator = new RootAllocator();
        batchAllocator = allocator.newChildAllocator("batches", 0, Long.MAX_VALUE);
        // One open scan at most, so a scan that keeps its slot fails the next one.
        registry = new ArrowScanRegistry(1, SLICES);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(concurrentSearch()).get();
    }

    @After
    public void tearDownScans() {
        try {
            batchAllocator.close();
            allocator.close();
        } finally {
            TakeOverPlugin.takeOver = false;
            Settings.Builder reset = Settings.builder();
            concurrentSearch().keySet().forEach(reset::putNull);
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(reset).get();
        }
    }

    private static Settings concurrentSearch() {
        return Settings.builder()
            .put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "auto")
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY, SLICES)
            .build();
    }

    /** Indexes {@code segments} segments of {@code perSegment} docs. */
    private void createIndex(int segments, int perSegment) {
        index = createIndexWithSimpleMappings(
            "idx",
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.merge.policy.segments_per_tier", 20)
                .build(),
            "id",
            "type=keyword",
            "l",
            "type=long",
            "ip",
            "type=ip",
            "tags",
            "type=keyword"
        );
        int next = 0;
        for (int s = 0; s < segments; s++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int d = 0; d < perSegment; d++, next++) {
                bulk.add(
                    client().prepareIndex("idx")
                        .setId("d" + next)
                        .setSource("id", "d" + next, "l", next, "ip", "10.0.0." + (next % 256), "tags", List.of("a", "b" + next % 3))
                );
            }
            assertFalse(bulk.get().hasFailures());
            client().admin().indices().prepareRefresh("idx").get();
        }
    }

    private ArrowShardStreamHandler handler() {
        ArrowSearchSettings settings = new ArrowSearchSettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, new HashSet<>(ArrowSearchSettings.all()))
        );
        return new ArrowShardStreamHandler(
            getInstanceFromNode(SearchService.class),
            getInstanceFromNode(IndicesService.class),
            taskManager(),
            getInstanceFromNode(ClusterService.class),
            registry,
            settings,
            batchAllocator,
            getInstanceFromNode(ThreadPool.class).executor(ArrowSearchPlugin.EXECUTOR)
        );
    }

    private TaskManager taskManager() {
        return getInstanceFromNode(TransportService.class).getTaskManager();
    }

    private static ArrowScanSpec spec(long batchBytes) {
        return new ArrowScanSpec(COLUMNS, batchBytes, batchBytes, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1));
    }

    private static ArrowShardStreamRequest request(ShardId shardId, QueryBuilder query, ArrowScanSpec spec, boolean allowPartial) {
        return request(shardId, new SearchSourceBuilder().query(query), spec, allowPartial, 1);
    }

    private static ArrowShardStreamRequest request(
        ShardId shardId,
        SearchSourceBuilder source,
        ArrowScanSpec spec,
        boolean allowPartial,
        int numberOfShards
    ) {
        spec.columns().forEach(column -> source.docValueField(column.name()));
        ShardSearchRequest shardRequest = new ShardSearchRequest(
            new OriginalIndices(new String[] { shardId.getIndexName() }, IndicesOptions.strictExpandOpenAndForbidClosed()),
            new SearchRequest(shardId.getIndexName()).source(source).allowPartialSearchResults(allowPartial),
            shardId,
            numberOfShards,
            AliasFilter.EMPTY,
            1f,
            System.currentTimeMillis(),
            null,
            Strings.EMPTY_ARRAY
        );
        return new ArrowShardStreamRequest(shardRequest, spec);
    }

    private ArrowShardScanTask task() {
        return new ArrowShardScanTask(
            taskIds.incrementAndGet(),
            "transport",
            ArrowShardStreamHandler.ACTION,
            "",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            () -> ""
        );
    }

    private ShardId shardId() {
        return index.getShard(0).shardId();
    }

    /** Serves a shard stream and waits for its end. */
    private RecordingChannel serve(ArrowShardStreamRequest request, RecordingChannel channel) throws Exception {
        handler().messageReceived(request, channel, task());
        assertTrue("the stream ended", channel.ended.await(30, TimeUnit.SECONDS));
        return channel;
    }

    /** Asserts that the scan released everything it held, so the node can run the next one. */
    private void assertReleased() throws Exception {
        assertBusy(() -> {
            assertEquals(0, registry.runningScans());
            try (Releasable scanSlot = registry.acquireScanSlot(TimeValue.ZERO)) {
                assertNotNull(scanSlot);
            }
            List<Releasable> computeSlots = new ArrayList<>();
            try {
                for (int s = 0; s < SLICES; s++) {
                    Releasable slot = registry.acquireBatchSlot(0);
                    assertNotNull("compute slot " + s + " is free", slot);
                    computeSlots.add(slot);
                }
            } finally {
                Releasables.close(computeSlots);
            }
            assertEquals("batches are released", 0, batchAllocator.getAllocatedMemory());
        });
    }

    public void testRowsLeftBySlicesEndTheStreamWithTheSummaryOnTheLastBatch() throws Exception {
        createIndex(SLICES, 25);
        // Batches large enough that every slice leaves its rows for the end of the stream.
        RecordingChannel channel = serve(request(shardId(), QueryBuilders.matchAllQuery(), spec(1 << 20), true), new RecordingChannel());

        assertNull(channel.failure);
        assertTrue(channel.completed);
        assertTrue("every slice leaves a batch: " + channel.batchRows, channel.batchRows.size() > 1);
        assertEquals(100, channel.batchRows.stream().mapToInt(Integer::intValue).sum());
        for (int b = 0; b < channel.summaries.size() - 1; b++) {
            assertNull("only the last batch carries the summary", channel.summaries.get(b));
        }
        assertEquals(new ShardSummary(100, false, false), channel.summaries.get(channel.summaries.size() - 1));
        assertFalse(channel.sentOnVirtualThread.contains(false));
        assertReleased();
    }

    public void testShardWithoutMatchesEndsWithAnEmptyBatch() throws Exception {
        // A shard without segments, then one whose segments hold no match.
        createIndex(0, 0);
        assertEndsWithAnEmptyBatch(QueryBuilders.matchAllQuery());
        for (int s = 0; s < 2; s++) {
            client().prepareIndex("idx").setId("d" + s).setSource("id", "d" + s).get();
            client().admin().indices().prepareRefresh("idx").get();
        }
        assertEndsWithAnEmptyBatch(QueryBuilders.termQuery("id", "none"));
    }

    private void assertEndsWithAnEmptyBatch(QueryBuilder query) throws Exception {
        RecordingChannel channel = serve(request(shardId(), query, spec(1 << 20), true), new RecordingChannel());
        assertNull(channel.failure);
        assertTrue(channel.completed);
        assertEquals(List.of(0), channel.batchRows);
        assertEquals(new ShardSummary(0, false, false), channel.summaries.get(0));
        assertReleased();
    }

    public void testFailedSendClosesTheBatchesNotSentYet() throws Exception {
        createIndex(SLICES, 25);
        RecordingChannel channel = serve(request(shardId(), QueryBuilders.matchAllQuery(), spec(1 << 20), true), new RecordingChannel(1));

        assertFalse(channel.completed);
        assertNotNull(channel.failure);
        assertEquals(StreamErrorCode.UNAVAILABLE, ((StreamException) channel.failure).getErrorCode());
        assertEquals("the stream stops at the failed send", 1, channel.batchRows.size());
        assertReleased();
    }

    public void testTimeoutWithoutPartialResultsFailsTheShard() throws Exception {
        createIndex(2, 50);
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).timeout(TimeValue.ZERO);
        RecordingChannel channel = serve(request(shardId(), source, spec(1 << 20), false, 1), new RecordingChannel());

        assertFalse(channel.completed);
        OpenSearchStatusException timeout = (OpenSearchStatusException) ExceptionsHelper.unwrap(
            channel.failure,
            OpenSearchStatusException.class
        );
        assertNotNull(String.valueOf(channel.failure), timeout);
        assertEquals(RestStatus.GATEWAY_TIMEOUT, timeout.status());
        assertEquals(List.of(), channel.batchRows);
        assertReleased();
    }

    public void testBatchMemoryLimitFailsTheShardAsARejection() throws Exception {
        createIndex(SLICES, 25);
        // Below the memory that each slice presizes its first batch with.
        limitBatchMemory(4 << 10);
        assertRejected();
    }

    public void testBatchMemoryLimitBelowAnEmptyBatchFailsTheShardAsARejection() throws Exception {
        createIndex(0, 0);
        limitBatchMemory(1);
        assertRejected();
    }

    /** Limits the memory of the scans' batches, as {@code search.arrow.shard.max_buffered_bytes} does on a data node. */
    private void limitBatchMemory(long limit) {
        batchAllocator.close();
        batchAllocator = allocator.newChildAllocator("batches", 0, limit);
    }

    /** Asserts that the shard fails as a rejection, which the coordinator retries on another copy, and releases what it holds. */
    private void assertRejected() throws Exception {
        RecordingChannel channel = serve(request(shardId(), QueryBuilders.matchAllQuery(), spec(1 << 20), true), new RecordingChannel());

        assertFalse(channel.completed);
        assertEquals(List.of(), channel.batchRows);
        StreamException failure = (StreamException) channel.failure;
        assertEquals(String.valueOf(failure), StreamErrorCode.RESOURCE_EXHAUSTED, failure.getErrorCode());
        assertThat(failure.getMessage(), containsString("native memory allocation rejected at [arrow-search-batches]"));
        assertReleased();
    }

    public void testMissingIndexFailsTheShard() throws Exception {
        ShardId missing = new ShardId(new Index("missing", "_na_"), 0);
        RecordingChannel channel = serve(request(missing, QueryBuilders.matchAllQuery(), spec(1 << 20), true), new RecordingChannel());

        assertFalse(channel.completed);
        assertNotNull(String.valueOf(channel.failure), ExceptionsHelper.unwrap(channel.failure, IndexNotFoundException.class));
        assertReleased();
    }

    public void testShardThatCannotMatchEndsOnAVirtualThreadWithAnEmptyBatch() throws Exception {
        createIndex(1, 10);
        // After the first shard response, the coordinator lets a shard that cannot match skip its query phase. The shard then
        // answers on the thread that found out, here the test thread.
        ArrowShardStreamRequest request = request(
            shardId(),
            new SearchSourceBuilder().query(new MatchNoneQueryBuilder()),
            spec(1 << 20),
            true,
            2
        );
        request.canReturnNullResponseIfMatchNoDocs(true);
        RecordingChannel channel = serve(request, new RecordingChannel());

        assertNull(channel.failure);
        assertTrue(channel.completed);
        assertEquals(List.of(0), channel.batchRows);
        assertEquals(new ShardSummary(0, false, false), channel.summaries.get(0));
        assertEquals("the stream ends on a virtual thread", List.of(true), channel.sentOnVirtualThread);
        // Arrow's default capacity would hold thousands of values of every column.
        assertThat(channel.allocatedOnSend.get(0), lessThan(1024L));
        assertReleased();
    }

    public void testQueryPhaseCollectedByAnotherPluginFailsTheShard() throws Exception {
        createIndex(2, 50);
        TakeOverPlugin.takeOver = true;
        RecordingChannel channel = serve(request(shardId(), QueryBuilders.matchAllQuery(), spec(1 << 20), true), new RecordingChannel());

        assertFalse("the shard's rows would be missing", channel.completed);
        assertEquals(List.of(), channel.batchRows);
        StreamException failure = (StreamException) channel.failure;
        assertEquals("the same on every copy", RestStatus.BAD_REQUEST, failure.getErrorCode().restStatus());
        assertThat(failure.getMessage(), containsString("another plugin collected its query phase"));
        assertReleased();
    }

    public void testAbortedShardEndsAScanPausedInASend() throws Exception {
        createIndex(1, 3000);
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        RecordingChannel channel = new RecordingChannel() {
            @Override
            void afterSend(int batches) throws InterruptedException {
                if (batches == 1) {
                    paused.countDown();
                    assertTrue(resume.await(30, TimeUnit.SECONDS));
                }
            }
        };
        ArrowShardStreamRequest request = request(shardId(), QueryBuilders.matchAllQuery(), spec(16 << 10), true);
        // Register the task, as the transport does for the requests it serves, so the abort can cancel it.
        Task task = taskManager().register("transport", ArrowShardStreamHandler.ACTION, request);
        try {
            handler().messageReceived(request, channel, task);
            assertTrue("the scan paused in a send", paused.await(30, TimeUnit.SECONDS));
            assertEquals(1, registry.runningScans());
            // Abort the shard's scans, as a closing shard does.
            registry.abortShard(shardId(), "the shard is closing");
            resume.countDown();
            assertTrue("the stream ended", channel.ended.await(30, TimeUnit.SECONDS));
        } finally {
            resume.countDown();
            taskManager().unregister(task);
        }
        assertFalse(channel.completed);
        assertNotNull(String.valueOf(channel.failure), ExceptionsHelper.unwrap(channel.failure, TaskCancelledException.class));
        assertReleased();
    }

    /** A shard stream as the stream transport takes it: it owns each batch it is handed, sent or not. */
    private class RecordingChannel implements TransportChannel {
        final List<Integer> batchRows = new CopyOnWriteArrayList<>();
        final List<ShardSummary> summaries = new CopyOnWriteArrayList<>();
        final List<Boolean> sentOnVirtualThread = new CopyOnWriteArrayList<>();
        final List<Long> allocatedOnSend = new CopyOnWriteArrayList<>();
        final CountDownLatch ended = new CountDownLatch(1);
        private final int failingBatch;
        volatile boolean completed;
        volatile Exception failure;

        RecordingChannel() {
            this(0);
        }

        /**
         * Creates a channel that may fail one send.
         *
         * @param failingBatch the 1-based number of the batch whose send fails, or 0 for none
         */
        RecordingChannel(int failingBatch) {
            this.failingBatch = failingBatch;
        }

        /** Runs after a batch was taken, on the sending thread. */
        void afterSend(int batches) throws InterruptedException {}

        @Override
        public String getProfileName() {
            return "test";
        }

        @Override
        public String getChannelType() {
            return "test";
        }

        @Override
        public void sendResponseBatch(TransportResponse response, boolean sync) {
            final ArrowScanBatchResponse batch = (ArrowScanBatchResponse) response;
            final int batches;
            try (VectorSchemaRoot root = batch.getRoot()) {
                sentOnVirtualThread.add(Thread.currentThread().isVirtual());
                allocatedOnSend.add(batchAllocator.getAllocatedMemory());
                // The transport writes the batch as an Arrow IPC message.
                try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
                    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(new ByteArrayOutputStream())), recordBatch);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                batchRows.add(root.getRowCount());
                summaries.add(batch.getMetadata() == null ? null : ShardSummary.decode(batch.getMetadata()));
                batches = batchRows.size();
                if (batches == failingBatch) {
                    throw new StreamException(StreamErrorCode.UNAVAILABLE, "the consumer is unavailable");
                }
            }
            try {
                afterSend(batches);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        @Override
        public void completeStream() {
            completed = true;
            ended.countDown();
        }

        @Override
        public void sendResponse(TransportResponse response) {
            throw new UnsupportedOperationException("shard streams send batches");
        }

        @Override
        public void sendResponse(Exception exception) {
            failure = exception;
            ended.countDown();
        }
    }
}
