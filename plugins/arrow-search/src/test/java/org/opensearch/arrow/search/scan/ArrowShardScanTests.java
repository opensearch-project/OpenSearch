/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.arrow.search.ArrowSearchPlugin;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.Column.ColumnType;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperExtrasModulePlugin;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QueryCollectorContextSpecFactory;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.VirtualExecutorBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * Runs shard scans on a real shard, as a data node does: each scan is one query phase whose collectors send every batch.
 */
public class ArrowShardScanTests extends OpenSearchSingleNodeTestCase {

    /** Registers the scan collector, the slice listener and the virtual-thread executor that scans run on, as the plugin does. */
    public static class ScanCollectorPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QueryCollectorContextSpecFactory> getCollectorContextSpecFactories() {
            return List.of(new ArrowScanCollectorSpecFactory());
        }

        @Override
        public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
            return List.of(new VirtualExecutorBuilder(ArrowSearchPlugin.EXECUTOR));
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new ArrowScanSliceListener());
        }
    }

    /** Provides a script that fails during scoring, outside the scan's collector. */
    public static class FailingScriptPlugin extends MockScriptPlugin {
        static final String FAIL = "fail";

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(FAIL, vars -> { throw new IllegalStateException("the script failed"); });
        }
    }

    private static final List<Column> COLUMNS = List.of(
        column("id", ColumnType.UTF8, "keyword"),
        column("l", ColumnType.INT64, "long"),
        column("i", ColumnType.INT32, "integer"),
        column("s", ColumnType.INT16, "short"),
        column("b", ColumnType.INT8, "byte"),
        column("d", ColumnType.FLOAT64, "double"),
        column("f", ColumnType.FLOAT32, "float"),
        column("h", ColumnType.FLOAT32, "half_float"),
        column("sf", ColumnType.FLOAT64, "scaled_float"),
        column("ul", ColumnType.UINT64, "unsigned_long"),
        column("bool", ColumnType.BOOL, "boolean"),
        column("dt", ColumnType.TIMESTAMP_MILLIS, "date"),
        column("dn", ColumnType.TIMESTAMP_NANOS, "date_nanos"),
        column("k", ColumnType.UTF8, "keyword"),
        column("ip", ColumnType.IP, "ip")
    );

    private BufferAllocator allocator;
    private BufferAllocator batchAllocator;
    ArrowScanRegistry registry = new ArrowScanRegistry(4, computeSlots());
    private final AtomicLong taskIds = new AtomicLong();
    private final List<Long> batchBytes = Collections.synchronizedList(new ArrayList<>());
    private IndexService index;
    final Map<String, Map<String, Object>> expected = new HashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ScanCollectorPlugin.class, MapperExtrasModulePlugin.class, FailingScriptPlugin.class);
    }

    /** Returns the number of compute slots: how many scans, or slices of scans, compute at once. */
    protected int computeSlots() {
        return 1;
    }

    /** Returns the executor that collects the slices of a scan, or {@code null} to collect a shard's segments one by one. */
    protected Executor sliceExecutor() {
        return null;
    }

    /** Returns the concurrent segment search settings that scans run with. */
    protected Settings concurrentSearchSettings() {
        return Settings.builder().put(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.getKey(), "none").build();
    }

    @Before
    public void createAllocators() {
        allocator = new RootAllocator();
        batchAllocator = allocator.newChildAllocator("batches", 0, Long.MAX_VALUE);
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(concurrentSearchSettings()).get();
    }

    @After
    public void closeAllocators() {
        try {
            assertEquals("batches are released", 0, batchAllocator.getAllocatedMemory());
            batchAllocator.close();
            allocator.close();
        } finally {
            Settings.Builder reset = Settings.builder();
            concurrentSearchSettings().keySet().forEach(reset::putNull);
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(reset).get();
        }
    }

    private static Column column(String name, ColumnType type, String mappingType) {
        return new Column(name, type, Set.of(mappingType));
    }

    void createIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (Column column : COLUMNS) {
            String type = column.mappingTypes().iterator().next();
            mapping.startObject(column.name()).field("type", type);
            if (type.equals("scaled_float")) {
                mapping.field("scaling_factor", 100);
            }
            mapping.endObject();
        }
        mapping.startObject("group").field("type", "integer").endObject();
        mapping.startObject("tags").field("type", "keyword").endObject();
        mapping.endObject().endObject();
        index = createIndex(
            "idx",
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                // Few enough segments that the tiered merge policy leaves them alone.
                .put("index.merge.policy.segments_per_tier", 20)
                .build(),
            mapping
        );
    }

    /** Indexes {@code count} docs in {@code segments} refreshes, keeping the values each doc expects. */
    void indexDocs(int count, int segments) throws IOException {
        int perSegment = Math.max(1, count / segments);
        int next = expected.size();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < count; n++) {
            String id = "d" + (next + n);
            Map<String, Object> source = new HashMap<>();
            Map<String, Object> values = new HashMap<>();
            source.put("id", id);
            values.put("id", id);
            source.put("group", n % 10);
            if (randomInt(9) > 0) {
                long l = randomLong();
                source.put("l", l);
                values.put("l", l);
            }
            if (randomInt(9) > 0) {
                int i = randomInt();
                source.put("i", i);
                values.put("i", i);
            }
            if (randomBoolean()) {
                short s = randomShort();
                source.put("s", s);
                values.put("s", s);
            }
            if (randomBoolean()) {
                byte b = randomByte();
                source.put("b", b);
                values.put("b", b);
            }
            if (randomBoolean()) {
                double d = randomDouble() * randomIntBetween(-1000, 1000);
                source.put("d", d);
                values.put("d", d);
            }
            if (randomBoolean()) {
                float f = randomFloat() * randomIntBetween(-1000, 1000);
                source.put("f", f);
                values.put("f", f);
            }
            if (randomBoolean()) {
                float h = randomIntBetween(-2048, 2048) / 4f;
                source.put("h", h);
                values.put("h", HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(h)));
            }
            if (randomBoolean()) {
                long cents = randomIntBetween(-100000, 100000);
                source.put("sf", cents / 100.0);
                values.put("sf", cents * (1 / 100.0));
            }
            if (randomBoolean()) {
                BigInteger ul = new BigInteger(64, random());
                source.put("ul", ul.toString());
                values.put("ul", ul.longValue());
            }
            if (randomBoolean()) {
                boolean bool = randomBoolean();
                source.put("bool", bool);
                values.put("bool", bool);
            }
            if (randomBoolean()) {
                long millis = randomLongBetween(0, 4_000_000_000_000L);
                source.put("dt", millis);
                values.put("dt", millis);
            }
            if (randomBoolean()) {
                long millis = randomLongBetween(0, 4_000_000_000_000L);
                source.put("dn", millis);
                values.put("dn", millis * 1_000_000L);
            }
            if (randomBoolean()) {
                String k = randomAlphaOfLengthBetween(0, 30);
                source.put("k", k);
                values.put("k", k);
            }
            if (randomBoolean()) {
                InetAddress ip = randomBoolean()
                    ? InetAddresses.forString(randomIntBetween(0, 255) + ".0.0." + randomIntBetween(0, 255))
                    : InetAddresses.forString("2001:db8::" + Integer.toHexString(randomIntBetween(1, 65535)));
                source.put("ip", InetAddresses.toAddrString(ip));
                values.put("ip", HexFormat.of().formatHex(InetAddressPoint.encode(ip)));
            }
            expected.put(id, values);
            bulk.add(client().prepareIndex("idx").setId(id).setSource(source));
            if ((n + 1) % perSegment == 0 || n == count - 1) {
                assertFalse(bulk.get().hasFailures());
                client().admin().indices().prepareRefresh("idx").get();
                bulk = client().prepareBulk();
            }
        }
    }

    private ShardId shardId() {
        return index.getShard(0).shardId();
    }

    static ArrowScanSpec spec(long batchBytes, long firstBatchBytes, long shardRowLimit) {
        return new ArrowScanSpec(COLUMNS, batchBytes, firstBatchBytes, 65536, 3 << 20, shardRowLimit, TimeValue.timeValueMinutes(1));
    }

    private ArrowScanSpec randomSpec() {
        long batch = randomFrom(16L << 10, 64L << 10, 1L << 20);
        return spec(batch, randomFrom(16L << 10, batch), Long.MAX_VALUE);
    }

    /** The rows and summary of a scan. */
    record ScanResult(List<Map<String, Object>> rows, List<Integer> batchRows, ShardSummary summary) {
    }

    /** Runs after each batch the scan sends, on the thread that sent it, so a test can pause the scan there. */
    @FunctionalInterface
    interface SendHook {
        void afterSend(int batches) throws Exception;
    }

    ScanResult scan(QueryBuilder query, ArrowScanSpec spec) throws Exception {
        return scan(query, spec, field -> true, -1, batches -> {});
    }

    /**
     * Runs a scan as the shard stream handler does: one query phase on a virtual thread, whose collectors send each full
     * batch, from as many slices as the scan collects in parallel.
     */
    ScanResult scan(QueryBuilder query, ArrowScanSpec spec, Predicate<String> fieldVisible, long computeBudgetNanos, SendHook hook)
        throws Exception {
        return scan(query, spec, fieldVisible, computeBudgetNanos, hook, () -> false);
    }

    /** As above, with a consumer that may go away. */
    ScanResult scan(
        QueryBuilder query,
        ArrowScanSpec spec,
        Predicate<String> fieldVisible,
        long computeBudgetNanos,
        SendHook hook,
        BooleanSupplier consumerGone
    ) throws Exception {
        SearchService searchService = getInstanceFromNode(SearchService.class);
        SearchSourceBuilder source = new SearchSourceBuilder().query(query);
        spec.columns().forEach(c -> source.docValueField(c.name()));
        List<Map<String, Object>> rows = new ArrayList<>();
        List<Integer> batchRows = new ArrayList<>();
        ReentrantLock received = new ReentrantLock();
        ScanState state = new ScanState(spec, batchAllocator, fieldVisible, batch -> {
            int batches;
            received.lock();
            try (batch) {
                batchBytes.add(BatchBytes.messageBytes(batch, batch.getRowCount()));
                batchRows.add(batch.getRowCount());
                rows.addAll(rows(batch));
                batches = batchRows.size();
            } finally {
                received.unlock();
            }
            hook.afterSend(batches);
        }, registry::acquireBatchSlot, consumerGone, computeBudgetNanos, sliceExecutor());
        ShardSearchRequest base = new ShardSearchRequest(
            new OriginalIndices(new String[] { "idx" }, IndicesOptions.strictExpandOpenAndForbidClosed()),
            new SearchRequest("idx").source(source).allowPartialSearchResults(true),
            shardId(),
            1,
            AliasFilter.EMPTY,
            1f,
            System.currentTimeMillis(),
            null,
            Strings.EMPTY_ARRAY
        );
        ShardSearchRequest request = ArrowShardStreamHandler.scanRequest(base, state);
        SearchShardTask task = new SearchShardTask(taskIds.incrementAndGet(), "transport", "scan", "", TaskId.EMPTY_TASK_ID, Map.of());
        try {
            PlainActionFuture<SearchPhaseResult> result = new PlainActionFuture<>();
            searchService.executeQueryPhase(request, false, task, result, ArrowSearchPlugin.EXECUTOR);
            result.get();
            for (VectorSchemaRoot last : state.takeRemainingRows()) {
                try (last) {
                    batchRows.add(last.getRowCount());
                    rows.addAll(rows(last));
                }
            }
        } finally {
            state.close();
        }
        return new ScanResult(rows, batchRows, new ShardSummary(rows.size(), state.terminatedEarly, state.timedOut));
    }

    private static List<Map<String, Object>> rows(VectorSchemaRoot root) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int r = 0; r < root.getRowCount(); r++) {
            Map<String, Object> row = new HashMap<>();
            for (FieldVector vector : root.getFieldVectors()) {
                if (vector.isNull(r) == false) {
                    row.put(vector.getName(), value(vector, r));
                }
            }
            rows.add(row);
        }
        return rows;
    }

    private static Object value(FieldVector vector, int index) {
        return switch (vector) {
            case VarCharVector v -> new String(v.get(index), StandardCharsets.UTF_8);
            case BigIntVector v -> v.get(index);
            case IntVector v -> v.get(index);
            case SmallIntVector v -> v.get(index);
            case TinyIntVector v -> v.get(index);
            case Float8Vector v -> v.get(index);
            case Float4Vector v -> v.get(index);
            case UInt8Vector v -> v.get(index);
            case BitVector v -> v.get(index) == 1;
            case TimeStampMilliTZVector v -> v.get(index);
            case TimeStampNanoTZVector v -> v.get(index);
            case FixedSizeBinaryVector v -> HexFormat.of().formatHex(v.get(index));
            case ListVector v -> {
                List<Object> items = new ArrayList<>();
                int start = v.getOffsetBuffer().getInt((long) index * ListVector.OFFSET_WIDTH);
                int end = v.getOffsetBuffer().getInt((long) (index + 1) * ListVector.OFFSET_WIDTH);
                for (int i = start; i < end; i++) {
                    assertFalse("list values are never null", v.getDataVector().isNull(i));
                    items.add(value(v.getDataVector(), i));
                }
                yield items;
            }
            default -> throw new AssertionError("unexpected vector " + vector.getClass());
        };
    }

    void assertRows(Set<String> ids, List<Map<String, Object>> rows) {
        Map<String, Map<String, Object>> byId = new HashMap<>();
        for (Map<String, Object> row : rows) {
            assertNull("row " + row.get("id") + " was emitted twice", byId.put((String) row.get("id"), row));
        }
        assertEquals(ids, byId.keySet());
        for (String id : ids) {
            Map<String, Object> want = expected.get(id);
            Map<String, Object> got = byId.get(id);
            assertEquals(id, want.keySet(), got.keySet());
            for (Map.Entry<String, Object> entry : want.entrySet()) {
                Object value = got.get(entry.getKey());
                if (entry.getValue() instanceof Short s) {
                    assertEquals(id + "." + entry.getKey(), (short) s, ((Number) value).shortValue());
                } else if (entry.getValue() instanceof Byte b) {
                    assertEquals(id + "." + entry.getKey(), (byte) b, ((Number) value).byteValue());
                } else {
                    assertEquals(id + "." + entry.getKey(), entry.getValue(), value);
                }
            }
        }
    }

    private Set<String> ids(Predicate<Map<String, Object>> filter) {
        Set<String> ids = new HashSet<>();
        expected.forEach((id, values) -> {
            if (filter.test(values)) {
                ids.add(id);
            }
        });
        return ids;
    }

    public void testEveryMatchIsEmittedOnceWithItsValues() throws Exception {
        createIndex();
        indexDocs(randomIntBetween(200, 2000), randomIntBetween(1, 6));
        ArrowScanSpec spec = randomSpec();
        ScanResult all = scan(QueryBuilders.matchAllQuery(), spec);
        assertRows(expected.keySet(), all.rows());
        assertFalse(all.summary().terminatedEarly());
        assertFalse(all.summary().timedOut());
    }

    public void testSmallBatchesSplitEverySegment() throws Exception {
        createIndex();
        indexDocs(3000, 3);
        ScanResult result = scan(QueryBuilders.matchAllQuery(), spec(16 << 10, 16 << 10, Long.MAX_VALUE));
        assertRows(expected.keySet(), result.rows());
        assertTrue("16KB batches split 3000 wide rows", result.batchRows().size() > 10);
    }

    public void testFilteredScanAcrossManyBatches() throws Exception {
        createIndex();
        indexDocs(randomIntBetween(1000, 3000), randomIntBetween(2, 5));
        Set<String> evenGroups = new HashSet<>();
        for (int n = 0; n < expected.size(); n++) {
            if ((n % 10) % 2 == 0) {
                evenGroups.add("d" + n);
            }
        }
        QueryBuilder query = QueryBuilders.termsQuery("group", List.of(0, 2, 4, 6, 8));
        ScanResult result = scan(query, spec(16 << 10, 16 << 10, Long.MAX_VALUE));
        assertTrue(result.batchRows().size() > 1);
        assertRows(evenGroups, result.rows());
    }

    public void testRangeQueryOverManyDocsPerSegmentReturnsEveryMatch() throws Exception {
        createIndex();
        int count = 12_000;
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < count; n++) {
            String id = "d" + n;
            expected.put(id, Map.of("id", id, "l", (long) n));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "l", n));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareRefresh("idx").get();
        // Unless hits are counted exactly, a range query stops after about 10k docs per segment.
        ScanResult result = scan(QueryBuilders.rangeQuery("l").gte(0), spec(1 << 20, 64 << 10, Long.MAX_VALUE));
        assertRows(expected.keySet(), result.rows());
    }

    public void testShardRowLimitEndsTheScan() throws Exception {
        createIndex();
        indexDocs(500, 2);
        int limit = randomIntBetween(1, 300);
        ScanResult result = scan(QueryBuilders.matchAllQuery(), spec(16 << 10, 16 << 10, limit));
        assertEquals(limit, result.rows().size());
        assertTrue(result.summary().terminatedEarly());
        assertEquals(limit, result.rows().stream().map(r -> r.get("id")).distinct().count());
    }

    public void testShardRowLimitReachedByTheLastMatchIsNotEarlyTermination() throws Exception {
        createIndex();
        indexDocs(500, 2);
        ScanResult exact = scan(QueryBuilders.matchAllQuery(), spec(16 << 10, 16 << 10, 500));
        assertRows(expected.keySet(), exact.rows());
        assertFalse("the limit left no match out", exact.summary().terminatedEarly());

        // With a lower limit, a segment past the limit, or a match past it in the same segment, is left out.
        for (int limit : new int[] { 250, 499 }) {
            ScanResult cut = scan(QueryBuilders.matchAllQuery(), spec(16 << 10, 16 << 10, limit));
            assertEquals(limit, cut.rows().size());
            assertTrue("limit " + limit, cut.summary().terminatedEarly());
        }

        // The same holds for a filter whose matches across both segments exactly fill the limit.
        Set<String> group = ids(values -> false);
        for (int n = 0; n < expected.size(); n++) {
            if (n % 10 == 3) {
                group.add("d" + n);
            }
        }
        ScanResult filtered = scan(QueryBuilders.termQuery("group", 3), spec(16 << 10, 16 << 10, group.size()));
        assertRows(group, filtered.rows());
        assertFalse(filtered.summary().terminatedEarly());
    }

    public void testDeletedDocsAreNotEmitted() throws Exception {
        createIndex();
        indexDocs(400, 2);
        List<String> deleted = randomSubsetOf(50, expected.keySet());
        for (String id : deleted) {
            client().prepareDelete("idx", id).get();
            expected.remove(id);
        }
        client().admin().indices().prepareRefresh("idx").get();
        assertRows(expected.keySet(), scan(QueryBuilders.matchAllQuery(), randomSpec()).rows());
    }

    public void testHiddenFieldsReadAsNulls() throws Exception {
        createIndex();
        indexDocs(100, 1);
        ScanResult result = scan(QueryBuilders.matchAllQuery(), randomSpec(), field -> field.equals("k") == false, -1, batches -> {});
        expected.values().forEach(values -> values.remove("k"));
        assertRows(expected.keySet(), result.rows());
    }

    public void testEmptyShardHasNoRows() throws Exception {
        createIndex();
        ScanResult result = scan(QueryBuilders.matchAllQuery(), randomSpec());
        assertEquals(List.of(), result.batchRows());
        assertEquals(0, result.summary().rows());
    }

    public void testNoMatchHasNoRows() throws Exception {
        createIndex();
        indexDocs(100, 2);
        ScanResult result = scan(QueryBuilders.termQuery("group", 42), randomSpec());
        assertEquals(List.of(), result.batchRows());
    }

    public void testMultiValuedFieldFailsTheScan() throws Exception {
        createIndex();
        indexDocs(50, 1);
        client().prepareIndex("idx").setId("multi").setSource("id", "multi", "l", List.of(1, 2)).get();
        client().admin().indices().prepareRefresh("idx").get();
        Exception e = expectThrows(Exception.class, () -> scan(QueryBuilders.matchAllQuery(), randomSpec()));
        Throwable cause = ExceptionsHelper.unwrap(e, ArrowScanDataException.class);
        assertThat(e.toString(), cause, instanceOf(ArrowScanDataException.class));
        assertThat(cause.getMessage(), containsString("field [l] has [2] values"));
    }

    public void testTypeMismatchOnTheShardFailsTheScan() throws Exception {
        createIndex();
        indexDocs(10, 1);
        ArrowScanSpec spec = new ArrowScanSpec(
            List.of(column("k", ColumnType.INT64, "long")),
            1 << 20,
            64 << 10,
            65536,
            3 << 20,
            Long.MAX_VALUE,
            TimeValue.timeValueMinutes(1)
        );
        Exception e = expectThrows(Exception.class, () -> scan(QueryBuilders.matchAllQuery(), spec));
        assertThat(e.toString(), ExceptionsHelper.unwrap(e, ArrowScanDataException.class), instanceOf(ArrowScanDataException.class));
    }

    public void testScanPausesInASendAndResumesTheSameQuery() throws Exception {
        createIndex();
        indexDocs(3000, 3);
        Set<String> before = Set.copyOf(expected.keySet());
        List<String> deleted = randomSubsetOf(20, before);
        long queriesBefore = queryCount();
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicReference<ScanResult> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread scanThread = Thread.ofVirtual().start(() -> {
            try {
                result.set(scan(QueryBuilders.matchAllQuery(), spec(16 << 10, 16 << 10, Long.MAX_VALUE), field -> true, -1, batches -> {
                    if (batches == 1) {
                        paused.countDown();
                        assertTrue(resume.await(30, TimeUnit.SECONDS));
                    }
                }));
            } catch (Exception e) {
                failure.set(e);
            }
        });
        assertTrue("the scan did not pause: " + failure.get(), paused.await(30, TimeUnit.SECONDS));

        // A paused scan computes nothing, so it leaves its compute slot to other scans.
        CountDownLatch acquired = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            try (Releasable slot = registry.acquireBatchSlot(Long.MAX_VALUE)) {
                acquired.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertTrue("the paused scan holds no compute slot", acquired.await(30, TimeUnit.SECONDS));

        // Changes made while the scan is paused are not seen: the scan reads the shard as its query phase opened it.
        indexDocs(100, 1);
        for (String id : deleted) {
            client().prepareDelete("idx", id).get();
        }
        client().admin().indices().prepareRefresh("idx").get();

        resume.countDown();
        scanThread.join();
        assertNull(failure.get());
        assertTrue("the scan needs several batches", result.get().batchRows().size() > 1);
        assertRows(before, result.get().rows());
        assertEquals("every batch comes from the one query phase", 1, queryCount() - queriesBefore);
    }

    public void testComputeBudgetEndsTheScan() throws Exception {
        createIndex();
        indexDocs(500, 2);
        ScanResult result = scan(QueryBuilders.matchAllQuery(), randomSpec(), field -> true, 0, batches -> {});
        assertTrue(result.summary().timedOut());
        assertTrue(result.rows().size() < expected.size());
    }

    private long queryCount() {
        return client().admin().indices().prepareStats("idx").setSearch(true).get().getTotal().getSearch().getTotal().getQueryCount();
    }

    public void testListColumnsHoldEveryValueInDocValuesOrder() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("id").field("type", "keyword").endObject();
        mapping.startObject("ml").field("type", "long").endObject();
        mapping.startObject("md").field("type", "double").endObject();
        mapping.startObject("msf").field("type", "scaled_float").field("scaling_factor", 100).endObject();
        mapping.startObject("mb").field("type", "boolean").endObject();
        mapping.startObject("mk").field("type", "keyword").endObject();
        mapping.startObject("mip").field("type", "ip").endObject();
        mapping.endObject().endObject();
        index = createIndex("idx", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(), mapping);
        List<Column> columns = List.of(
            column("id", ColumnType.UTF8, "keyword"),
            new Column("ml", ColumnType.INT64, Set.of("long"), true),
            new Column("md", ColumnType.FLOAT64, Set.of("double"), true),
            new Column("msf", ColumnType.FLOAT64, Set.of("scaled_float"), true),
            new Column("mb", ColumnType.BOOL, Set.of("boolean"), true),
            new Column("mk", ColumnType.UTF8, Set.of("keyword"), true),
            new Column("mip", ColumnType.IP, Set.of("ip"), true)
        );
        int count = randomIntBetween(200, 1500);
        int segments = randomIntBetween(1, 4);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < count; n++) {
            String id = "d" + n;
            Map<String, Object> source = new HashMap<>();
            Map<String, Object> values = new HashMap<>();
            source.put("id", id);
            values.put("id", id);
            int values0 = randomIntBetween(0, 4);
            if (values0 > 0 || randomBoolean()) {
                List<Long> longs = new ArrayList<>();
                List<Long> cents = new ArrayList<>();
                List<Double> doubles = new ArrayList<>();
                List<Boolean> bools = new ArrayList<>();
                TreeSet<String> keywords = new TreeSet<>();
                TreeMap<String, String> ips = new TreeMap<>();
                List<String> rawKeywords = new ArrayList<>();
                List<String> rawIps = new ArrayList<>();
                for (int v = 0; v < values0; v++) {
                    longs.add(randomLong());
                    cents.add((long) randomIntBetween(-100000, 100000));
                    doubles.add(randomDouble() * randomIntBetween(-100, 100));
                    bools.add(randomBoolean());
                    String keyword = randomFrom("a", "b", "c", randomAlphaOfLength(5));
                    rawKeywords.add(keyword);
                    keywords.add(keyword);
                    InetAddress ip = InetAddresses.forString("10.0." + randomIntBetween(0, 3) + "." + randomIntBetween(0, 255));
                    rawIps.add(InetAddresses.toAddrString(ip));
                    String hex = HexFormat.of().formatHex(InetAddressPoint.encode(ip));
                    ips.put(hex, hex);
                }
                source.put("ml", longs);
                source.put("md", doubles);
                source.put("msf", cents.stream().map(c -> c / 100.0).toList());
                source.put("mb", bools);
                source.put("mk", rawKeywords);
                source.put("mip", rawIps);
                if (values0 > 0) {
                    values.put("ml", longs.stream().sorted().map(Object.class::cast).toList());
                    values.put("md", doubles.stream().sorted().map(Object.class::cast).toList());
                    values.put("msf", cents.stream().sorted().map(c -> (Object) (c * (1 / 100.0))).toList());
                    values.put("mb", bools.stream().sorted().map(Object.class::cast).toList());
                    values.put("mk", new ArrayList<Object>(keywords));
                    values.put("mip", new ArrayList<Object>(ips.keySet()));
                }
            }
            expected.put(id, values);
            bulk.add(client().prepareIndex("idx").setId(id).setSource(source));
            if ((n + 1) % Math.max(1, count / segments) == 0 || n == count - 1) {
                assertFalse(bulk.get().hasFailures());
                client().admin().indices().prepareRefresh("idx").get();
                bulk = client().prepareBulk();
            }
        }
        ArrowScanSpec spec = new ArrowScanSpec(columns, 16 << 10, 16 << 10, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1));
        ScanResult result = scan(QueryBuilders.matchAllQuery(), spec);
        assertTrue("small batches cut lists across batches", result.batchRows().size() > 1);
        assertRows(expected.keySet(), result.rows());
    }

    public void testRowsThatGrowSuddenlyTakeAboutOneBatchOfMemory() throws Exception {
        createIndex();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 3104; n++) {
            String id = "d" + n;
            // Tiny rows teach the scan a small row size, then a chunk's worth of 8KB rows follows in the same segment.
            String k = n < 2080 ? "k" : randomAlphaOfLength(8 * 1024);
            expected.put(id, Map.of("id", id, "k", k));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "k", k));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        List<Column> columns = List.of(column("id", ColumnType.UTF8, "keyword"), column("k", ColumnType.UTF8, "keyword"));
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, budget, budget, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        assertRows(expected.keySet(), result.rows());
        // A pass sized by the tiny rows would read 1024 rows of 8KB.
        assertThat(batchAllocator.getPeakMemoryAllocation(), lessThan(1L << 20));
    }

    public void testARowLargerThanTheBatchBeforeAnotherVariableColumnCutsOnlyItsOwnBatch() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("mk").field("type", "keyword").endObject();
        mapping.startObject("id").field("type", "keyword").endObject();
        mapping.endObject().endObject();
        index = createIndex("idx", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(), mapping);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 600; n++) {
            String id = "d" + n;
            // One row in the middle is larger than a batch; its large values are in the first column.
            TreeSet<String> mk = new TreeSet<>();
            mk.add("k");
            if (n == 300) {
                while (mk.size() < 17) {
                    mk.add(randomAlphaOfLength(8 * 1024));
                }
            }
            expected.put(id, Map.of("id", id, "mk", new ArrayList<Object>(mk)));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "mk", new ArrayList<>(mk)));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareForceMerge("idx").setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        List<Column> columns = List.of(
            new Column("mk", ColumnType.UTF8, Set.of("keyword"), true),
            column("id", ColumnType.UTF8, "keyword")
        );
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, budget, budget, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        assertRows(expected.keySet(), result.rows());
        // The rows before the large one go out together, then the large row on its own; not one batch per row.
        assertThat(result.batchRows().toString(), result.batchRows().size(), lessThan(10));
        // The batch after the large row is presized for one row of that size, not for 16 such rows, which would take 2MB.
        assertThat(batchAllocator.getPeakMemoryAllocation(), lessThan(1L << 20));
    }

    public void testALaterColumnGrowingMoreThanTheOneThatOverdrawsTakesAboutOneBatchOfMemory() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (String field : List.of("id", "a", "b")) {
            mapping.startObject(field).field("type", "keyword").endObject();
        }
        mapping.endObject().endObject();
        index = createIndex("idx", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(), mapping);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 2280; n++) {
            String id = "d" + n;
            // Tiny rows teach the scan a small row size, then rows follow whose last column grows far more than the one before.
            boolean large = n >= 2080;
            String a = large ? randomAlphaOfLength(256) : "a";
            String b = large ? randomAlphaOfLength(30 * 1024) : "b";
            expected.put(id, Map.of("id", id, "a", a, "b", b));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "a", a, "b", b));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareForceMerge("idx").setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        List<Column> columns = List.of(
            column("id", ColumnType.UTF8, "keyword"),
            column("a", ColumnType.UTF8, "keyword"),
            column("b", ColumnType.UTF8, "keyword")
        );
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, budget, budget, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        assertRows(expected.keySet(), result.rows());
        // Column a overdraws a pass of tiny rows after about 90 rows; column b reading all of them would take 2.7MB.
        assertThat(batchAllocator.getPeakMemoryAllocation(), lessThan(1L << 20));
    }

    public void testColumnsThatGrowTogetherFillTheirBatches() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        for (String field : List.of("id", "a", "b")) {
            mapping.startObject(field).field("type", "keyword").endObject();
        }
        mapping.endObject().endObject();
        index = createIndex("idx", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(), mapping);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 2380; n++) {
            String id = "d" + n;
            // Tiny rows teach the scan a small row size, then rows follow whose columns a and b grow alike, by varying sizes.
            int length = n < 2080 ? 1 : randomIntBetween(256, 2048);
            String a = randomAlphaOfLength(length);
            String b = randomAlphaOfLength(length);
            expected.put(id, Map.of("id", id, "a", a, "b", b));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "a", a, "b", b));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareForceMerge("idx").setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        List<Column> columns = List.of(
            column("id", ColumnType.UTF8, "keyword"),
            column("a", ColumnType.UTF8, "keyword"),
            column("b", ColumnType.UTF8, "keyword")
        );
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, budget, budget, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        assertRows(expected.keySet(), result.rows());
        // A pass ends at the row that overdraws the batch, whichever column overdraws it. So every batch sent holds the rows
        // that fit, not only the first row of a pass whose column a overdrew the batch.
        for (int b = 0; b < batchBytes.size(); b++) {
            assertThat(result.batchRows().toString(), batchBytes.get(b), greaterThan(budget / 2));
        }
    }

    public void testRowsNearTheBatchBudgetPresizeAboutOneBatch() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("id").field("type", "keyword").endObject();
        mapping.startObject("tags").field("type", "keyword").endObject();
        mapping.endObject().endObject();
        index = createIndex("idx", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(), mapping);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 30; n++) {
            String id = "d" + n;
            // Rows of 48KB, three quarters of a batch.
            TreeSet<String> tags = new TreeSet<>();
            while (tags.size() < 6) {
                tags.add(randomAlphaOfLength(8 * 1024));
            }
            expected.put(id, Map.of("id", id, "tags", new ArrayList<Object>(tags)));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "tags", new ArrayList<>(tags)));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareForceMerge("idx").setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        List<Column> columns = List.of(
            column("id", ColumnType.UTF8, "keyword"),
            new Column("tags", ColumnType.UTF8, Set.of("keyword"), true)
        );
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, budget, budget, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        assertRows(expected.keySet(), result.rows());
        // A batch presized for 16 of the rows measured last would take 768KB.
        assertThat(batchAllocator.getPeakMemoryAllocation(), lessThan(512L << 10));
    }

    public void testBatchMemoryLimitRejectsTheFirstBatchAndReleasesTheScan() throws Exception {
        createIndex();
        indexDocs(200, 1);
        // Below the memory the first batch is presized with.
        limitBatchMemory(16 << 10);
        assertRejectedAndReleased(spec(64 << 10, 64 << 10, Long.MAX_VALUE));
    }

    public void testBatchMemoryLimitRejectsTheValuesOfABatchAndReleasesTheScan() throws Exception {
        createIndex();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 100; n++) {
            bulk.add(client().prepareIndex("idx").setId("d" + n).setSource("id", "d" + n, "k", randomAlphaOfLength(8 * 1024)));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareRefresh("idx").get();
        // The first batch is presized within the limit, but the values of its first rows exceed it.
        limitBatchMemory(256 << 10);
        List<Column> columns = List.of(column("id", ColumnType.UTF8, "keyword"), column("k", ColumnType.UTF8, "keyword"));
        assertRejectedAndReleased(
            new ArrowScanSpec(columns, 1 << 20, 1 << 20, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
    }

    /** Limits the memory of the scans' batches, as {@code search.arrow.shard.max_buffered_bytes} does on a data node. */
    private void limitBatchMemory(long limit) {
        batchAllocator.close();
        batchAllocator = allocator.newChildAllocator("batches", 0, limit);
    }

    /**
     * Asserts that the scan fails as a rejection, which the coordinator retries on another copy, and releases its batches and
     * compute slots.
     */
    private void assertRejectedAndReleased(ArrowScanSpec spec) throws Exception {
        Exception e = expectThrows(Exception.class, () -> scan(QueryBuilders.matchAllQuery(), spec));
        OpenSearchRejectedExecutionException rejection = (OpenSearchRejectedExecutionException) ExceptionsHelper.unwrap(
            e,
            OpenSearchRejectedExecutionException.class
        );
        assertNotNull(ExceptionsHelper.stackTrace(e), rejection);
        assertThat(List.of(rejection.getSuppressed()).toString(), rejection.getSuppressed()[0], instanceOf(OutOfMemoryException.class));
        assertEquals("batches are released", 0, batchAllocator.getAllocatedMemory());
        List<Releasable> slots = new ArrayList<>();
        try {
            for (int s = 0; s < computeSlots(); s++) {
                Releasable slot = registry.acquireBatchSlot(0);
                assertNotNull("compute slot " + s + " is free", slot);
                slots.add(slot);
            }
        } finally {
            Releasables.close(slots);
        }
    }

    public void testColumnsThatGrowTogetherKeepFullBatches() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("id").field("type", "keyword").endObject();
        mapping.startObject("a").field("type", "keyword").endObject();
        mapping.startObject("b").field("type", "keyword").endObject();
        mapping.endObject().endObject();
        index = createIndex("idx", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build(), mapping);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 4000; n++) {
            String id = "d" + n;
            // Tiny rows teach the scan a small row size, then both value columns grow to 1KB together.
            String a = n < 2000 ? "a" : randomAlphaOfLength(1024);
            String b = n < 2000 ? "b" : randomAlphaOfLength(1024);
            expected.put(id, Map.of("id", id, "a", a, "b", b));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "a", a, "b", b));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareForceMerge("idx").setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        List<Column> columns = List.of(
            column("id", ColumnType.UTF8, "keyword"),
            column("a", ColumnType.UTF8, "keyword"),
            column("b", ColumnType.UTF8, "keyword")
        );
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, budget, budget, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        assertRows(expected.keySet(), result.rows());
        // About 30 rows of 2KB fit a 64KB batch. A pass that misjudges the row size cuts its batch to the rows that fit,
        // not to a single row.
        long small = result.batchRows().stream().filter(rows -> rows < 8).count();
        assertThat(result.batchRows().toString(), small, lessThan(4L));
    }

    public void testRowsThatShrinkSuddenlyKeepTheMemoryOfTheirBatch() throws Exception {
        createIndex();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 4100; n++) {
            String id = "d" + n;
            // Rows of 8KB teach the scan a large value size, then tiny rows follow in the same segment.
            String k = n < 100 ? randomAlphaOfLength(8 * 1024) : "k";
            expected.put(id, Map.of("id", id, "k", k));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "k", k));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        List<Column> columns = List.of(column("id", ColumnType.UTF8, "keyword"), column("k", ColumnType.UTF8, "keyword"));
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, budget, budget, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        assertRows(expected.keySet(), result.rows());
        // A batch sized for 8KB values holds thousands of tiny rows; their data must not grow with the row count.
        assertThat(batchAllocator.getPeakMemoryAllocation(), lessThan(4L << 20));
    }

    public void testFirstBatchOfAWideSchemaIsSizedByItsBudget() throws Exception {
        int fields = 200;
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        List<Column> columns = new ArrayList<>();
        for (int f = 0; f < fields; f++) {
            mapping.startObject("f" + f).field("type", "long").endObject();
            columns.add(column("f" + f, ColumnType.INT64, "long"));
        }
        mapping.endObject().endObject();
        index = createIndex(
            "idx",
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey(), fields)
                .build(),
            mapping
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 20; n++) {
            Map<String, Object> source = new HashMap<>();
            for (int f = 0; f < fields; f++) {
                source.put("f" + f, (long) n * fields + f);
            }
            bulk.add(client().prepareIndex("idx").setId("d" + n).setSource(source));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareRefresh("idx").get();
        ScanResult result = scan(
            QueryBuilders.matchAllQuery(),
            new ArrowScanSpec(columns, 1 << 20, 64 << 10, 65536, 3 << 20, Long.MAX_VALUE, TimeValue.timeValueMinutes(1))
        );
        Set<Long> firstValues = new HashSet<>();
        for (Map<String, Object> row : result.rows()) {
            assertEquals(fields, row.size());
            firstValues.add((Long) row.get("f0"));
        }
        assertEquals(20, firstValues.size());
        // 1024 rows of every column would take 3MB for a first batch of 64KB.
        assertThat(batchAllocator.getPeakMemoryAllocation(), lessThan(512L << 10));
    }

    public void testRowsThatGrowSuddenlyStayWithinTheBatchBudget() throws Exception {
        createIndex();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int n = 0; n < 3200; n++) {
            String id = "d" + n;
            // Tiny rows teach the scan a small row size, then rows of 8KB follow in the same segment.
            String k = n < 3000 ? "k" : randomAlphaOfLength(8 * 1024);
            expected.put(id, Map.of("id", id, "k", k));
            bulk.add(client().prepareIndex("idx").setId(id).setSource("id", id, "k", k));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareRefresh("idx").get();
        long budget = 64 << 10;
        ScanResult result = scan(QueryBuilders.matchAllQuery(), spec(budget, budget, Long.MAX_VALUE));
        assertRows(expected.keySet(), result.rows());
        for (int b = 0; b < batchBytes.size(); b++) {
            if (result.batchRows().get(b) > 1) {
                assertTrue("batch " + b + " of " + batchBytes.get(b) + " bytes", batchBytes.get(b) <= budget);
            }
        }
    }
}
