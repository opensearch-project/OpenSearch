/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.lucene.document.InetAddressPoint;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.search.action.ArrowSearchAction;
import org.opensearch.arrow.search.action.TransportArrowSearchAction;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.index.mapper.MapperExtrasModulePlugin;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginsService;
import org.opensearch.protobufs.FieldAndFormat;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.AuxTransport;
import org.opensearch.transport.grpc.GrpcPlugin;
import org.opensearch.transport.grpc.Netty4GrpcServerTransport;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, maxNumDataNodes = 3)
public class ArrowSearchIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "arrow";
    private static final List<String> ALL_FIELDS = List.of(
        "id",
        "b",
        "s",
        "i",
        "l",
        "ul",
        "hf",
        "f",
        "d",
        "sf",
        "bool",
        "dt",
        "dn",
        "kw",
        "ip"
    );

    private BufferAllocator allocator;
    private ArrowStreamClient client;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, MapperExtrasModulePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(GrpcPlugin.class, List.of()),
            classpathPlugin(ArrowSearchPlugin.class, List.of(ArrowBasePlugin.class.getName(), GrpcPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> plugin, List<String> extended) {
        return new PluginInfo(plugin.getName(), "classpath plugin", "NA", Version.CURRENT, "21", plugin.getName(), null, extended, false);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(AuxTransport.AUX_TRANSPORT_TYPES_KEY, Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY)
            .build();
    }

    @Before
    public void openClient() {
        allocator = new RootAllocator();
        final List<TransportAddress> addresses = new ArrayList<>();
        for (Netty4GrpcServerTransport transport : internalCluster().getInstances(Netty4GrpcServerTransport.class)) {
            addresses.add(new TransportAddress(transport.getBoundAddress().publishAddress().address()));
        }
        client = new ArrowStreamClient(randomFrom(addresses));
    }

    private TransportAddress randomGrpcAddress() {
        final List<TransportAddress> addresses = new ArrayList<>();
        for (Netty4GrpcServerTransport transport : internalCluster().getInstances(Netty4GrpcServerTransport.class)) {
            addresses.add(new TransportAddress(transport.getBoundAddress().publishAddress().address()));
        }
        return randomFrom(addresses);
    }

    @After
    public void closeClient() throws Exception {
        client.close();
        allocator.close();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().putNull(ArrowSearchSettings.BATCH_ROWS.getKey()).putNull(ArrowSearchSettings.BATCH_SIZE.getKey())
            )
            .get();
        assertBusy(() -> assertEquals(List.of(), arrowTasks()));
        // Every batch is released on every node: relayed, sent, cancelled or failed.
        assertBusy(this::assertBatchesReleased);
        // Every shard scan ended on every node and freed its open-scan slot for the next scan.
        assertBusy(() -> {
            for (PluginsService plugins : internalCluster().getInstances(PluginsService.class)) {
                for (ArrowSearchPlugin plugin : plugins.filterPlugins(ArrowSearchPlugin.class)) {
                    assertEquals("open shard scans", 0, plugin.components().registry().runningScans());
                }
            }
        });
        // Every search returned its coordinator reservation, however it ended.
        assertBusy(() -> {
            for (TransportArrowSearchAction action : internalCluster().getInstances(TransportArrowSearchAction.class)) {
                assertEquals("reserved coordinator bytes", 0, action.reservedBytes());
            }
        });
    }

    private void assertBatchesReleased() {
        for (ArrowNativeAllocator nativeAllocator : internalCluster().getInstances(ArrowNativeAllocator.class)) {
            for (BufferAllocator child : nativeAllocator.getPoolAllocator(NativeAllocatorPoolConfig.POOL_FLIGHT).getChildAllocators()) {
                if (child.getName().startsWith("arrow-search")) {
                    assertEquals(child.getName(), 0, child.getAllocatedMemory());
                }
            }
        }
    }

    private List<String> arrowTasks() {
        final ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(ArrowSearchAction.NAME + "*").get();
        return tasks.getTasks().stream().map(task -> task.getAction() + " " + task.getDescription()).toList();
    }

    private void setBatchRows(int rows) {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(ArrowSearchSettings.BATCH_ROWS.getKey(), rows))
            .get();
    }

    private void createTypedIndex(String index, int shards) {
        assertAcked(
            prepareCreate(index).setSettings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 1))
                .setMapping(
                    "id",
                    "type=long",
                    "b",
                    "type=byte",
                    "s",
                    "type=short",
                    "i",
                    "type=integer",
                    "l",
                    "type=long",
                    "ul",
                    "type=unsigned_long",
                    "hf",
                    "type=half_float",
                    "f",
                    "type=float",
                    "d",
                    "type=double",
                    "sf",
                    "type=scaled_float,scaling_factor=100",
                    "bool",
                    "type=boolean",
                    "dt",
                    "type=date",
                    "dn",
                    "type=date_nanos",
                    "kw",
                    "type=keyword",
                    "ip",
                    "type=ip",
                    "txt",
                    "type=text"
                )
        );
        ensureGreen(index);
    }

    /** Indexes documents with random values, leaving each optional field out of some documents. */
    private Map<Long, Map<String, Object>> indexDocuments(String index, int count) {
        final Map<Long, Map<String, Object>> expected = new HashMap<>();
        for (long id = 0; id < count; id++) {
            final Map<String, Object> source = new HashMap<>();
            final Map<String, Object> row = new HashMap<>();
            source.put("id", id);
            row.put("id", id);
            for (String field : ALL_FIELDS) {
                if (field.equals("id")) {
                    continue;
                }
                if (rarely()) {
                    row.put(field, null);
                    continue;
                }
                final Object[] value = randomValue(field);
                source.put(field, value[0]);
                row.put(field, value[1]);
            }
            source.put("txt", randomAlphaOfLength(8));
            client().prepareIndex(index).setId(Long.toString(id)).setSource(source).get();
            expected.put(id, row);
        }
        refresh(index);
        return expected;
    }

    /** Returns a random value to index in {@code field}, and the value an Arrow reader returns for it after {@link #normalize}. */
    private Object[] randomValue(String field) {
        return switch (field) {
            case "b" -> {
                byte v = randomByte();
                yield new Object[] { v, v };
            }
            case "s" -> {
                short v = randomShort();
                yield new Object[] { v, v };
            }
            case "i" -> {
                int v = randomInt();
                yield new Object[] { v, v };
            }
            case "l" -> {
                long v = randomLong();
                yield new Object[] { v, v };
            }
            case "ul" -> {
                // Arrow's UInt64 holds the unsigned bits, which a signed long getter reads as negative above 2^63 - 1.
                long v = randomLongBetween(0, Long.MAX_VALUE);
                yield randomBoolean() ? new Object[] { "18446744073709551615", -1L } : new Object[] { Long.toString(v), v };
            }
            case "hf" -> {
                float v = randomIntBetween(-1000, 1000) / 4f;
                yield new Object[] { v, v };
            }
            case "f" -> {
                float v = randomIntBetween(-100000, 100000) / 8f;
                yield new Object[] { v, v };
            }
            case "d" -> {
                double v = randomDouble();
                yield new Object[] { v, v };
            }
            case "sf" -> {
                long cents = randomLongBetween(-100000, 100000);
                // Doc values decode as stored * (1 / scaling_factor), like aggregations and docvalue_fields do.
                yield new Object[] { cents / 100d, cents * (1d / 100) };
            }
            case "bool" -> {
                boolean v = randomBoolean();
                yield new Object[] { v, v };
            }
            case "dt" -> {
                long v = randomLongBetween(0, 4102444800000L);
                yield new Object[] { v, v };
            }
            case "dn" -> {
                long millis = randomLongBetween(0, 4102444800000L);
                long nanos = randomLongBetween(0, 999_999);
                String value = java.time.Instant.ofEpochMilli(millis).plusNanos(nanos).toString();
                yield new Object[] { value, millis * 1_000_000 + nanos };
            }
            case "kw" -> {
                String v = randomUnicodeOfCodepointLengthBetween(0, 20);
                yield new Object[] { v, v };
            }
            case "ip" -> {
                InetAddress v = randomBoolean()
                    ? InetAddresses.forString(randomIntBetween(0, 255) + ".1.2." + randomIntBetween(0, 255))
                    : InetAddresses.forString("2001:db8::" + Integer.toHexString(randomIntBetween(1, 0xffff)));
                yield new Object[] { InetAddresses.toAddrString(v), v };
            }
            default -> throw new AssertionError(field);
        };
    }

    private static Map<String, Object> normalize(Map<String, Object> row) {
        final Map<String, Object> normalized = new HashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            Object value = e.getValue();
            if (value instanceof Text text) {
                value = text.toString();
            } else if (value instanceof byte[] bytes) {
                value = InetAddressPoint.decode(bytes);
            }
            normalized.put(e.getKey(), value);
        }
        return normalized;
    }

    private static SearchRequest.Builder request(String index, Iterable<String> fields) {
        final SearchRequestBody.Builder body = SearchRequestBody.newBuilder();
        for (String field : fields) {
            body.addDocvalueFields(FieldAndFormat.newBuilder().setField(field));
        }
        return SearchRequest.newBuilder().addIndex(index).setSearchRequestBody(body);
    }

    private static SearchRequest.Builder withBody(
        SearchRequest.Builder request,
        java.util.function.Consumer<SearchRequestBody.Builder> edit
    ) {
        final SearchRequestBody.Builder body = request.getSearchRequestBody().toBuilder();
        edit.accept(body);
        return request.setSearchRequestBody(body);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamsEverySupportedTypeFromAllShards() throws Exception {
        createTypedIndex(INDEX, randomIntBetween(2, 5));
        final Map<Long, Map<String, Object>> expected = indexDocuments(INDEX, randomIntBetween(50, 150));
        setBatchRows(randomIntBetween(7, 20));

        final ArrowStreamClient.Result result = client.search(request(INDEX, ALL_FIELDS).build(), allocator);

        assertEquals(ALL_FIELDS, result.schema().getFields().stream().map(Field::getName).toList());
        assertEquals(new ArrowType.Int(16, true), result.schema().findField("s").getType());
        assertEquals(new ArrowType.Int(64, false), result.schema().findField("ul").getType());
        assertEquals(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), result.schema().findField("hf").getType());
        assertEquals(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), result.schema().findField("dn").getType());
        assertEquals(new ArrowType.FixedSizeBinary(16), result.schema().findField("ip").getType());
        assertEquals("ip", result.schema().findField("ip").getMetadata().get(Column.MAPPING_TYPES_METADATA_KEY));
        assertTrue("small batches must split the stream", result.batches() > 1);

        assertEquals(expected.size(), result.rows().size());
        for (Map<String, Object> row : result.rows()) {
            final Map<String, Object> actual = normalize(row);
            assertEquals(expected.get((Long) actual.get("id")), actual);
        }
        assertEquals(expected.size(), result.summary().getHits().getTotal().getTotalHits().getValue());
        assertEquals(result.summary().getXShards().getTotal(), result.summary().getXShards().getSuccessful());
        assertFalse(result.summary().getTerminatedEarly());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testQueryFiltersRows() throws Exception {
        createTypedIndex(INDEX, 3);
        final Map<Long, Map<String, Object>> expected = indexDocuments(INDEX, 60);
        final Set<Long> matching = new HashSet<>();
        final String keyword = (String) expected.values()
            .stream()
            .map(row -> row.get("kw"))
            .filter(value -> value != null)
            .findAny()
            .orElseThrow();
        expected.forEach((id, row) -> {
            if (keyword.equals(row.get("kw"))) {
                matching.add(id);
            }
        });

        final SearchRequest.Builder request = withBody(
            request(INDEX, List.of("id")),
            body -> body.setQuery(
                QueryContainer.newBuilder()
                    .setTerm(TermQuery.newBuilder().setField("kw").setValue(FieldValue.newBuilder().setString(keyword)))
            )
        );
        final ArrowStreamClient.Result result = client.search(request.build(), allocator);

        final Set<Long> ids = new HashSet<>();
        result.rows().forEach(row -> ids.add((Long) row.get("id")));
        assertEquals(matching, ids);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testSizeCapsTotalRows() throws Exception {
        createTypedIndex(INDEX, 3);
        indexDocuments(INDEX, 80);
        setBatchRows(5);
        final int size = randomIntBetween(1, 40);

        final ArrowStreamClient.Result result = client.search(
            withBody(request(INDEX, List.of("id")), body -> body.setSize(size)).build(),
            allocator
        );

        assertEquals(size, result.rows().size());
        assertEquals(size, result.summary().getHits().getTotal().getTotalHits().getValue());
        assertTrue(result.summary().getTerminatedEarly());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testTerminateAfterCapsRowsPerShard() throws Exception {
        createTypedIndex(INDEX, 3);
        indexDocuments(INDEX, 60);

        final ArrowStreamClient.Result result = client.search(
            withBody(request(INDEX, List.of("id")), body -> body.setTerminateAfter(2)).build(),
            allocator
        );

        assertThat(result.rows().size(), lessThanOrEqualTo(3 * 2));
        assertTrue(result.rows().size() > 0);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testAliasFilterAndDeletesApply() throws Exception {
        createTypedIndex(INDEX, 2);
        indexDocuments(INDEX, 40);
        assertAcked(client().admin().indices().prepareAliases().addAlias(INDEX, "low", QueryBuilders.rangeQuery("id").lt(20)));
        client().prepareDelete(INDEX, "3").get();
        refresh(INDEX);

        final ArrowStreamClient.Result result = client.search(request("low", List.of("id")).build(), allocator);

        final Set<Long> ids = new HashSet<>();
        result.rows().forEach(row -> ids.add((Long) row.get("id")));
        final Set<Long> expected = new HashSet<>();
        for (long id = 0; id < 20; id++) {
            if (id != 3) {
                expected.add(id);
            }
        }
        assertEquals(expected, ids);
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testWildcardSkipsFieldsWithoutDocValues() throws Exception {
        createTypedIndex(INDEX, 1);
        indexDocuments(INDEX, 5);

        final ArrowStreamClient.Result result = client.search(request(INDEX, List.of("*")).build(), allocator);

        final List<String> names = result.schema().getFields().stream().map(Field::getName).toList();
        assertFalse("text has no doc values", names.contains("txt"));
        assertTrue(names.containsAll(ALL_FIELDS));
        assertEquals(5, result.rows().size());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testEmptyResultStillSendsSchemaAndSummary() throws Exception {
        createTypedIndex(INDEX, 2);
        indexDocuments(INDEX, 10);

        final ArrowStreamClient.Result result = client.search(
            withBody(
                request(INDEX, List.of("id", "kw")),
                body -> body.setQuery(
                    QueryContainer.newBuilder()
                        .setTerm(TermQuery.newBuilder().setField("kw").setValue(FieldValue.newBuilder().setString("no-such-value-x")))
                )
            ).build(),
            allocator
        );

        assertEquals(List.of("id", "kw"), result.schema().getFields().stream().map(Field::getName).toList());
        assertEquals(0, result.rows().size());
        assertEquals(0, result.summary().getHits().getTotal().getTotalHits().getValue());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testUnsupportedOptionsAreRejected() throws Exception {
        createTypedIndex(INDEX, 1);
        final SearchRequest withSort = withBody(
            request(INDEX, List.of("id")),
            body -> body.addSort(SortCombinations.newBuilder().setField("id"))
        ).build();
        StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> client.search(withSort, allocator));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertThat(e.getStatus().getDescription(), containsString("[sort] is not supported"));

        final SearchRequest noFields = SearchRequest.newBuilder()
            .addIndex(INDEX)
            .setSearchRequestBody(
                SearchRequestBody.newBuilder().setQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder()))
            )
            .build();
        e = expectThrows(StatusRuntimeException.class, () -> client.search(noFields, allocator));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertThat(e.getStatus().getDescription(), containsString("docvalue_fields"));

        e = expectThrows(StatusRuntimeException.class, () -> client.search(request(INDEX, List.of("txt")).build(), allocator));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertThat(e.getStatus().getDescription(), containsString("[txt]"));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testConflictingMappingsAreRejected() throws Exception {
        assertAcked(prepareCreate("left").setMapping("v", "type=long"));
        assertAcked(prepareCreate("right").setMapping("v", "type=keyword"));
        ensureGreen("left", "right");

        final StatusRuntimeException e = expectThrows(
            StatusRuntimeException.class,
            () -> client.search(request("left", List.of("v")).addIndex("right").build(), allocator)
        );
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertThat(e.getStatus().getDescription(), containsString("incompatible types"));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testFieldsInsideNestedFieldsAreRejected() throws Exception {
        assertAcked(prepareCreate("orders").setMapping("""
            {"properties": {"order": {"type": "long"}, "items": {"type": "nested", "properties": {"sku": {"type": "keyword"}}}}}"""));
        ensureGreen("orders");
        client().prepareIndex("orders")
            .setId("1")
            .setSource(Map.of("order", 1, "items", List.of(Map.of("sku", "a"), Map.of("sku", "b"))))
            .get();
        refresh("orders");

        // The scan reads top-level documents, which hold no value of a nested field.
        final StatusRuntimeException e = expectThrows(
            StatusRuntimeException.class,
            () -> client.search(request("orders", List.of("order", "items.sku")).build(), allocator)
        );
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertThat(e.getStatus().getDescription(), containsString("inside the nested field [items]"));

        final ArrowStreamClient.Result result = client.search(request("orders", List.of("*")).build(), allocator);
        final List<String> names = result.schema().getFields().stream().map(Field::getName).toList();
        assertTrue(names.toString(), names.contains("order"));
        assertFalse("a pattern skips the fields of nested documents", names.contains("items.sku"));
        assertEquals(1, result.rows().size());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testColumnsAcrossIndicesAreBoundedByTheDocValueFieldsLimit() throws Exception {
        // Each index maps three of the fields the pattern selects, as many as it lets a search read.
        final Settings limit = Settings.builder().put("index.max_docvalue_fields_search", 3).build();
        assertAcked(
            prepareCreate("metrics-a").setSettings(limit).setMapping("m.a1", "type=long", "m.a2", "type=long", "m.a3", "type=long")
        );
        assertAcked(
            prepareCreate("metrics-b").setSettings(limit).setMapping("m.b1", "type=long", "m.b2", "type=long", "m.b3", "type=long")
        );
        ensureGreen("metrics-a", "metrics-b");
        // The test engine sets itself up on an index's first searcher, which must not be acquired on a virtual thread.
        refresh("metrics-a", "metrics-b");

        final ArrowStreamClient.Result one = client.search(request("metrics-a", List.of("m.*")).build(), allocator);
        assertEquals(3, one.schema().getFields().size());

        // Every shard of either index would read all six columns.
        final StatusRuntimeException e = expectThrows(
            StatusRuntimeException.class,
            () -> client.search(request("metrics-*", List.of("m.*")).build(), allocator)
        );
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertThat(e.getStatus().getDescription(), containsString("[index.max_docvalue_fields_search]"));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMultiValuedFieldNeedsTheListFormat() throws Exception {
        createTypedIndex(INDEX, 1);
        client().prepareIndex(INDEX).setId("1").setSource("id", 1, "kw", List.of("a", "b")).get();
        refresh(INDEX);

        final StatusRuntimeException e = expectThrows(
            StatusRuntimeException.class,
            () -> client.search(request(INDEX, List.of("kw")).build(), allocator)
        );
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        assertThat(e.getStatus().getDescription(), containsString("request it with format [list]"));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testListFormatReadsEveryValue() throws Exception {
        createTypedIndex(INDEX, randomIntBetween(1, 3));
        client().prepareIndex(INDEX).setId("1").setSource("id", 1, "kw", List.of("b", "a", "b"), "l", List.of(3, 1, 3)).get();
        client().prepareIndex(INDEX).setId("2").setSource("id", 2, "kw", "c", "l", 7).get();
        client().prepareIndex(INDEX).setId("3").setSource("id", 3).get();
        refresh(INDEX);

        final SearchRequestBody.Builder body = SearchRequestBody.newBuilder()
            .addDocvalueFields(FieldAndFormat.newBuilder().setField("id"))
            .addDocvalueFields(FieldAndFormat.newBuilder().setField("kw").setFormat(Column.LIST_FORMAT))
            .addDocvalueFields(FieldAndFormat.newBuilder().setField("l").setFormat(Column.LIST_FORMAT));
        final ArrowStreamClient.Result result = client.search(
            SearchRequest.newBuilder().addIndex(INDEX).setSearchRequestBody(body).build(),
            allocator
        );

        assertEquals(ArrowType.List.INSTANCE, result.schema().findField("kw").getType());
        assertEquals(ArrowType.Utf8.INSTANCE, result.schema().findField("kw").getChildren().get(0).getType());
        final Map<Long, Map<String, Object>> rows = new HashMap<>();
        for (Map<String, Object> row : result.rows()) {
            final Map<String, Object> values = new HashMap<>();
            row.forEach(
                (name, value) -> values.put(name, value instanceof List<?> list ? list.stream().map(String::valueOf).toList() : value)
            );
            rows.put((Long) row.get("id"), values);
        }
        // Doc values sort the values; keywords are also deduplicated.
        assertEquals(List.of("a", "b"), rows.get(1L).get("kw"));
        assertEquals(List.of("1", "3", "3"), rows.get(1L).get("l"));
        assertEquals(List.of("c"), rows.get(2L).get("kw"));
        assertEquals(List.of("7"), rows.get(2L).get("l"));
        assertNull("a document without values reads as null", rows.get(3L).get("kw"));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testCompatibleMappingsWiden() throws Exception {
        assertAcked(prepareCreate("ints").setMapping("v", "type=integer"));
        assertAcked(prepareCreate("longs").setMapping("v", "type=long"));
        ensureGreen("ints", "longs");
        client().prepareIndex("ints").setId("1").setSource("v", 5).get();
        client().prepareIndex("longs").setId("1").setSource("v", Long.MAX_VALUE).get();
        refresh("ints", "longs");

        final ArrowStreamClient.Result result = client.search(request("ints", List.of("v")).addIndex("longs").build(), allocator);

        assertEquals(new ArrowType.Int(64, true), result.schema().findField("v").getType());
        assertEquals(Set.of(5L, Long.MAX_VALUE), new HashSet<>(result.rows().stream().map(row -> row.get("v")).toList()));
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testEachShardIsScannedByOneQueryPhase() throws Exception {
        final int shards = randomIntBetween(1, 4);
        createTypedIndex(INDEX, shards);
        indexDocuments(INDEX, 200);
        setBatchRows(3);
        final long before = queryCount(INDEX);

        final ArrowStreamClient.Result result = client.search(request(INDEX, List.of("id", "kw")).build(), allocator);

        assertEquals(200, result.rows().size());
        assertTrue("many batches per shard", result.batches() > shards * 2);
        // Batches pause and resume one query phase per shard instead of searching again for each batch.
        assertEquals(shards, queryCount(INDEX) - before);
    }

    private long queryCount(String index) {
        return client().admin().indices().prepareStats(index).setSearch(true).get().getTotal().getSearch().getTotal().getQueryCount();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testDefaultSearchPipelineIsRejected() throws Exception {
        createTypedIndex(INDEX, 1);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX)
                .setSettings(Settings.builder().put("index.search.default_pipeline", "restricting"))
        );
        try {
            final StatusRuntimeException e = expectThrows(
                StatusRuntimeException.class,
                () -> client.search(request(INDEX, List.of("id")).build(), allocator)
            );
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
            assertThat(e.getStatus().getDescription(), containsString("default search pipeline [restricting]"));
        } finally {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(INDEX)
                    .setSettings(Settings.builder().putNull("index.search.default_pipeline"))
            );
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMissingIndexIsNotFound() {
        final StatusRuntimeException e = expectThrows(
            StatusRuntimeException.class,
            () -> client.search(request("missing", List.of("id")).build(), allocator)
        );
        assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    /**
     * A client that stops reading pauses the search on every node. Once the client reads again, each shard resumes its one
     * query phase and the search completes OK.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStuckClientPausesTheSearch() throws Exception {
        createTypedIndex(INDEX, 2);
        final int docs = 5000;
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int id = 0; id < docs; id++) {
            bulk.add(client().prepareIndex(INDEX).setId(Integer.toString(id)).setSource("id", id, "kw", randomAlphaOfLength(200)));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);
        setBatchRows(10);
        final long queriesBefore = queryCount(INDEX);

        final ArrowStreamClient stuckClient = new ArrowStreamClient(randomGrpcAddress(), true);
        try {
            final AtomicInteger messages = new AtomicInteger();
            final AtomicReference<Status> closed = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(1);
            final CountDownLatch stuck = new CountDownLatch(1);
            final CountDownLatch unstick = new CountDownLatch(1);
            final ClientCall<SearchRequest, byte[]> call = stuckClient.startManual(
                request(INDEX, List.of("id", "kw")).build(),
                new ClientCall.Listener<>() {
                    @Override
                    public void onMessage(byte[] message) {
                        messages.incrementAndGet();
                        if (stuck.getCount() > 0) {
                            stuck.countDown();
                            try {
                                unstick.await(60, java.util.concurrent.TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        closed.set(status);
                        done.countDown();
                    }
                }
            );
            call.request(Integer.MAX_VALUE);
            assertTrue(stuck.await(30, java.util.concurrent.TimeUnit.SECONDS));
            // Paused, not finished: the shard streams wait for the client.
            assertFalse(done.await(2, java.util.concurrent.TimeUnit.SECONDS));
            assertFalse(arrowTasks().isEmpty());

            unstick.countDown();
            assertTrue(done.await(60, java.util.concurrent.TimeUnit.SECONDS));
            assertTrue(closed.get().toString(), closed.get().isOk());
            assertTrue("the rows arrive in many messages", messages.get() > docs / 10);
            assertEquals("each shard resumes its one query phase", 2, queryCount(INDEX) - queriesBefore);
        } finally {
            stuckClient.close();
        }
        // A new search on the same index returns every row.
        final ArrowStreamClient.Result result = client.search(request(INDEX, List.of("id")).build(), allocator);
        assertEquals(docs, result.rows().stream().map(row -> row.get("id")).distinct().count());
    }

    /**
     * A client that stops reading for longer than the idle timeout fails the search, which releases what it holds on every
     * node. The client gets the status when it reads again within the idle timeout after the failure.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testIdleClientFailsTheSearch() throws Exception {
        createTypedIndex(INDEX, 2);
        // Far more than the client's flow-control window and the server's buffers hold, so the search must stall.
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int id = 0; id < 5000; id++) {
            bulk.add(client().prepareIndex(INDEX).setSource("id", id, "kw", randomAlphaOfLength(200)));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);
        setBatchRows(10);
        // Gives the test time to unstick the client after the failure, before its call is reset.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.getKey(), "2s"))
            .get();
        final ArrowStreamClient stuckClient = new ArrowStreamClient(randomGrpcAddress(), true);
        try {
            final AtomicReference<Status> closed = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(1);
            final CountDownLatch stuck = new CountDownLatch(1);
            final CountDownLatch unstick = new CountDownLatch(1);
            final ClientCall<SearchRequest, byte[]> call = stuckClient.startManual(
                request(INDEX, List.of("id", "kw")).build(),
                new ClientCall.Listener<>() {
                    @Override
                    public void onMessage(byte[] message) {
                        if (stuck.getCount() > 0) {
                            stuck.countDown();
                            // Blocks the client's event loop, so it stops reading the connection.
                            try {
                                unstick.await(60, java.util.concurrent.TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        closed.set(status);
                        done.countDown();
                    }
                }
            );
            call.request(Integer.MAX_VALUE);
            assertTrue(stuck.await(30, java.util.concurrent.TimeUnit.SECONDS));
            // The search gives up on the stuck client and releases its shard streams and buffers. Poll at a short, fixed
            // interval so the client reads again well within the idle timeout after the failure, before its call is reset.
            assertBusyWithFixedSleepTime(
                () -> assertEquals(List.of(), arrowTasks()),
                TimeValue.timeValueSeconds(30),
                TimeValue.timeValueMillis(50)
            );
            unstick.countDown();
            assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(closed.get().toString(), Status.Code.DEADLINE_EXCEEDED, closed.get().getCode());
            assertThat(closed.get().getDescription(), containsString("did not read"));
        } finally {
            stuckClient.close();
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.getKey()))
                .get();
        }
    }

    /**
     * A client that stops reading for good but keeps its connection open has its call reset once it stays idle for the
     * timeout after the search fails. The reset releases the batches gRPC still holds for it.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testClientThatNeverReadsAgainIsReset() throws Exception {
        createTypedIndex(INDEX, 2);
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int id = 0; id < 5000; id++) {
            bulk.add(client().prepareIndex(INDEX).setSource("id", id, "kw", randomAlphaOfLength(200)));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(INDEX);
        setBatchRows(10);
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.getKey(), "1s"))
            .get();
        final ArrowStreamClient stuckClient = new ArrowStreamClient(randomGrpcAddress());
        try {
            final CountDownLatch received = new CountDownLatch(3);
            final ClientCall<SearchRequest, byte[]> call = stuckClient.startManual(
                request(INDEX, List.of("id", "kw")).build(),
                new ClientCall.Listener<>() {
                    @Override
                    public void onMessage(byte[] message) {
                        received.countDown();
                    }
                }
            );
            // The schema and two batches, then no more: the client's flow-control window fills up and stays full.
            call.request(3);
            assertTrue(received.await(30, java.util.concurrent.TimeUnit.SECONDS));
            assertBusy(() -> {
                assertEquals(List.of(), arrowTasks());
                assertBatchesReleased();
            }, 30, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            stuckClient.close();
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(ArrowSearchSettings.CLIENT_IDLE_TIMEOUT.getKey()))
                .get();
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testClientCancellationStopsShardStreams() throws Exception {
        createTypedIndex(INDEX, 3);
        indexDocuments(INDEX, 300);
        setBatchRows(1);

        final CountDownLatch firstMessage = new CountDownLatch(1);
        final ClientCall<SearchRequest, byte[]> call = client.startManual(
            request(INDEX, List.of("id")).build(),
            new ClientCall.Listener<>() {
                @Override
                public void onMessage(byte[] message) {
                    firstMessage.countDown();
                }
            }
        );
        call.request(2);
        assertTrue(firstMessage.await(30, java.util.concurrent.TimeUnit.SECONDS));
        assertFalse(arrowTasks().isEmpty());

        call.cancel("client gave up", null);

        assertBusy(() -> assertEquals(List.of(), arrowTasks()), 30, java.util.concurrent.TimeUnit.SECONDS);
    }
}
