/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.engine.dataformat.DataFormatTestUtils.assignTestCapabilities;
import static org.opensearch.parquet.ParquetDataFormatPlugin.PARQUET_DATA_FORMAT;

/**
 * Writes an OTel-traces Parquet file through the real write path — the same
 * {@link ParquetField} implementations the mapping uses, the same {@link VSRManager}, and the same
 * native arrow-rs writer — so the file is byte-for-byte what an index with this mapping produces.
 *
 * <p>Not a unit test: it exists to produce an artifact for inspection with external tools. The output
 * path is taken from {@code -Dotel.parquet.out}, defaulting to {@code /tmp/otel_traces.parquet}.
 */
public class OtelTracesParquetGeneratorTests extends ParquetBaseTests {

    private ArrowNativeAllocator nativeAllocator;
    private ArrowBufferPool bufferPool;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE, null);
        bufferPool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        IndexMetadata meta = IndexMetadata.builder("otel-traces")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .build()
            )
            .build();
        indexSettings = new IndexSettings(meta, Settings.EMPTY);
        Settings nodeSettings = Settings.builder().put("node.name", "otel-gen").build();
        threadPool = new ThreadPool(
            nodeSettings,
            new FixedExecutorBuilder(
                nodeSettings,
                ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME,
                1,
                -1,
                "thread_pool." + ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        bufferPool.close();
        if (nativeAllocator != null) {
            nativeAllocator.close();
            nativeAllocator = null;
        }
        super.tearDown();
    }

    /** A scalar keyword column. */
    private MappedFieldType kw(String name) {
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType(name);
        assignTestCapabilities(ft, PARQUET_DATA_FORMAT);
        return ft;
    }

    /** A multi_value keyword column, optionally stamped into a correlated group. */
    private MappedFieldType kwList(String name, String group) {
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType(name);
        ft.setMultiValued(true);
        ft.setCorrelationGroup(group);
        assignTestCapabilities(ft, PARQUET_DATA_FORMAT);
        return ft;
    }

    private MappedFieldType lng(String name) {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
        assignTestCapabilities(ft, PARQUET_DATA_FORMAT);
        return ft;
    }

    private MappedFieldType dateNanos(String name) {
        return dateNanos(name, false, null);
    }

    /** A date_nanos column, optionally multi_value and stamped into a correlated group. */
    private MappedFieldType dateNanos(String name, boolean multiValue, String group) {
        MappedFieldType ft = new DateFieldMapper.DateFieldType(name, DateFieldMapper.Resolution.NANOSECONDS);
        if (multiValue) {
            ft.setMultiValued(true);
            ft.setCorrelationGroup(group);
        }
        assignTestCapabilities(ft, PARQUET_DATA_FORMAT);
        return ft;
    }

    /** A flat_object column: {@code MAP<utf8,utf8>}, or {@code LIST<MAP>} when multi-valued. */
    private MappedFieldType flat(String name, boolean multiValue, String group) {
        MappedFieldType ft = new FlatObjectFieldMapper.FlatObjectFieldType(name, null, true, true);
        if (multiValue) {
            ft.setMultiValued(true);
            ft.setCorrelationGroup(group);
        }
        assignTestCapabilities(ft, PARQUET_DATA_FORMAT);
        return ft;
    }

    private static Field arrow(MappedFieldType ft) {
        ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
        return pf.toArrowField(ft.name(), ft.isMultiValued());
    }

    private static List<Map.Entry<String, String>> attrs(String... kv) {
        List<Map.Entry<String, String>> out = new ArrayList<>();
        for (int i = 0; i < kv.length; i += 2) {
            out.add(Map.entry(kv[i], kv[i + 1]));
        }
        return out;
    }

    public void testGenerateOtelTracesParquet() throws Exception {
        // Defaults to the test temp directory so the suite passes under the test security manager with
        // no extra flags. Point [otel.parquet.out] somewhere durable to keep the file for inspection,
        // which also needs -Dtests.security.manager=false since that path is outside the sandbox.
        String configured = System.getProperty("otel.parquet.out");
        Path out = configured != null ? Path.of(configured) : createTempDir().resolve("otel_traces.parquet");
        int rowCount = Integer.getInteger("otel.parquet.rows", 12);
        Files.deleteIfExists(out);

        // The span-level columns, then the two correlated groups. Mirrors the ClickHouse otel_traces
        // column list, restricted to what the mapping accepts today (see the note printed at the end).
        MappedFieldType timestamp = dateNanos("Timestamp");
        MappedFieldType traceId = kw("TraceId");
        MappedFieldType spanId = kw("SpanId");
        MappedFieldType parentSpanId = kw("ParentSpanId");
        MappedFieldType spanName = kw("SpanName");
        MappedFieldType spanKind = kw("SpanKind");
        MappedFieldType serviceName = kw("ServiceName");
        MappedFieldType statusCode = kw("StatusCode");
        MappedFieldType duration = lng("Duration");
        MappedFieldType resourceAttrs = flat("ResourceAttributes", false, null);
        MappedFieldType spanAttrs = flat("SpanAttributes", false, null);
        MappedFieldType eventsTimestamp = dateNanos("Events.Timestamp", true, "Events");
        MappedFieldType eventsName = kwList("Events.Name", "Events");
        MappedFieldType eventsAttrs = flat("Events.Attributes", true, "Events");
        MappedFieldType linksTraceId = kwList("Links.TraceId", "Links");
        MappedFieldType linksSpanId = kwList("Links.SpanId", "Links");
        MappedFieldType linksAttrs = flat("Links.Attributes", true, "Links");

        List<MappedFieldType> columns = List.of(
            timestamp,
            traceId,
            spanId,
            parentSpanId,
            spanName,
            spanKind,
            serviceName,
            statusCode,
            duration,
            resourceAttrs,
            spanAttrs,
            eventsTimestamp,
            eventsName,
            eventsAttrs,
            linksTraceId,
            linksSpanId,
            linksAttrs
        );

        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : columns) {
            fields.add(arrow(ft));
        }
        fields.addAll(metadataFields());
        Schema schema = new Schema(fields);

        VSRManager manager = new VSRManager(out.toString(), indexSettings, schema, bufferPool, 50000, threadPool, 0L);
        try {
            long base = 1755684000000000000L; // 2025-08-20T10:00:00Z in epoch nanos
            String[] services = { "checkout", "payment", "cart" };
            String[] spans = { "GET /checkout", "POST /pay", "GET /cart" };
            String[] langs = { "java", "python", "go" };

            for (int row = 0; row < rowCount; row++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.setRowId(DocumentInput.ROW_ID_FIELD, row);

                int svc = row % 3;
                boolean errored = row % 4 == 0;
                doc.addField(timestamp, base + row * 1_000_000_000L);
                // Three spans per trace, so a trace lookup returns several rows.
                doc.addField(traceId, String.format(java.util.Locale.ROOT, "trace-%08d", row / 3));
                doc.addField(spanId, String.format(java.util.Locale.ROOT, "span-%08d", row));
                doc.addField(parentSpanId, row % 3 == 0 ? "" : String.format(java.util.Locale.ROOT, "span-%08d", row - 1));
                doc.addField(spanName, spans[svc]);
                doc.addField(spanKind, "SPAN_KIND_SERVER");
                doc.addField(serviceName, services[svc]);
                doc.addField(statusCode, errored ? "STATUS_CODE_ERROR" : "STATUS_CODE_OK");
                doc.addField(duration, (row + 1) * 1_500_000L);
                doc.addField(resourceAttrs, attrs("host.name", "host-" + svc, "telemetry.sdk.language", langs[svc]));
                doc.addField(spanAttrs, attrs("http.method", "GET", "http.status_code", errored ? "500" : "200"));

                // Two events per errored span, one otherwise. Each addField call is one element, which
                // is how the document parser hands over the elements of a JSON array.
                if (errored) {
                    doc.addField(eventsTimestamp, base + row * 1_000_000_000L + 1L);
                    doc.addField(eventsName, "exception");
                    doc.addField(eventsAttrs, attrs("exception.type", "IOError", "exception.message", "disk full"));
                    doc.addField(eventsTimestamp, base + row * 1_000_000_000L + 2L);
                    doc.addField(eventsName, "retry");
                    doc.addField(eventsAttrs, attrs("retry.count", "3"));
                } else {
                    doc.addField(eventsTimestamp, base + row * 1_000_000_000L + 1L);
                    doc.addField(eventsName, "cache.hit");
                    doc.addField(eventsAttrs, attrs("cache.key", "k" + row));
                }

                // One link on every third span.
                if (row % 3 == 2) {
                    doc.addField(linksTraceId, String.format(java.util.Locale.ROOT, "trace-%08d", (row / 3 + 1)));
                    doc.addField(linksSpanId, "span-linked-" + row);
                    doc.addField(linksAttrs, attrs("link.kind", "follows_from"));
                }

                manager.addDocument(doc);
            }

            var metadata = manager.flush();
            assertNotNull(metadata);
            assertEquals(rowCount, metadata.numRows());
        } finally {
            manager.close();
        }

        assertTrue("parquet file should exist at " + out, Files.exists(out));
        logger.info("WROTE {} ({} bytes)", out, Files.size(out));
    }

    /**
     * {@code Events.Timestamp} as {@code date_nanos, multi_value: true} is a {@code LIST<int64>} of
     * nanos, so the generated file now matches the ClickHouse column list for the whole group.
     */
    public void testEventsTimestampSupportsMultiValue() {
        ParquetField dateNanosField = ArrowFieldRegistry.getParquetField(DateFieldMapper.DATE_NANOS_CONTENT_TYPE);
        assertTrue(dateNanosField.supportsMultiValue());
        Field arrowField = dateNanosField.toArrowField("Events.Timestamp", true);
        assertEquals(ArrowType.List.INSTANCE, arrowField.getType());
        assertEquals(
            new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, null),
            arrowField.getChildren().get(0).getType()
        );
    }
}
