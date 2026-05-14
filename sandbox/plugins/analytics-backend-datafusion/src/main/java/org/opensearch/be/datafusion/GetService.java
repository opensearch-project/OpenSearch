/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.substrait.proto.FetchRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;

/**
 * Non-Lucene get-by-id implementation for DataFusion-backed indexes.
 *
 * <p>Resolves the {@code _id} term via the sibling Lucene backend's {@code DirectoryReader},
 * maps the matching global docId to a {@code (writerGeneration, rowId)} pair, locates the
 * parquet file in the {@link CatalogSnapshot}, and issues a Substrait
 * {@code Fetch(offset=rowId, count=1) → Read(NamedTable=[indexName])} plan through the
 * DataFusion native runtime. The single returned Arrow record batch is flattened into a
 * JSON source with reserved columns stripped.
 */
@ExperimentalApi
public class GetService {

    private static final Logger logger = LogManager.getLogger(GetService.class);

    /** Reserved columns that must not leak into the JSON _source payload. */
    private static final Set<String> RESERVED_FIELDS = Set.of(
        IdFieldMapper.NAME,
        "_seq_no",
        "_primary_term",
        "_version",
        "_doc_count",
        "_size",
        "_routing",
        "_ignored",
        DocumentInput.ROW_ID_FIELD
    );

    private static final String PARQUET_FORMAT = "parquet";

    private final LuceneReaderAccessor luceneReaderAccessor;
    private final SubstraitPlanFactory planFactory;
    private final NativeExecutor executor;

    /** Production constructor. */
    public GetService(DataFusionPlugin dfPlugin) {
        this(
            new RegistryLuceneReaderAccessor(requireRegistry(dfPlugin)),
            new SubstraitPlanFactory(),
            new NativeBridgeExecutor(dfPlugin)
        );
    }

    private static DataFormatRegistry requireRegistry(DataFusionPlugin dfPlugin) {
        DataFormatRegistry registry = dfPlugin.getDataFormatRegistry();
        if (registry == null) {
            throw new IllegalStateException("DataFormatRegistry not initialized on DataFusionPlugin");
        }
        return registry;
    }

    /** Test constructor accepting seam interfaces. */
    GetService(LuceneReaderAccessor luceneReaderAccessor, SubstraitPlanFactory planFactory, NativeExecutor executor) {
        this.luceneReaderAccessor = luceneReaderAccessor;
        this.planFactory = planFactory;
        this.executor = executor;
    }

    /**
     * Resolves the document identified by {@code get} against {@code reader}.
     *
     * @return {@link DocumentLookupResult#notFound(String)} if the id is not present, otherwise a
     *         result with {@code exists==true} and the JSON source.
     */
    public DocumentLookupResult getById(Engine.Get get, IndexReaderProvider.Reader reader, String indexName) throws IOException {
        DirectoryReader luceneReader = luceneReaderAccessor.directoryReader(reader);
        if (luceneReader == null) {
            throw new IllegalStateException("No Lucene DirectoryReader available on the acquired reader");
        }

        BytesRef idBytes = Uid.encodeId(get.id());
        IndexSearcher searcher = new IndexSearcher(luceneReader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term(IdFieldMapper.NAME, idBytes)), 1);
        if (topDocs.scoreDocs.length == 0) {
            return DocumentLookupResult.notFound(get.id());
        }
        ScoreDoc hit = topDocs.scoreDocs[0];
        int leafOrd = ReaderUtil.subIndex(hit.doc, luceneReader.leaves());
        LeafReaderContext leafCtx = luceneReader.leaves().get(leafOrd);
        int localDocId = hit.doc - leafCtx.docBase;

        SortedNumericDocValues rowIdDv = leafCtx.reader().getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
        if (rowIdDv == null || rowIdDv.advanceExact(localDocId) == false) {
            throw new IllegalStateException("Leaf segment missing " + DocumentInput.ROW_ID_FIELD + " doc values");
        }
        long rowId = rowIdDv.nextValue();

        if ((leafCtx.reader() instanceof SegmentReader) == false) {
            throw new IllegalStateException("Expected SegmentReader leaf, got " + leafCtx.reader().getClass());
        }
        SegmentReader segReader = (SegmentReader) leafCtx.reader();
        SegmentCommitInfo sci = segReader.getSegmentInfo();
        String genAttr = sci.info.getAttribute("writer_generation");
        if (genAttr == null) {
            throw new IllegalStateException("Leaf segment missing writer_generation attribute");
        }
        long writerGeneration;
        try {
            writerGeneration = Long.parseLong(genAttr);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid writer_generation attribute: [" + genAttr + "]", e);
        }

        CatalogSnapshot snapshot = reader.catalogSnapshot();
        WriterFileSet parquetSet = findParquetSet(snapshot, writerGeneration);

        String parquetFile = parquetSet.files().iterator().next();
        Map<String, Object> row = executor.executeSingleRow(parquetSet.directory(), parquetFile, indexName, rowId);
        if (row == null) {
            // Lucene hit but empty parquet fetch — treat as a rare race (e.g., parquet not yet
            // visible to the native reader) rather than an error.
            logger.debug(
                "get-by-id hit in Lucene but empty parquet fetch for id=[{}] gen=[{}] rowId=[{}]",
                get.id(),
                writerGeneration,
                rowId
            );
            return DocumentLookupResult.notFound(get.id());
        }

        long seqNo = extractLong(row, "_seq_no", SequenceNumbers.UNASSIGNED_SEQ_NO);
        long primaryTerm = extractLong(row, "_primary_term", SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        long version = extractLong(row, "_version", Versions.NOT_FOUND);

        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (RESERVED_FIELDS.contains(e.getKey())) continue;
            filtered.put(e.getKey(), e.getValue());
        }

        BytesReference source;
        try (XContentBuilder xcb = XContentFactory.jsonBuilder()) {
            xcb.map(filtered);
            source = BytesReference.bytes(xcb);
        }

        return new DocumentLookupResult(get.id(), version, true, source, seqNo, primaryTerm, Map.of(), Map.of());
    }

    private static WriterFileSet findParquetSet(CatalogSnapshot snapshot, long writerGeneration) {
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet candidate = segment.dfGroupedSearchableFiles().get(PARQUET_FORMAT);
            if (candidate == null) continue;
            if (candidate.writerGeneration() != writerGeneration) continue;
            if (candidate.files().isEmpty()) continue;
            return candidate;
        }
        throw new IllegalStateException("No parquet file-set for writer_generation=" + writerGeneration + " in snapshot");
    }

    private static long extractLong(Map<String, Object> row, String key, long fallback) {
        Object v = row.get(key);
        if (v == null) return fallback;
        if (v instanceof Number) return ((Number) v).longValue();
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Strategy for getting at the Lucene {@code DirectoryReader}. Production code uses
     * reflection to avoid a compile-time dep on the Lucene backend module; tests supply a
     * direct override.
     */
    interface LuceneReaderAccessor {
        DirectoryReader directoryReader(IndexReaderProvider.Reader reader) throws IOException;
    }

    /**
     * Registry-based {@link LuceneReaderAccessor}. Looks up the Lucene {@link DataFormat} by name
     * on the {@link DataFormatRegistry}. Avoids a compile-time dep on the Lucene backend module
     * (which would invert the module graph: DF is downstream of analytics-engine, not of
     * analytics-backend-lucene) and avoids reflection across plugin classloaders (each plugin has
     * its own isolated classloader at runtime).
     */
    static final class RegistryLuceneReaderAccessor implements LuceneReaderAccessor {
        private static final String LUCENE_FORMAT_NAME = "lucene";
        private final DataFormatRegistry registry;

        RegistryLuceneReaderAccessor(DataFormatRegistry registry) {
            this.registry = registry;
        }

        @Override
        public DirectoryReader directoryReader(IndexReaderProvider.Reader reader) {
            DataFormat luceneFormat;
            try {
                luceneFormat = registry.format(LUCENE_FORMAT_NAME);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("Lucene data format not registered — get-by-id requires the analytics-backend-lucene plugin", e);
            }
            return reader.getReader(luceneFormat, DirectoryReader.class);
        }
    }

    /**
     * Builds the narrow Substrait "fetch-one-row-from-named-table" plan. Exposed as a seam for
     * unit tests that want to assert plan shape without running the native runtime.
     */
    static final class SubstraitPlanFactory {
        byte[] buildFetchPlan(String tableName, long offset) {
            // Empty NamedStruct satisfies the required-presence contract; DataFusion's listing
            // table resolves the actual parquet schema from the file footer at read time.
            NamedStruct baseSchema = NamedStruct.newBuilder().setStruct(Type.Struct.newBuilder().build()).build();
            Rel read = Rel.newBuilder()
                .setRead(
                    ReadRel.newBuilder()
                        .setBaseSchema(baseSchema)
                        .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(tableName).build())
                        .build()
                )
                .build();
            Rel fetch = Rel.newBuilder().setFetch(FetchRel.newBuilder().setInput(read).setOffset(offset).setCount(1L).build()).build();
            RelRoot root = RelRoot.newBuilder().setInput(fetch).build();
            Plan plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(root).build()).build();
            return plan.toByteArray();
        }
    }

    /**
     * Executes a {@code SELECT * FROM tableName LIMIT 1 OFFSET rowId} query over a single parquet
     * file via the DataFusion native bridge and returns the first row as a map. Uses
     * {@link NativeBridge#sqlToSubstrait} to let the native planner project the parquet file's
     * true schema — passing a hand-rolled Substrait plan with an empty {@code base_schema} yields
     * zero-column projection.
     */
    interface NativeExecutor {
        Map<String, Object> executeSingleRow(String parquetDir, String parquetFile, String tableName, long rowId) throws IOException;
    }

    /**
     * Production {@link NativeExecutor} driving {@link NativeBridge}. Spins up a short-lived
     * reader scoped to one parquet file, executes the plan, imports the single resulting batch
     * via the Arrow C Data Interface, and flattens the first row into a Java map.
     */
    static final class NativeBridgeExecutor implements NativeExecutor {

        private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

        private final DataFusionPlugin dfPlugin;

        NativeBridgeExecutor(DataFusionPlugin dfPlugin) {
            this.dfPlugin = dfPlugin;
        }

        @Override
        public Map<String, Object> executeSingleRow(String parquetDir, String parquetFile, String tableName, long rowId)
            throws IOException {
            long runtimePtr = dfPlugin.getDataFusionService().getNativeRuntime().get();
            // ReaderHandle registers the native pointer with NativeHandle so downstream
            // validatePointer() calls in executeQueryAsync() find it in the live set.
            try (ReaderHandle readerHandle = new ReaderHandle(parquetDir, new String[] { parquetFile })) {
                long readerPtr = readerHandle.getPointer();
                // Let the native planner discover the parquet schema from the file footer and
                // project every column — hand-rolling a Substrait ReadRel with an empty
                // base_schema collapses projection to zero columns.
                String sql = "SELECT * FROM " + tableName + " LIMIT 1 OFFSET " + rowId;
                byte[] substraitPlan = NativeBridge.sqlToSubstrait(readerPtr, tableName, sql, runtimePtr);
                long streamPtr;
                CompletableFuture<Long> future = new CompletableFuture<>();
                WireConfigSnapshot configSnapshot = dfPlugin.getDatafusionSettings().getSnapshot();
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment configSegment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                    configSnapshot.writeTo(configSegment);
                    NativeBridge.executeQueryAsync(
                        readerPtr,
                        tableName,
                        substraitPlan,
                        runtimePtr,
                        0L,
                        configSegment.address(),
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
                    try {
                        streamPtr = future.join();
                    } catch (Exception e) {
                        throw new IOException("DataFusion get-by-id query failed", e);
                    }
                }
                try (
                    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                    StreamHandle streamHandle = new StreamHandle(streamPtr, dfPlugin.getDataFusionService().getNativeRuntime());
                    DatafusionResultStream stream = new DatafusionResultStream(streamHandle, allocator)
                ) {
                    var iter = stream.iterator();
                    if (!iter.hasNext()) return null;
                    var batch = iter.next();
                    VectorSchemaRoot root = batch.getArrowRoot();
                    try {
                        if (root.getRowCount() == 0) return null;
                        return rowToMap(root, 0);
                    } finally {
                        root.close();
                    }
                }
            }
        }

        private static Map<String, Object> rowToMap(VectorSchemaRoot root, int rowIdx) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Field field : root.getSchema().getFields()) {
                String name = field.getName();
                FieldVector vec = root.getVector(name);
                Object converted = convert(vec, rowIdx);
                if (converted != null) {
                    out.put(name, converted);
                }
            }
            return out;
        }

        private static Object convert(FieldVector vec, int idx) {
            if (vec == null || vec.isNull(idx)) return null;
            ArrowType type = vec.getField().getType();
            ArrowType.ArrowTypeID id = type.getTypeID();
            Object raw = vec.getObject(idx);
            switch (id) {
                case Utf8:
                case LargeUtf8:
                case Utf8View:
                    return raw == null ? null : raw.toString();
                case Int:
                    return raw instanceof Number ? ((Number) raw).longValue() : raw;
                case FloatingPoint:
                    return raw instanceof Number ? ((Number) raw).doubleValue() : raw;
                case Bool:
                    return raw;
                case Timestamp:
                    if (raw instanceof Number) {
                        ArrowType.Timestamp ts = (ArrowType.Timestamp) type;
                        return ISO_FORMATTER.format(toInstant(((Number) raw).longValue(), ts.getUnit()));
                    }
                    return raw == null ? null : raw.toString();
                case Date:
                    return raw == null ? null : raw.toString();
                case Binary:
                case LargeBinary:
                case FixedSizeBinary:
                case BinaryView:
                    // Skip — used for the _id byte encoding; already reserved, but drop here too.
                    return null;
                default:
                    // TODO type coverage (list, struct, decimal)
                    return null;
            }
        }

        private static Instant toInstant(long v, TimeUnit unit) {
            switch (unit) {
                case SECOND:
                    return Instant.ofEpochSecond(v);
                case MILLISECOND:
                    return Instant.ofEpochMilli(v);
                case MICROSECOND:
                    return Instant.ofEpochSecond(v / 1_000_000L, (v % 1_000_000L) * 1_000L);
                case NANOSECOND:
                default:
                    return Instant.ofEpochSecond(v / 1_000_000_000L, v % 1_000_000_000L);
            }
        }
    }

    /** Package-private accessor for unit tests: the reserved-column skip-list. */
    static Set<String> reservedFields() {
        return RESERVED_FIELDS;
    }
}
