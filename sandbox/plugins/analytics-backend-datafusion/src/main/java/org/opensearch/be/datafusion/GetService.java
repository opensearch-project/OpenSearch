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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.ArrowValues;
import org.opensearch.analytics.spi.DocumentLookupService;
import org.opensearch.analytics.spi.DocumentRowReader;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * DataFusion-backed get-by-id executor. The resolver maps an {@code _id} to a
 * {@code (writerGeneration, rowId)}; this locates the parquet file in the {@link CatalogSnapshot}
 * and reads that row via a Substrait scan on the native runtime, flattening the Arrow batch into a
 * source map.
 */
@ExperimentalApi
public class GetService implements Closeable {

    private static final Logger logger = LogManager.getLogger(GetService.class);

    private final DocumentRowReader executor;

    /** Production constructor. */
    public GetService(DataFusionPlugin dfPlugin) {
        this(new NativeBridgeExecutor(dfPlugin));
    }

    GetService(DocumentRowReader executor) {
        this.executor = executor;
    }

    /** Returns a core-layer DocumentLookupService wired with this backend's executor and the supplied document resolver. */
    public DocumentLookupService documentLookupService(DocumentMetadataResolver resolver) {
        return new DocumentLookupService(resolver, executor);
    }

    @Override
    public void close() throws IOException {
        if (executor instanceof Closeable c) {
            c.close();
        }
    }

    /**
     * Production executor driving {@link NativeBridge}. Spins up a short-lived
     * reader scoped to one parquet file, executes the plan, imports the single resulting batch
     * via the Arrow C Data Interface, and flattens the first row into a Java map.
     */
    static final class NativeBridgeExecutor implements DocumentRowReader, Closeable {

        private static final String GET_BY_ID_TABLE_ALIAS = "_t";
        private static final String PARQUET_FORMAT = "parquet";
        /** Empty Substrait plan — the internal-search path builds its plan natively and ignores it. */
        private static final byte[] EMPTY_PLAN = new byte[0];

        private static final long GET_BY_ID_TIMEOUT_MILLIS = 30_000L;

        private final DataFusionPlugin dfPlugin;
        private final BufferAllocator sharedAllocator = new RootAllocator(64 * 1024 * 1024);

        NativeBridgeExecutor(DataFusionPlugin dfPlugin) {
            this.dfPlugin = dfPlugin;
        }

        @Override
        public void close() {
            sharedAllocator.close();
        }

        @Override
        public String formatName() {
            return PARQUET_FORMAT;
        }

        @Override
        public Map<String, Object> executeSingleRow(long rowId, WriterFileSet parquetSet) throws IOException {
            if (rowId < 0) {
                throw new IllegalArgumentException("rowId must be non-negative, got: " + rowId);
            }
            String parquetDir = parquetSet.directory();
            String parquetFile = parquetSet.files().iterator().next();
            long runtimePtr = dfPlugin.getDataFusionService().getNativeRuntime().get();
            // ReaderHandle registers the native pointer with NativeHandle so downstream
            // validatePointer() calls in executeQueryAsync() find it in the live set.
            MonoFileWriterSet segment = MonoFileWriterSet.of(parquetDir, parquetSet.writerGeneration(), parquetFile, 0L);
            try (ReaderHandle readerHandle = new ReaderHandle(parquetDir, List.of(segment), null)) {
                long readerPtr = readerHandle.getPointer();
                // Internal-search get-by-row-id: the native side ignores Substrait and builds a
                // DataFrame plan filtering `__row_id__ = rowId` with pushdown enabled. __row_id__ is
                // the physical row position the parquet writer stamps at flush (sequential 0..N after
                // any index sort) and remaps into the Lucene secondary index, so the resolver's rowId
                // equals the row's __row_id__. An equality predicate returns exactly that row
                // independent of scan order and lets DataFusion prune row-groups/pages via the
                // column's min/max statistics.
                long streamPtr = executeInternalSearch(
                    readerPtr,
                    runtimePtr,
                    NativeBridge.INTERNAL_SEARCH_BY_ROW_ID,
                    rowId,
                    "DataFusion get-by-id query failed"
                );
                return readSingleRow(streamPtr);
            }
        }

        @Override
        public List<Map<String, Object>> executeRowsAboveSeqNo(List<WriterFileSet> fileSets, long seqNoFloor) throws IOException {
            long runtimePtr = dfPlugin.getDataFusionService().getNativeRuntime().get();
            List<Map<String, Object>> all = new ArrayList<>();
            for (WriterFileSet parquetSet : fileSets) {
                String parquetDir = parquetSet.directory();
                String parquetFile = parquetSet.files().iterator().next();
                MonoFileWriterSet writerSet = MonoFileWriterSet.of(parquetDir, parquetSet.writerGeneration(), parquetFile, 0L);
                try (ReaderHandle readerHandle = new ReaderHandle(parquetDir, List.of(writerSet), null)) {
                    long readerPtr = readerHandle.getPointer();
                    // Internal-search seq-no scan: the native side ignores Substrait and builds a
                    // DataFrame plan filtering `_seq_no > seqNoFloor`, projecting only the version
                    // metadata columns, with pushdown enabled.
                    long streamPtr = executeInternalSearch(
                        readerPtr,
                        runtimePtr,
                        NativeBridge.INTERNAL_SEARCH_SEQ_NO_ABOVE,
                        seqNoFloor,
                        "DataFusion range query failed"
                    );
                    all.addAll(readAllRows(streamPtr));
                }
            }
            return all;
        }

        private List<Map<String, Object>> readAllRows(long streamPtr) {
            List<Map<String, Object>> results = new ArrayList<>();
            try (
                StreamHandle streamHandle = new StreamHandle(streamPtr, dfPlugin.getDataFusionService().getNativeRuntime());
                DatafusionResultStream stream = new DatafusionResultStream(streamHandle, sharedAllocator)
            ) {
                var iter = stream.iterator();
                while (iter.hasNext()) {
                    var batch = iter.next();
                    try (VectorSchemaRoot root = batch.getArrowRoot()) {
                        FieldVector idVec = root.getVector(IdFieldMapper.NAME);
                        for (int i = 0; i < root.getRowCount(); i++) {
                            Map<String, Object> row = ArrowValues.toSourceMap(root, i);
                            if (idVec != null && !idVec.isNull(i)) {
                                row.put(IdFieldMapper.NAME, Uid.decodeId((byte[]) idVec.getObject(i)));
                            }
                            results.add(row);
                        }
                    }
                }
            }
            return results;
        }

        private Map<String, Object> readSingleRow(long streamPtr) {
            try (
                StreamHandle streamHandle = new StreamHandle(streamPtr, dfPlugin.getDataFusionService().getNativeRuntime());
                DatafusionResultStream stream = new DatafusionResultStream(streamHandle, sharedAllocator)
            ) {
                var iter = stream.iterator();
                if (!iter.hasNext()) return null;
                var batch = iter.next();
                try (VectorSchemaRoot root = batch.getArrowRoot()) {
                    if (root.getRowCount() == 0) return null;
                    return ArrowValues.toSourceMap(root, 0);
                }
            }
        }

        /**
         * Runs an engine-internal point lookup through {@link NativeBridge#executeQueryAsync} and
         * returns the result stream pointer. No Substrait is generated: the native side builds the
         * filter plan from {@code mode} + {@code bound} via the DataFrame API. The plan is empty,
         * but a valid {@link WireConfigSnapshot} is still required.
         */
        private long executeInternalSearch(long readerPtr, long runtimePtr, long mode, long bound, String errorMessage) throws IOException {
            CompletableFuture<Long> future = new CompletableFuture<>();
            WireConfigSnapshot configSnapshot = WireConfigSnapshot.builder(dfPlugin.getDatafusionSettings().getSnapshot()).build();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment configSegment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
                configSnapshot.writeTo(configSegment);
                NativeBridge.executeQueryAsync(
                    readerPtr,
                    GET_BY_ID_TABLE_ALIAS,
                    EMPTY_PLAN,
                    runtimePtr,
                    0L,
                    configSegment.address(),
                    mode,
                    bound,
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
                    return future.get(GET_BY_ID_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    throw new IOException(errorMessage, e);
                }
            }
        }
    }

}
