/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene-side {@link SearchExecEngine}. Mirrors {@code DatafusionSearchExecEngine}'s role
 * for the Lucene backend: takes the {@link LuceneSearcherState} produced upstream by the
 * instruction handler, executes the operation, and returns an {@link EngineResultStream}
 * the framework drains into the Flight transport.
 *
 * <p>Today's only operation is the count fast path —
 * {@link org.apache.lucene.search.IndexSearcher#count(org.apache.lucene.search.Query)} —
 * exported through the Arrow C-Data interface so the result VSR has the same
 * foreign-allocation-managed buffer layout DataFusion's result stream produces. Pure-Java
 * {@code setSafe}-built VSRs don't survive Flight's {@code VectorTransfer.transferRoot};
 * see {@link LuceneResultStream} for the detailed comparison.
 *
 * <p>No deletes gate. {@code IndexSearcher.count} is self-healing: per-leaf
 * {@code Weight.count(leaf)} returns -1 on dirty leaves and falls back to full iteration —
 * correct under deletes, just slower. Even the slow case is substantially cheaper than
 * DataFusion decoding rows.
 *
 * @opensearch.internal
 */
final class LuceneSearchExecEngine implements SearchExecEngine<ShardScanExecutionContext, EngineResultStream> {

    private static final Logger LOGGER = LogManager.getLogger(LuceneSearchExecEngine.class);

    private final LuceneSearcherState state;

    LuceneSearchExecEngine(LuceneSearcherState state) {
        this.state = state;
    }

    @Override
    public void prepare(ShardScanExecutionContext context) {
        // No preparation needed — the LuceneSearcherState was fully built by the instruction
        // handler. {@code prepare} is part of the SearchExecEngine contract for backends that
        // need to assemble plans from the context (e.g. DataFusion); Lucene has nothing to do.
    }

    @Override
    public EngineResultStream execute(ShardScanExecutionContext context) throws IOException {
        long count = state.searcher().count(state.filterQuery());
        LOGGER.debug(
            "[lucene-count] shardId={} query={} count={} columns={}",
            context.getShardId(),
            state.filterQuery(),
            count,
            state.outputColumnNames()
        );
        BufferAllocator allocator = context.getAllocator();
        Schema schema = buildSchema(state.outputColumnNames());
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        boolean transferred = false;
        try {
            populateBatchToCData(allocator, schema, state.outputColumnNames(), count, array, arrowSchema);
            LuceneResultStream stream = new LuceneResultStream(array, arrowSchema, allocator);
            transferred = true;
            return stream;
        } finally {
            if (transferred == false) {
                try {
                    array.close();
                } finally {
                    arrowSchema.close();
                }
            }
        }
    }

    private static Schema buildSchema(List<String> columnNames) {
        FieldType int64Nullable = new FieldType(true, new ArrowType.Int(64, true), null);
        List<Field> fields = new ArrayList<>(columnNames.size());
        for (String name : columnNames) {
            fields.add(new Field(name, int64Nullable, null));
        }
        return new Schema(fields);
    }

    /**
     * Builds a one-row scratch VSR carrying {@code count} for every column, exports it to
     * the supplied {@code array}/{@code arrowSchema} via the Arrow C-Data interface, then
     * closes the scratch VSR. Mirrors the export side of {@code DatafusionResultStream}'s
     * contract: the populated {@link ArrowArray} is what {@link LuceneResultStream}
     * re-imports into its result VSR — same call shape DataFusion uses for native batches.
     */
    private static void populateBatchToCData(
        BufferAllocator allocator,
        Schema schema,
        List<String> columnNames,
        long count,
        ArrowArray array,
        ArrowSchema arrowSchema
    ) {
        VectorSchemaRoot scratch = VectorSchemaRoot.create(schema, allocator);
        try {
            scratch.allocateNew();
            for (int i = 0; i < columnNames.size(); i++) {
                BigIntVector v = (BigIntVector) scratch.getVector(i);
                v.setSafe(0, count);
            }
            scratch.setRowCount(1);
            try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                Data.exportVectorSchemaRoot(allocator, scratch, dictProvider, array, arrowSchema);
            }
        } finally {
            scratch.close();
        }
    }
}
