/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.backend.ShardExecutionContext;
import org.opensearch.analytics.exec.DefaultShardExecutionContext;
import org.opensearch.be.lucene.predicate.QueryBuilderSerializer;
import org.opensearch.be.lucene.predicate.RexToQueryBuilderConverter;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Lucene EngineBridge implementation.
 * <p>
 * Planner side ({@link #convertFragment}): RexNode → QueryBuilder → byte[].
 * Executor side: delegates Lucene query execution to {@link LuceneIndexFilterProvider}
 * for Weight creation, per-segment scoring, and doc range collection.
 * Packages results as Arrow BitVector.
 */
public class LuceneFilterExecutor implements EngineBridge<byte[], Iterator<VectorSchemaRoot>, RelNode> {

    /** Column name for the document ID bitset in the result schema. */
    public static final String DOC_IDS_COLUMN = "doc_ids";

    private static final Schema DOC_IDS_SCHEMA = new Schema(
        List.of(Field.nullable(DOC_IDS_COLUMN, new ArrowType.Bool()))
    );

    // Cached shard-level resources
    private Engine.Searcher engineSearcher;
    private QueryShardContext queryShardContext;
    private BufferAllocator allocator;

    // Upstream execution provider — handles Weight, Scorer, doc collection
    private final LuceneIndexFilterProvider filterProvider = new LuceneIndexFilterProvider();
    // Cached filter contexts per query fragment
    private final Map<ByteBuffer, LuceneIndexFilterContext> contextCache = new HashMap<>();

    @Override
    public void initialize(ShardExecutionContext context) {
        if (context instanceof DefaultShardExecutionContext shardCtx == false) {
            throw new IllegalArgumentException(
                "LuceneFilterExecutor requires DefaultShardExecutionContext, got: "
                    + (context == null ? "null" : context.getClass().getSimpleName())
            );
        }
        DefaultShardExecutionContext shardCtx = (DefaultShardExecutionContext) context;
        Engine.Searcher searcher = shardCtx.indexShard().acquireSearcher("lucene-analytics");
        try {
            this.queryShardContext = shardCtx.createQueryShardContext(searcher);
            this.engineSearcher = searcher;
        } catch (Exception e) {
            searcher.close();
            throw e;
        }
    }

    @Override
    public void close() {
        // Release cached collectors
        for (var entry : contextCache.entrySet()) {
            LuceneIndexFilterContext ctx = entry.getValue();
            ctx.close(); // releases all collectors via CollectorQueryLifecycleManager
        }
        collectorCache.clear();
        contextCache.clear();
        if (engineSearcher != null) {
            engineSearcher.close();
            engineSearcher = null;
            queryShardContext = null;
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    private BufferAllocator getAllocator() {
        if (allocator == null) {
            allocator = new RootAllocator();
        }
        return allocator;
    }

    /**
     * Gets or creates a LuceneIndexFilterContext for the given fragment.
     * Converts byte[] → QueryBuilder → Lucene Query, then delegates to
     * upstream LuceneIndexFilterProvider for Weight creation.
     */
    private LuceneIndexFilterContext getOrCreateContext(byte[] fragment) throws IOException {
        ByteBuffer key = ByteBuffer.wrap(fragment);
        LuceneIndexFilterContext cached = contextCache.get(key);
        if (cached != null) {
            return cached;
        }

        QueryBuilder queryBuilder = QueryBuilderSerializer.deserialize(fragment);
        Query query = queryBuilder.toQuery(queryShardContext);
        DirectoryReader directoryReader = engineSearcher.getDirectoryReader();
        LuceneIndexFilterContext ctx = filterProvider.createContext(query, directoryReader);
        contextCache.put(key, ctx);
        return ctx;
    }

    // --- Planner side ---

    @Override
    public byte[] convertFragment(RelNode fragment) {
        return convertFragment(fragment, null);
    }

    public byte[] convertFragment(RelNode fragment, MapperService mapperService) {
        Objects.requireNonNull(fragment, "RelNode fragment must not be null");
        if (!(fragment instanceof LogicalFilter)) {
            throw new IllegalArgumentException(
                "Lucene backend expects a LogicalFilter, got: " + fragment.getClass().getSimpleName()
            );
        }
        LogicalFilter filter = (LogicalFilter) fragment;
        RexNode condition = filter.getCondition();
        RelDataType inputRowType = filter.getInput().getRowType();
        RexToQueryBuilderConverter converter = new RexToQueryBuilderConverter(inputRowType, mapperService);
        QueryBuilder queryBuilder = converter.convert(condition);
        return QueryBuilderSerializer.serialize(queryBuilder);
    }

    // --- Executor side ---

    @Override
    public Iterator<VectorSchemaRoot> execute(byte[] fragment) {
        if (fragment == null || fragment.length == 0) {
            throw new IllegalArgumentException("Fragment byte array must not be null or empty");
        }
        // Validate bytes are well-formed
        QueryBuilderSerializer.deserialize(fragment);
        if (engineSearcher == null) {
            return createEmptyResult();
        }
        return executeAllSegments(fragment);
    }

    // Cached collector keys per (fragment, segmentOrd) — reuses Scorer across batch calls
    private final Map<Long, Integer> collectorCache = new HashMap<>();

    private static long collectorCacheKey(ByteBuffer fragmentKey, int segmentOrd) {
        return ((long) fragmentKey.hashCode() << 32) | (segmentOrd & 0xFFFFFFFFL);
    }

    /**
     * Executes for a specific segment and doc ID range.
     * Reuses the Scorer across sequential batch calls within the same segment.
     */
    public Iterator<VectorSchemaRoot> executeForSegment(byte[] fragment, int segmentOrd, int startDocId, int endDocId) {
        if (fragment == null || fragment.length == 0) {
            throw new IllegalArgumentException("Fragment byte array must not be null or empty");
        }
        if (engineSearcher == null) {
            return createEmptyResult();
        }
        try {
            LuceneIndexFilterContext ctx = getOrCreateContext(fragment);
            ByteBuffer fragmentKey = ByteBuffer.wrap(fragment);
            long cacheKey = collectorCacheKey(fragmentKey, segmentOrd);

            Integer collectorKey = collectorCache.get(cacheKey);
            if (collectorKey == null) {
                collectorKey = filterProvider.createCollector(ctx, segmentOrd, startDocId, endDocId);
                collectorCache.put(cacheKey, collectorKey);
            }

            long[] bits = filterProvider.collectDocs(ctx, collectorKey, startDocId, endDocId);
            return createResultFromLongArray(bits, endDocId - startDocId);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed during Lucene segment query execution: " + e.getMessage(), e);
        }
    }

    public int getSegmentCount() {
        if (engineSearcher == null) return 0;
        return engineSearcher.getIndexReader().leaves().size();
    }

    public int getSegmentMaxDoc(int segmentOrd) {
        if (engineSearcher == null) return 0;
        var leaves = engineSearcher.getIndexReader().leaves();
        if (segmentOrd < 0 || segmentOrd >= leaves.size()) {
            throw new IllegalArgumentException("Invalid segment ordinal: " + segmentOrd);
        }
        return leaves.get(segmentOrd).reader().maxDoc();
    }

    // --- Result packaging ---

    private Iterator<VectorSchemaRoot> executeAllSegments(byte[] fragment) {
        try {
            LuceneIndexFilterContext ctx = getOrCreateContext(fragment);
            int totalMaxDoc = 0;
            for (int i = 0; i < ctx.segmentCount(); i++) {
                totalMaxDoc += ctx.segmentMaxDoc(i);
            }

            java.util.BitSet globalBitSet = new java.util.BitSet(totalMaxDoc);
            int docBase = 0;
            for (int seg = 0; seg < ctx.segmentCount(); seg++) {
                int segMaxDoc = ctx.segmentMaxDoc(seg);
                int collectorKey = filterProvider.createCollector(ctx, seg, 0, segMaxDoc);
                long[] bits = filterProvider.collectDocs(ctx, collectorKey, 0, segMaxDoc);
                filterProvider.releaseCollector(ctx, collectorKey);

                java.util.BitSet segBits = java.util.BitSet.valueOf(bits);
                for (int doc = segBits.nextSetBit(0); doc >= 0; doc = segBits.nextSetBit(doc + 1)) {
                    globalBitSet.set(docBase + doc);
                }
                docBase += segMaxDoc;
            }

            return createResultFromJavaBitSet(globalBitSet, totalMaxDoc);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed during Lucene query execution: " + e.getMessage(), e);
        }
    }

    private Iterator<VectorSchemaRoot> createEmptyResult() {
        BufferAllocator alloc = getAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(DOC_IDS_SCHEMA, alloc);
        BitVector docIds = (BitVector) root.getVector(DOC_IDS_COLUMN);
        docIds.allocateNew(0);
        docIds.setValueCount(0);
        root.setRowCount(0);
        return Collections.singletonList(root).iterator();
    }

    private Iterator<VectorSchemaRoot> createResultFromLongArray(long[] bits, int rangeSize) {
        java.util.BitSet bitSet = java.util.BitSet.valueOf(bits);
        return createResultFromJavaBitSet(bitSet, rangeSize);
    }

    private Iterator<VectorSchemaRoot> createResultFromJavaBitSet(java.util.BitSet bitSet, int totalDocs) {
        BufferAllocator alloc = getAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(DOC_IDS_SCHEMA, alloc);
        BitVector docIds = (BitVector) root.getVector(DOC_IDS_COLUMN);
        docIds.allocateNew(totalDocs);
        for (int i = 0; i < totalDocs; i++) {
            docIds.setSafe(i, bitSet.get(i) ? 1 : 0);
        }
        docIds.setValueCount(totalDocs);
        root.setRowCount(totalDocs);
        return Collections.singletonList(root).iterator();
    }
}
