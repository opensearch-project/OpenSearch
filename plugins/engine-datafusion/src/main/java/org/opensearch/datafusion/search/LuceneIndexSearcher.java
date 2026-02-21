/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lucene index searcher with static JNI callback methods for Rust integration.
 *
 * Rust's JniShardSearcher calls these static methods via JNI to:
 * 1. Create partition scorers from a shard-level Weight
 * 2. Collect matching doc IDs as bitsets for each row group
 * 3. Release scorer resources
 *
 * The Weight pointer is a key into {@link #activeShardWeights} — Java owns the
 * Weight lifecycle, Rust borrows it for the duration of the native call.
 */
public class LuceneIndexSearcher {

    private static final Logger logger = LogManager.getLogger(LuceneIndexSearcher.class);

    // Active ShardWeight contexts (one per query)
    private static final Map<Long, ShardWeightContext> activeShardWeights = new ConcurrentHashMap<>();

    // Active PartitionScorer contexts (one per partition per segment)
    private static final Map<Long, PartitionScorerContext> activePartitionScorers = new ConcurrentHashMap<>();

    // ID generator for pointers
    private static final AtomicLong nextId = new AtomicLong(1);

    /**
     * Context holding a shard-level Weight and its segment leaves.
     */
    static class ShardWeightContext {
        final IndexSearcher searcher;
        final Weight weight;
        final List<LeafReaderContext> leaves;

        ShardWeightContext(IndexSearcher searcher, Weight weight, List<LeafReaderContext> leaves) {
            this.searcher = searcher;
            this.weight = weight;
            this.leaves = leaves;
        }
    }

    /**
     * Context holding a partition-scoped scorer for a single segment.
     */
    static class PartitionScorerContext {
        final DocIdSetIterator iterator;
        final int partitionMinDocId;
        final int partitionMaxDocId;
        int currentDoc;

        PartitionScorerContext(DocIdSetIterator iterator, int minDocId, int maxDocId) {
            this.iterator = iterator;
            this.partitionMinDocId = minDocId;
            this.partitionMaxDocId = maxDocId;
            this.currentDoc = -1;
        }
    }

    /**
     * Register a pre-built Weight for use by Rust via JNI.
     *
     * Called by the OpenSearch query path after creating the Weight from the
     * user's query. Returns a pointer (ID) that Rust uses to reference this Weight.
     *
     * @param searcher The IndexSearcher that created the Weight
     * @param weight   The pre-built Weight
     * @param leaves   Segment leaf contexts
     * @return pointer ID for this Weight context
     */
    public static long registerShardWeight(IndexSearcher searcher, Weight weight, List<LeafReaderContext> leaves) {
        long id = nextId.getAndIncrement();
        activeShardWeights.put(id, new ShardWeightContext(searcher, weight, leaves));
        return id;
    }

    public static int getShardWeightSegmentCount(long shardWeightPointer) {
        ShardWeightContext context = activeShardWeights.get(shardWeightPointer);
        return (context != null) ? context.leaves.size() : -1;
    }

    public static int getShardWeightSegmentMaxDoc(long shardWeightPointer, int segmentOrd) {
        ShardWeightContext context = activeShardWeights.get(shardWeightPointer);
        if (context == null || segmentOrd < 0 || segmentOrd >= context.leaves.size()) {
            return -1;
        }
        return context.leaves.get(segmentOrd).reader().maxDoc();
    }

    public static void releaseShardWeight(long shardWeightPointer) {
        activeShardWeights.remove(shardWeightPointer);
    }

    /**
     * Create a partition scorer for a specific segment and doc ID range.
     * Called by Rust's JniShardSearcher.collector().
     */
    public static long createPartitionScorerFromShard(long shardWeightPointer, int segmentOrd, int minDocId, int maxDocId) {
        ShardWeightContext shardCtx = activeShardWeights.get(shardWeightPointer);
        if (shardCtx == null) {
            logger.error("Invalid ShardWeight pointer: {}", shardWeightPointer);
            return -1;
        }

        if (segmentOrd < 0 || segmentOrd >= shardCtx.leaves.size()) {
            logger.error("Invalid segment ordinal: {}", segmentOrd);
            return -1;
        }

        try {
            LeafReaderContext leafContext = shardCtx.leaves.get(segmentOrd);
            long st = System.nanoTime();

            Scorer scorer = shardCtx.weight.scorer(leafContext);
            logger.info("scorer took : {} ms for : {}, {}, {} " , (System.nanoTime() - st) / 1_000_000, segmentOrd, minDocId,maxDocId);
            if (scorer == null) {
                return -1;  // No matches in this segment
            }

            DocIdSetIterator iterator = scorer.iterator();
            long id = nextId.getAndIncrement();
            activePartitionScorers.put(id, new PartitionScorerContext(iterator, minDocId, maxDocId));
            return id;

        } catch (Exception e) {
            logger.error("Error creating PartitionScorer for segment {}: {}", segmentOrd, e.getMessage());
            return -1;
        }
    }

    /**
     * Get matching doc IDs for a row group range as a bitset.
     * Called by Rust's JniShardSearcher SegmentCollector.collect().
     *
     * Returns a long[] representing a java.util.BitSet — each bit corresponds
     * to a doc ID relative to effectiveMin.
     */
    public static long[] getNextRowGroupDocs(long scorerPointer, int rowGroupMin, int rowGroupMax) {
        PartitionScorerContext context = activePartitionScorers.get(scorerPointer);
        if (context == null) {
            return new long[0];
        }

        int effectiveMin = Math.max(rowGroupMin, context.partitionMinDocId);
        int effectiveMax = Math.min(rowGroupMax, context.partitionMaxDocId);

        if (effectiveMin >= effectiveMax) {
            return new long[0];
        }

        java.util.BitSet bitSet = new java.util.BitSet(effectiveMax - effectiveMin);
        int offset = effectiveMin;

        try {
            DocIdSetIterator iterator = context.iterator;
            int docId = context.currentDoc;

            if (docId == DocIdSetIterator.NO_MORE_DOCS || docId >= context.partitionMaxDocId) {
                return new long[0];
            }

            if (docId < effectiveMin) {
                docId = iterator.advance(effectiveMin);
            }

            while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < effectiveMax) {
                bitSet.set(docId - offset);
                docId = iterator.nextDoc();
            }

            context.currentDoc = docId;

        } catch (Exception e) {
            logger.error("Error in getNextRowGroupDocs: {}", e.getMessage());
        }

        return bitSet.toLongArray();
    }

    public static void releasePartitionScorer(long scorerPointer) {
        activePartitionScorers.remove(scorerPointer);
    }
}
