/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineSearcher;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lucene searcher — produces bitsets of matching doc IDs on demand.
 * <p>
 * Supports two usage patterns:
 * <ul>
 *   <li>Direct: {@link #search(LuceneSearchContext)} creates a Weight, builds
 *       per-segment collectors, and populates the context with the Weight pointer
 *       for downstream use.</li>
 *   <li>JNI callback: Rust calls {@link #createCollector}, {@link #collectDocs},
 *       {@link #releaseCollector} to stream bitsets per partition range.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneEngineSearcher implements EngineSearcher<LuceneSearchContext> {

    private final String source;
    private final IndexSearcher indexSearcher;
    private final DirectoryReader directoryReader;

    /** Active Weight contexts keyed by opaque pointer. */
    private static final Map<Long, WeightContext> activeWeights = new ConcurrentHashMap<>();
    /** Active partition scorer contexts keyed by opaque pointer. */
    private static final Map<Long, PartitionScorerContext> activeScorers = new ConcurrentHashMap<>();
    private static final AtomicLong nextId = new AtomicLong(1);

    public LuceneEngineSearcher(String source, IndexSearcher indexSearcher, DirectoryReader directoryReader) {
        this.source = source;
        this.indexSearcher = indexSearcher;
        this.directoryReader = directoryReader;
    }

    @Override
    public String source() {
        return source;
    }

    /**
     * Execute: create a Weight from the query, register it, and store the
     * pointer on the context so the indexed query path can use it.
     */
    @Override
    public void search(LuceneSearchContext context) throws IOException {
        Query query = context.getQuery();
        if (query == null) {
            throw new IllegalStateException("No query set on LuceneSearchContext");
        }
        Query rewritten = indexSearcher.rewrite(query);
        Weight weight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafReaderContext> leaves = directoryReader.leaves();

        // TODO : need to redo this - this is specific to indexed table flow
        long ptr = nextId.getAndIncrement();
        activeWeights.put(ptr, new WeightContext(weight, leaves));
        context.setWeightPointer(ptr);
        context.setSegmentCount(leaves.size());
        context.setSegmentMaxDocs(leaves.stream().mapToInt(l -> l.reader().maxDoc()).toArray());
    }

    /** Create a partition scorer for a segment + doc range. Returns -1 if no matches. */
    public static long createCollector(long weightPtr, int segmentOrd, int minDoc, int maxDoc) {
        WeightContext ctx = activeWeights.get(weightPtr);
        if (ctx == null || segmentOrd < 0 || segmentOrd >= ctx.leaves.size()) {
            return -1;
        }
        try {
            Scorer scorer = ctx.weight.scorer(ctx.leaves.get(segmentOrd));
            if (scorer == null) return -1;
            long id = nextId.getAndIncrement();
            activeScorers.put(id, new PartitionScorerContext(scorer.iterator(), minDoc, maxDoc));
            return id;
        } catch (IOException e) {
            return -1;
        }
    }

    /** Collect matching doc IDs in [rowGroupMin, rowGroupMax) as a bitset (long[]). */
    public static long[] collectDocs(long scorerPtr, int rowGroupMin, int rowGroupMax) {
        PartitionScorerContext ctx = activeScorers.get(scorerPtr);
        if (ctx == null) return new long[0];

        int effectiveMin = Math.max(rowGroupMin, ctx.minDoc);
        int effectiveMax = Math.min(rowGroupMax, ctx.maxDoc);
        if (effectiveMin >= effectiveMax) return new long[0];

        BitSet bitset = new BitSet(effectiveMax - effectiveMin);
        try {
            DocIdSetIterator iter = ctx.iterator;
            int docId = ctx.currentDoc;
            if (docId == DocIdSetIterator.NO_MORE_DOCS || docId >= ctx.maxDoc) return new long[0];
            if (docId < effectiveMin) docId = iter.advance(effectiveMin);
            while (docId != DocIdSetIterator.NO_MORE_DOCS && docId < effectiveMax) {
                bitset.set(docId - effectiveMin);
                docId = iter.nextDoc();
            }
            ctx.currentDoc = docId;
        } catch (IOException e) {
            return new long[0];
        }
        return bitset.toLongArray();
    }

    /** Release a partition scorer. */
    public static void releaseCollector(long scorerPtr) {
        activeScorers.remove(scorerPtr);
    }

    /** Release a Weight context. */
    public static void releaseWeight(long weightPtr) {
        activeWeights.remove(weightPtr);
    }

    public static int getSegmentCount(long weightPtr) {
        WeightContext ctx = activeWeights.get(weightPtr);
        return ctx != null ? ctx.leaves.size() : -1;
    }

    public static int getSegmentMaxDoc(long weightPtr, int segmentOrd) {
        WeightContext ctx = activeWeights.get(weightPtr);
        if (ctx == null || segmentOrd < 0 || segmentOrd >= ctx.leaves.size()) return -1;
        return ctx.leaves.get(segmentOrd).reader().maxDoc();
    }

    public IndexSearcher getIndexSearcher() {
        return indexSearcher;
    }

    public DirectoryReader getDirectoryReader() {
        return directoryReader;
    }

    @Override
    public void close() {}

    static class WeightContext {
        final Weight weight;
        final List<LeafReaderContext> leaves;

        WeightContext(Weight weight, List<LeafReaderContext> leaves) {
            this.weight = weight;
            this.leaves = leaves;
        }
    }

    static class PartitionScorerContext {
        final DocIdSetIterator iterator;
        final int minDoc;
        final int maxDoc;
        int currentDoc = -1;

        PartitionScorerContext(DocIdSetIterator iterator, int minDoc, int maxDoc) {
            this.iterator = iterator;
            this.minDoc = minDoc;
            this.maxDoc = maxDoc;
        }
    }
}
