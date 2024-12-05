/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.Nullable;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.BitMixer;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.opensearch.search.SearchService.CARDINALITY_AGGREGATION_PRUNING_THRESHOLD;

/**
 * An aggregator that computes approximate counts of unique values.
 *
 * @opensearch.internal
 */
public class CardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private static final Logger logger = LogManager.getLogger(CardinalityAggregator.class);

    private final int precision;
    private final ValuesSource valuesSource;

    private final ValuesSourceConfig valuesSourceConfig;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    private HyperLogLogPlusPlus counts;

    private Collector collector;

    private int emptyCollectorsUsed;
    private int numericCollectorsUsed;
    private int ordinalsCollectorsUsed;
    private int ordinalsCollectorsOverheadTooHigh;
    private int stringHashingCollectorsUsed;
    private int dynamicPrunedSegments;

    public CardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        this.precision = precision;
        this.counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        this.valuesSourceConfig = valuesSourceConfig;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            emptyCollectorsUsed++;
            return new EmptyCollector();
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) valuesSource;
            MurmurHash3Values hashValues = (source.isFloatingPoint() || source.isBigInteger())
                ? MurmurHash3Values.hash(source.doubleValues(ctx))
                : MurmurHash3Values.hash(source.longValues(ctx));
            numericCollectorsUsed++;
            return new DirectCollector(counts, hashValues);
        }

        Collector collector = null;
        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
            final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
            final long maxOrd = ordinalValues.getValueCount();
            if (maxOrd == 0) {
                emptyCollectorsUsed++;
                return new EmptyCollector();
            } else {
                final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
                final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
                // only use ordinals if they don't increase memory usage by more than 25%
                if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
                    ordinalsCollectorsUsed++;
                    collector = new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
                } else {
                    ordinalsCollectorsOverheadTooHigh++;
                }
            }
        }

        if (collector == null) { // not able to build an OrdinalsCollector
            stringHashingCollectorsUsed++;
            collector = new DirectCollector(counts, MurmurHash3Values.hash(valuesSource.bytesValues(ctx)));
        }

        if (canPrune(parent, subAggregators, valuesSourceConfig)) {
            Terms terms = ctx.reader().terms(valuesSourceConfig.fieldContext().field());
            if (terms == null) return collector;
            if (exceedMaxThreshold(terms)) {
                return collector;
            }

            Collector pruningCollector = tryWrapWithPruningCollector(collector, terms, ctx);
            if (pruningCollector == null) {
                return collector;
            }

            if (!tryScoreWithPruningCollector(ctx, pruningCollector)) {
                return collector;
            }
            logger.debug("Dynamic pruned segment {} of shard {}", ctx.ord, context.indexShard().shardId());
            dynamicPrunedSegments++;

            return getNoOpCollector();
        }

        return collector;
    }

    private boolean canPrune(Aggregator parent, Aggregator[] subAggregators, ValuesSourceConfig valuesSourceConfig) {
        return parent == null && subAggregators.length == 0 && valuesSourceConfig.missing() == null && valuesSourceConfig.script() == null;
    }

    private boolean exceedMaxThreshold(Terms terms) throws IOException {
        if (terms.size() > context.cardinalityAggregationPruningThreshold()) {
            logger.debug(
                "Cannot prune because terms size {} is greater than the threshold {}",
                terms.size(),
                context.cardinalityAggregationPruningThreshold()
            );
            return true;
        }
        return false;
    }

    private Collector tryWrapWithPruningCollector(Collector collector, Terms terms, LeafReaderContext ctx) {
        try {
            return new PruningCollector(collector, terms.iterator(), ctx, context, valuesSourceConfig.fieldContext().field());
        } catch (Exception e) {
            logger.warn("Failed to build collector for dynamic pruning.", e);
            return null;
        }
    }

    private boolean tryScoreWithPruningCollector(LeafReaderContext ctx, Collector pruningCollector) throws IOException {
        try {
            Weight weight = context.query().rewrite(context.searcher()).createWeight(context.searcher(), ScoreMode.TOP_DOCS, 1f);
            BulkScorer scorer = weight.bulkScorer(ctx);
            if (scorer == null) {
                return false;
            }
            Bits liveDocs = ctx.reader().getLiveDocs();
            scorer.score(pruningCollector, liveDocs);
            pruningCollector.postCollect();
            Releasables.close(pruningCollector);
        } catch (Exception e) {
            throw new OpenSearchStatusException(
                "Failed when performing dynamic pruning in cardinality aggregation. You can set cluster setting ["
                    + CARDINALITY_AGGREGATION_PRUNING_THRESHOLD.getKey()
                    + "] to 0 to disable.",
                RestStatus.INTERNAL_SERVER_ERROR,
                e
            );
        }
        return true;
    }

    private Collector getNoOpCollector() {
        return new Collector() {
            @Override
            public void close() {}

            @Override
            public void postCollect() throws IOException {}

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                throw new CollectionTerminatedException();
            }
        };
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();

        collector = pickCollector(ctx);
        return collector;
    }

    private void postCollectLastCollector() throws IOException {
        if (collector != null) {
            try {
                collector.postCollect();
            } finally {
                collector.close();
                collector = null;
            }
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        postCollectLastCollector();
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxOrd() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs to remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        AbstractHyperLogLogPlusPlus copy = counts.clone(owningBucketOrdinal, BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(name, copy, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("empty_collectors_used", emptyCollectorsUsed);
        add.accept("numeric_collectors_used", numericCollectorsUsed);
        add.accept("ordinals_collectors_used", ordinalsCollectorsUsed);
        add.accept("ordinals_collectors_overhead_too_high", ordinalsCollectorsOverheadTooHigh);
        add.accept("string_hashing_collectors_used", stringHashingCollectorsUsed);
        add.accept("dynamic_pruned_segments", dynamicPrunedSegments);
    }

    /**
     * Collector for the cardinality agg
     *
     * @opensearch.internal
     */
    private abstract static class Collector extends LeafBucketCollector implements Releasable {

        public abstract void postCollect() throws IOException;

    }

    /**
     * This collector enhance the delegate collector with pruning ability on term field
     * The iterators of term field values are wrapped into a priority queue, and able to
     * pop/prune the values after being collected
     */
    private static class PruningCollector extends Collector {

        private final Collector delegate;
        private final DisiPriorityQueue queue;
        private final DocIdSetIterator competitiveIterator;

        PruningCollector(Collector delegate, TermsEnum terms, LeafReaderContext ctx, SearchContext context, String field)
            throws IOException {
            this.delegate = delegate;

            Map<BytesRef, Scorer> postingMap = new HashMap<>();
            while (terms.next() != null) {
                BytesRef term = terms.term();
                TermQuery termQuery = new TermQuery(new Term(field, term));
                Weight subWeight = termQuery.createWeight(context.searcher(), ScoreMode.COMPLETE_NO_SCORES, 1f);
                Scorer scorer = subWeight.scorer(ctx);
                postingMap.put(term, scorer);
            }

            this.queue = new DisiPriorityQueue(postingMap.size());
            for (Scorer scorer : postingMap.values()) {
                queue.add(new DisiWrapper(scorer));
            }

            competitiveIterator = new DisjunctionDISI(queue);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            delegate.collect(doc, owningBucketOrd);
            prune(doc);
        }

        /**
         * Note: the queue may be empty or the queue top may be null after pruning
         */
        private void prune(int doc) {
            DisiWrapper top = queue.top();
            int curTopDoc = top.doc;
            if (curTopDoc == doc) {
                do {
                    queue.pop();
                    top = queue.updateTop();
                } while (queue.size() > 1 && top.doc == curTopDoc);
            }
        }

        @Override
        public DocIdSetIterator competitiveIterator() {
            return competitiveIterator;
        }

        @Override
        public void postCollect() throws IOException {
            delegate.postCollect();
        }
    }

    /**
     * This DISI is a disjunction of all terms in a segment
     * And it will be the competitive iterator of the leaf pruning collector
     * After pruning done after collect, queue top doc may exceed the next doc of (lead) iterator
     * To still providing a docID slower than the lead iterator for the next iteration
     * We keep track of a slowDocId that will be updated later during advance
     */
    private static class DisjunctionDISI extends DocIdSetIterator {
        private final DisiPriorityQueue queue;
        private int slowDocId = -1;

        public DisjunctionDISI(DisiPriorityQueue queue) {
            this.queue = queue;
        }

        @Override
        public int docID() {
            return slowDocId;
        }

        @Override
        public int advance(int target) throws IOException {
            DisiWrapper top = queue.top();
            if (top == null) {
                return slowDocId = NO_MORE_DOCS;
            }

            // This would be the outcome of last pruning
            // this DISI's docID is already making to the target
            if (top.doc >= target) {
                slowDocId = top.doc;
                return top.doc;
            }

            do {
                top.doc = top.approximation.advance(target);
                top = queue.updateTop();
            } while (top.doc < target);
            slowDocId = queue.size() == 0 ? NO_MORE_DOCS : queue.top().doc;

            return slowDocId;
        }

        @Override
        public int nextDoc() {
            // don't expect this to be called based on its usage in DefaultBulkScorer
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            // don't expect this to be called based on its usage in DefaultBulkScorer
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Empty Collector for the Cardinality agg
     *
     * @opensearch.internal
     */
    private static class EmptyCollector extends Collector {

        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * Direct Collector for the cardinality agg
     *
     * @opensearch.internal
     */
    private static class DirectCollector extends Collector {

        private final MurmurHash3Values hashes;
        private final HyperLogLogPlusPlus counts;

        DirectCollector(HyperLogLogPlusPlus counts, MurmurHash3Values values) {
            this.counts = counts;
            this.hashes = values;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (hashes.advanceExact(doc)) {
                final int valueCount = hashes.count();
                for (int i = 0; i < valueCount; ++i) {
                    counts.collect(bucketOrd, hashes.nextValue());
                }
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }

    }

    /**
     * Ordinals Collector for the cardinality agg
     *
     * @opensearch.internal
     */
    private static class OrdinalsCollector extends Collector {

        private static final long SHALLOW_FIXEDBITSET_SIZE = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

        /**
         * Return an approximate memory overhead per bucket for this collector.
         */
        public static long memoryOverhead(long maxOrd) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + SHALLOW_FIXEDBITSET_SIZE + (maxOrd + 7) / 8; // 1 bit per ord
        }

        private final BigArrays bigArrays;
        private final SortedSetDocValues values;
        private final int maxOrd;
        private final HyperLogLogPlusPlus counts;
        private ObjectArray<BitArray> visitedOrds;

        OrdinalsCollector(HyperLogLogPlusPlus counts, SortedSetDocValues values, BigArrays bigArrays) {
            if (values.getValueCount() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            maxOrd = (int) values.getValueCount();
            this.bigArrays = bigArrays;
            this.counts = counts;
            this.values = values;
            visitedOrds = bigArrays.newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            visitedOrds = bigArrays.grow(visitedOrds, bucketOrd + 1);
            BitArray bits = visitedOrds.get(bucketOrd);
            if (bits == null) {
                bits = new BitArray(maxOrd, bigArrays);
                visitedOrds.set(bucketOrd, bits);
            }
            if (values.advanceExact(doc)) {
                for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                    bits.set((int) ord);
                }
            }
        }

        @Override
        public void postCollect() throws IOException {
            try (BitArray allVisitedOrds = new BitArray(maxOrd, bigArrays)) {
                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    final BitArray bits = visitedOrds.get(bucket);
                    if (bits != null) {
                        allVisitedOrds.or(bits);
                    }
                }

                try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                    final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                    for (long ord = allVisitedOrds.nextSetBit(0); ord < Long.MAX_VALUE; ord = ord + 1 < maxOrd
                        ? allVisitedOrds.nextSetBit(ord + 1)
                        : Long.MAX_VALUE) {
                        final BytesRef value = values.lookupOrd(ord);
                        MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                        hashes.set(ord, hash.h1);
                    }

                    for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                        final BitArray bits = visitedOrds.get(bucket);
                        if (bits != null) {
                            for (long ord = bits.nextSetBit(0); ord < Long.MAX_VALUE; ord = ord + 1 < maxOrd
                                ? bits.nextSetBit(ord + 1)
                                : Long.MAX_VALUE) {
                                counts.collect(bucket, hashes.get(ord));
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            for (int i = 0; i < visitedOrds.size(); i++) {
                Releasables.close(visitedOrds.get(i));
            }
            Releasables.close(visitedOrds);
        }
    }

    /**
     * Representation of a list of hash values. There might be dups and there is no guarantee on the order.
     *
     * @opensearch.internal
     */
    abstract static class MurmurHash3Values {

        public abstract boolean advanceExact(int docId) throws IOException;

        public abstract int count();

        public abstract long nextValue() throws IOException;

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3Values hash(SortedNumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3Values hash(SortedNumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3Values hash(SortedBinaryDocValues values) {
            return new Bytes(values);
        }

        /**
         * Long hash value
         *
         * @opensearch.internal
         */
        private static class Long extends MurmurHash3Values {

            private final SortedNumericDocValues values;

            Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return BitMixer.mix64(values.nextValue());
            }
        }

        /**
         * Double hash value
         *
         * @opensearch.internal
         */
        private static class Double extends MurmurHash3Values {

            private final SortedNumericDoubleValues values;

            Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return BitMixer.mix64(java.lang.Double.doubleToLongBits(values.nextValue()));
            }
        }

        /**
         * Byte hash value
         *
         * @opensearch.internal
         */
        private static class Bytes extends MurmurHash3Values {

            private final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

            private final SortedBinaryDocValues values;

            Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                final BytesRef bytes = values.nextValue();
                MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
