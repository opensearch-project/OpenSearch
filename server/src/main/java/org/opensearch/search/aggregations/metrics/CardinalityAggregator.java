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
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.BitMixer;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.core.common.unit.ByteSizeValue;
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
import java.util.function.LongConsumer;

import org.roaringbitmap.ArrayContainer;
import org.roaringbitmap.FastRankRoaringBitmap;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.PeekableIntRankIterator;
import org.roaringbitmap.RoaringArray;
import org.roaringbitmap.RoaringBitmap;

import static org.opensearch.search.SearchService.CARDINALITY_AGGREGATION_PRUNING_THRESHOLD;

/**
 * An aggregator that computes approximate counts of unique values.
 *
 * @opensearch.internal
 */
public class CardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private static final Logger logger = LogManager.getLogger(CardinalityAggregator.class);

    /**
     * Exception thrown when OrdinalsCollector exceeds memory threshold.
     * Uses singleton pattern and optimized constructor for performance in control flow scenarios.
     */
    static class MemoryLimitExceededException extends RuntimeException {
        static final MemoryLimitExceededException INSTANCE = new MemoryLimitExceededException();

        private MemoryLimitExceededException() {
            // Optimized constructor for control flow exceptions:
            // - null message/cause: no debugging info needed
            // - false enableSuppression: disable suppressed exception tracking
            // - false writableStackTrace: disable expensive stack trace generation (~100x faster)
            super(null, null, false, false);
        }
    }

    /**
     * Setting to enable/disable hybrid collector for cardinality aggregation.
     */
    public static final Setting<Boolean> CARDINALITY_AGGREGATION_HYBRID_COLLECTOR_ENABLED = Setting.boolSetting(
        "search.aggregations.cardinality.hybrid_collector.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for hybrid collector memory threshold. Supports both percentage (e.g., "1%")
     * and absolute values (e.g., "10mb", "1gb").
     * Note: This threshold is applied at the Lucene segment level since collectors are created per segment.
     */
    public static final Setting<ByteSizeValue> CARDINALITY_AGGREGATION_HYBRID_COLLECTOR_MEMORY_THRESHOLD = Setting.memorySizeSetting(
        "search.aggregations.cardinality.hybrid_collector.memory_threshold",
        "1%",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    final CardinalityAggregatorFactory.ExecutionMode executionMode;
    final int precision;
    final ValuesSource valuesSource;

    private final ValuesSourceConfig valuesSourceConfig;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    HyperLogLogPlusPlus counts;

    Collector collector;

    int emptyCollectorsUsed;
    int numericCollectorsUsed;
    int ordinalsCollectorsUsed;
    int hybridCollectorsUsed;
    int ordinalsCollectorsOverheadTooHigh;
    int stringHashingCollectorsUsed;
    int dynamicPrunedSegments;
    int roaringOrdinalsCollectorsUsed;
    int hybridCollectorsSwitchedToDirect;

    public CardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        CardinalityAggregatorFactory.ExecutionMode executionMode
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        this.precision = precision;
        this.counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        this.valuesSourceConfig = valuesSourceConfig;
        this.executionMode = executionMode;
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

        Collector collector;
        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals source) {
            final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
            if (ordinalValues.getValueCount() == 0) {
                emptyCollectorsUsed++;
                return new EmptyCollector();
            }
            collector = ordinalsCollector(source, ordinalValues, ctx);
        } else {
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

    private Collector ordinalsCollector(ValuesSource.Bytes.WithOrdinals source, SortedSetDocValues ordinalValues, LeafReaderContext ctx)
        throws IOException {
        if (executionMode == CardinalityAggregatorFactory.ExecutionMode.DIRECT) {
            stringHashingCollectorsUsed++;
            return new DirectCollector(counts, MurmurHash3Values.hash(source.bytesValues(ctx)));
        }
        if (executionMode == CardinalityAggregatorFactory.ExecutionMode.ORDINALS) {
            ordinalsCollectorsUsed++;
            return new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
        }
        final CardinalityAggregationContext cardinalityContext = context.cardinalityAggregationContext();
        if (cardinalityContext.isHybridCollectorEnabled()) {
            // BitArray per bucket while that is affordable, then roaring bitmaps that only pay for the ordinals they hold,
            // then plain hashing. Each stage hashes what it collected into counts before handing over.
            hybridCollectorsUsed++;
            final long memoryThreshold = cardinalityContext.getMemoryThreshold();
            return new HybridCollector(new OrdinalsCollector(counts, ordinalValues, context.bigArrays(), memoryThreshold, true), () -> {
                roaringOrdinalsCollectorsUsed++;
                return new RoaringOrdinalsCollector(counts, source.ordinalsValues(ctx), newBucketOrdinalBitmaps(), memoryThreshold);
            }, () -> {
                hybridCollectorsSwitchedToDirect++;
                return new DirectCollector(counts, MurmurHash3Values.hash(source.bytesValues(ctx)));
            });
        }
        final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(ordinalValues.getValueCount());
        final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
        if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
            ordinalsCollectorsUsed++;
            return new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
        }
        ordinalsCollectorsOverheadTooHigh++;
        stringHashingCollectorsUsed++;
        return new DirectCollector(counts, MurmurHash3Values.hash(source.bytesValues(ctx)));
    }

    BucketOrdinalBitmaps newBucketOrdinalBitmaps() {
        return new BucketOrdinalBitmaps(context.bigArrays(), this::addRequestCircuitBreakerBytes);
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
            Scorer scorer = weight.scorer(ctx);
            if (scorer == null) {
                return false;
            }
            pruningCollector.setScorer(scorer);
            DocIdSetIterator iterator = scorer.iterator();
            bulkCollect(ctx.reader().getLiveDocs(), iterator, pruningCollector);

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

    private void bulkCollect(Bits acceptDocs, DocIdSetIterator iterator, Collector pruningCollector) throws IOException {
        DocIdSetIterator competitiveIterator = pruningCollector.competitiveIterator();

        int doc = iterator.nextDoc();
        while (doc < DocIdSetIterator.NO_MORE_DOCS) {
            assert competitiveIterator.docID() <= doc; // invariant
            if (competitiveIterator.docID() < doc) {
                int competitiveNext = competitiveIterator.advance(doc);
                if (competitiveNext != doc) {
                    doc = iterator.advance(competitiveNext);
                    continue;
                }
            }
            if ((acceptDocs == null || acceptDocs.get(doc))) {
                pruningCollector.collect(doc);
            }
            doc = iterator.nextDoc();
        }
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
        add.accept("hybrid_collectors_used", hybridCollectorsUsed);
        add.accept("ordinals_collectors_overhead_too_high", ordinalsCollectorsOverheadTooHigh);
        add.accept("string_hashing_collectors_used", stringHashingCollectorsUsed);
        add.accept("dynamic_pruned_segments", dynamicPrunedSegments);
        add.accept("roaring_ordinals_collectors_used", roaringOrdinalsCollectorsUsed);
        add.accept("hybrid_collectors_switched_to_direct", hybridCollectorsSwitchedToDirect);
    }

    /**
     * Collector for the cardinality agg
     *
     * @opensearch.internal
     */
    abstract static class Collector extends LeafBucketCollector implements Releasable {

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

            this.queue = DisiPriorityQueue.ofMaxSize(postingMap.size());
            for (Scorer scorer : postingMap.values()) {
                queue.add(new DisiWrapper(scorer, false));
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
            if (queue.size() == 0) {
                return;
            }
            DisiWrapper top = queue.top();
            if (top == null) {
                return;
            }
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
        public int nextDoc() throws IOException {
            return advance(slowDocId + 1);
        }

        @Override
        public long cost() {
            return queue.top() == null ? 0 : queue.top().approximation.cost();
        }
    }

    /**
     * Empty Collector for the Cardinality agg
     *
     * @opensearch.internal
     */
    static class EmptyCollector extends Collector {

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
    static class DirectCollector extends Collector {

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
    static class OrdinalsCollector extends Collector {

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
        private long currentMemoryUsage = 0;

        private final long memoryThreshold;
        private final boolean memoryMonitoringEnabled;

        OrdinalsCollector(HyperLogLogPlusPlus counts, SortedSetDocValues values, BigArrays bigArrays) {
            this(counts, values, bigArrays, Long.MAX_VALUE, false);
        }

        OrdinalsCollector(
            HyperLogLogPlusPlus counts,
            SortedSetDocValues values,
            BigArrays bigArrays,
            long memoryThreshold,
            boolean memoryMonitoringEnabled
        ) {
            if (values.getValueCount() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            maxOrd = (int) values.getValueCount();
            this.bigArrays = bigArrays;
            this.counts = counts;
            this.values = values;
            this.memoryThreshold = memoryThreshold;
            this.memoryMonitoringEnabled = memoryMonitoringEnabled;
            visitedOrds = bigArrays.newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            collect(doc, getBitArray(bucketOrd));
        }

        @Override
        public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
            final BitArray bits = getBitArray(owningBucketOrd);
            stream.forEach((doc) -> collect(doc, bits));
        }

        @Override
        public void collectRange(int minDoc, int maxDoc) throws IOException {
            final BitArray bits = getBitArray(0);
            for (int doc = minDoc; doc < maxDoc; ++doc) {
                collect(doc, bits);
            }
        }

        private BitArray getBitArray(long bucket) {
            visitedOrds = bigArrays.grow(visitedOrds, bucket + 1);
            BitArray bits = visitedOrds.get(bucket);
            if (bits == null) {
                bits = new BitArray(maxOrd, bigArrays);
                visitedOrds.set(bucket, bits);

                // Check memory threshold only when monitoring is enabled (hybrid collector)
                if (memoryMonitoringEnabled) {
                    currentMemoryUsage += memoryOverhead(maxOrd);
                    if (currentMemoryUsage > memoryThreshold) {
                        // Throw singleton exception for efficient control flow to HybridCollector
                        throw MemoryLimitExceededException.INSTANCE;
                    }
                }
            }
            return bits;
        }

        private void collect(final int doc, final BitArray bits) throws IOException {
            if (values.advanceExact(doc)) {
                int count = values.docValueCount();
                long ord;
                while ((count-- > 0) && (ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
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

        public long getCurrentMemoryUsage() {
            return currentMemoryUsage;
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
     * Per-bucket {@link RoaringBitmap}s of ordinals with an estimate of their heap usage that is charged to the request
     * circuit breaker as the bitmaps grow.
     * <p>
     * {@link RoaringBitmap#getSizeInBytes()} leaves out object headers, which dominate small bitmaps, so a fixed overhead per
     * bitmap and per container is added on top. A bitmap is re-measured whenever its number of values doubles, and every
     * {@value #MAX_VALUES_BETWEEN_MEASUREMENTS} values once it is large, which keeps the estimate within a small factor of the
     * real size at an amortized constant cost per value.
     *
     * @opensearch.internal
     */
    static final class BucketOrdinalBitmaps implements Releasable {
        static final long BITMAP_OVERHEAD_BYTES = RamUsageEstimator.shallowSizeOfInstance(RoaringBitmap.class) + RamUsageEstimator
            .shallowSizeOfInstance(RoaringArray.class) + 2L * RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        static final long CONTAINER_OVERHEAD_BYTES = RamUsageEstimator.shallowSizeOfInstance(ArrayContainer.class)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        static final int MAX_VALUES_BETWEEN_MEASUREMENTS = 4096;
        private static final long BREAKER_FLUSH_BYTES = 64 * 1024;

        private final BigArrays bigArrays;
        private final LongConsumer breaker;
        private ObjectArray<RoaringBitmap> bitmaps;
        private LongArray accountedBytes;
        private IntArray valueCounts;
        private long maxBucket = -1;
        private long totalBytes;
        private long breakerBytes;

        BucketOrdinalBitmaps(BigArrays bigArrays, LongConsumer breaker) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            boolean success = false;
            try {
                bitmaps = bigArrays.newObjectArray(1);
                accountedBytes = bigArrays.newLongArray(1, true);
                valueCounts = bigArrays.newIntArray(1, true);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        RoaringBitmap get(long bucket) {
            return bucket < bitmaps.size() ? bitmaps.get(bucket) : null;
        }

        RoaringBitmap getOrCreate(long bucket) {
            RoaringBitmap bitmap = get(bucket);
            if (bitmap == null) {
                bitmaps = bigArrays.grow(bitmaps, bucket + 1);
                accountedBytes = bigArrays.grow(accountedBytes, bucket + 1);
                valueCounts = bigArrays.grow(valueCounts, bucket + 1);
                bitmap = new RoaringBitmap();
                bitmaps.set(bucket, bitmap);
                maxBucket = Math.max(maxBucket, bucket);
                account(bucket, bitmap);
            }
            return bitmap;
        }

        /**
         * Adds {@code ord} to {@code bitmap}, which must be the bitmap of {@code bucket}.
         */
        void add(long bucket, RoaringBitmap bitmap, int ord) {
            if (bitmap.checkedAdd(ord) && shouldMeasure(valueCounts.increment(bucket, 1))) {
                account(bucket, bitmap);
            }
        }

        private static boolean shouldMeasure(int values) {
            if (values <= MAX_VALUES_BETWEEN_MEASUREMENTS) {
                return (values & (values - 1)) == 0; // power of two
            }
            return values % MAX_VALUES_BETWEEN_MEASUREMENTS == 0;
        }

        /**
         * Estimated heap used by {@code bitmap}.
         */
        static long ramBytesUsed(RoaringBitmap bitmap) {
            return BITMAP_OVERHEAD_BYTES + bitmap.getSizeInBytes() + (long) bitmap.getContainerCount() * CONTAINER_OVERHEAD_BYTES;
        }

        private void account(long bucket, RoaringBitmap bitmap) {
            final long bytes = ramBytesUsed(bitmap);
            totalBytes += bytes - accountedBytes.get(bucket);
            accountedBytes.set(bucket, bytes);
            final long unflushed = totalBytes - breakerBytes;
            if (unflushed >= BREAKER_FLUSH_BYTES || unflushed <= -BREAKER_FLUSH_BYTES) {
                flushBreaker();
            }
        }

        /**
         * Brings the circuit breaker up to date with {@link #bytesUsed()}.
         */
        void flushBreaker() {
            final long delta = totalBytes - breakerBytes;
            if (delta != 0) {
                breaker.accept(delta);
                breakerBytes += delta;
            }
        }

        long bytesUsed() {
            return totalBytes;
        }

        /**
         * Hashes every distinct ordinal once, then adds the hashes of each bucket's ordinals to that bucket of {@code counts}.
         *
         * @param values resolves the ordinals held by the bitmaps to their values
         */
        void collectInto(HyperLogLogPlusPlus counts, SortedSetDocValues values) throws IOException {
            final FastRankRoaringBitmap visited = new FastRankRoaringBitmap();
            for (long bucket = 0; bucket <= maxBucket; bucket++) {
                final RoaringBitmap bitmap = bitmaps.get(bucket);
                if (bitmap != null) {
                    visited.or(bitmap);
                }
            }
            final int visitedCount = visited.getCardinality();
            if (visitedCount == 0) {
                return;
            }
            final long visitedBytes = ramBytesUsed(visited);
            breaker.accept(visitedBytes);
            try (LongArray hashes = bigArrays.newLongArray(visitedCount, false)) {
                final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                final PeekableIntIterator ords = visited.getIntIterator();
                for (long i = 0; ords.hasNext(); i++) {
                    final BytesRef value = values.lookupOrd(ords.next());
                    MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(i, hash.h1);
                }
                // A bucket's ordinals are a sorted subset of the visited ones, so a rank iterator over the visited ordinals
                // only ever moves forward and finds each ordinal's index without rescanning from the start.
                for (long bucket = 0; bucket <= maxBucket; bucket++) {
                    final RoaringBitmap bitmap = bitmaps.get(bucket);
                    if (bitmap == null) {
                        continue;
                    }
                    final PeekableIntRankIterator ranks = visited.getIntRankIterator();
                    final PeekableIntIterator bucketOrds = bitmap.getIntIterator();
                    while (bucketOrds.hasNext()) {
                        ranks.advanceIfNeeded(bucketOrds.next());
                        counts.collect(bucket, hashes.get(ranks.peekNextRank() - 1));
                    }
                }
            } finally {
                breaker.accept(-visitedBytes);
            }
        }

        @Override
        public void close() {
            try {
                if (breakerBytes != 0) {
                    breaker.accept(-breakerBytes);
                }
            } finally {
                breakerBytes = 0;
                totalBytes = 0;
                Releasables.close(bitmaps, accountedBytes, valueCounts);
            }
        }
    }

    /**
     * Collects segment ordinals into a {@link RoaringBitmap} per bucket and hashes them into {@code counts} in
     * {@link #postCollect()}. Unlike {@link OrdinalsCollector} a bucket only pays for the ordinals it holds rather than
     * {@code maxOrd} bits, which keeps many sparse buckets affordable.
     * <p>
     * Throws {@link MemoryLimitExceededException} once the bitmaps exceed the memory threshold, so that
     * {@link HybridCollector} can fall back to {@link DirectCollector}.
     *
     * @opensearch.internal
     */
    static class RoaringOrdinalsCollector extends Collector {

        private final HyperLogLogPlusPlus counts;
        private final SortedSetDocValues values;
        private final BucketOrdinalBitmaps bitmaps;
        private final long memoryThreshold;
        private long cachedBucket = -1;
        private RoaringBitmap cachedBitmap;

        RoaringOrdinalsCollector(
            HyperLogLogPlusPlus counts,
            SortedSetDocValues values,
            BucketOrdinalBitmaps bitmaps,
            long memoryThreshold
        ) {
            if (values.getValueCount() > Integer.MAX_VALUE) {
                Releasables.close(bitmaps);
                throw new IllegalArgumentException();
            }
            this.counts = counts;
            this.values = values;
            this.bitmaps = bitmaps;
            this.memoryThreshold = memoryThreshold;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (values.advanceExact(doc) == false) {
                return;
            }
            if (bucketOrd != cachedBucket) {
                cachedBitmap = bitmaps.getOrCreate(bucketOrd);
                cachedBucket = bucketOrd;
            }
            int count = values.docValueCount();
            long ord;
            while ((count-- > 0) && (ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                bitmaps.add(bucketOrd, cachedBitmap, (int) ord);
            }
            if (bitmaps.bytesUsed() > memoryThreshold) {
                throw MemoryLimitExceededException.INSTANCE;
            }
        }

        @Override
        public void postCollect() throws IOException {
            bitmaps.collectInto(counts, values);
        }

        @Override
        public void close() {
            bitmaps.close();
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

    /**
     * Creates the collector that a {@link HybridCollector} switches to.
     */
    @FunctionalInterface
    interface FallbackCollectorSupplier {
        Collector get() throws IOException;
    }

    /**
     * Collector that starts with a memory-hungry but fast collector and moves to the next fallback each time the active one
     * throws {@link MemoryLimitExceededException}. The active collector is post-collected before switching, so everything it
     * gathered is preserved, and the document being collected is collected again by the fallback. Exception-based control
     * flow keeps the common, under-threshold path free of overhead. The last fallback must never exceed its memory budget.
     */
    static class HybridCollector extends Collector {
        private final FallbackCollectorSupplier[] fallbacks;
        private int nextFallback;
        private Collector activeCollector;

        HybridCollector(Collector initialCollector, FallbackCollectorSupplier... fallbacks) {
            this.activeCollector = initialCollector;
            this.fallbacks = fallbacks;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            while (true) {
                try {
                    activeCollector.collect(doc, bucketOrd);
                    return;
                } catch (MemoryLimitExceededException e) {
                    switchToFallback();
                }
            }
        }

        @Override
        public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
            while (true) {
                try {
                    activeCollector.collect(stream, owningBucketOrd);
                    return;
                } catch (MemoryLimitExceededException e) {
                    switchToFallback();
                }
            }
        }

        @Override
        public void collectRange(int minDoc, int maxDoc) throws IOException {
            while (true) {
                try {
                    activeCollector.collectRange(minDoc, maxDoc);
                    return;
                } catch (MemoryLimitExceededException e) {
                    switchToFallback();
                }
            }
        }

        private void switchToFallback() throws IOException {
            if (nextFallback == fallbacks.length) {
                throw new IllegalStateException("the last fallback collector exceeded its memory budget");
            }
            final Collector previous = activeCollector;
            activeCollector = null;
            try {
                previous.postCollect();
            } finally {
                previous.close();
            }
            activeCollector = fallbacks[nextFallback++].get();
            logger.debug(
                "Hybrid collector switched from {} to {}",
                previous.getClass().getSimpleName(),
                activeCollector.getClass().getSimpleName()
            );
        }

        @Override
        public void postCollect() throws IOException {
            activeCollector.postCollect();
        }

        @Override
        public void close() {
            Releasables.close(activeCollector);
        }

        // Visible for testing
        Collector getActiveCollector() {
            return activeCollector;
        }
    }
}
