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
import org.apache.lucene.index.DirectoryReader;
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
import org.apache.lucene.search.IndexSearcher;
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
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.roaringbitmap.PeekableIntIterator;
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
    private final CardinalityUpperBound bucketCardinality;
    final int precision;
    final ValuesSource valuesSource;

    private final ValuesSourceConfig valuesSourceConfig;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    HyperLogLogPlusPlus counts;

    Collector collector;
    DeferredOrdinalsCollector deferredCollector; // lives across all segments when using global ordinals

    int emptyCollectorsUsed;
    int numericCollectorsUsed;
    int ordinalsCollectorsUsed;
    int hybridCollectorsUsed;
    int ordinalsCollectorsOverheadTooHigh;
    int stringHashingCollectorsUsed;
    int dynamicPrunedSegments;
    int deferredOrdinalsCollectorsUsed;
    int roaringOrdinalsCollectorsUsed;
    long peakBreakerDuringCollect;
    long peakBreakerDuringPostCollect;
    long baselineBreakerAtStart = -1;
    String firstCollectorSelectionReason;
    boolean globalOrdinalsAvailable;
    boolean hasParentMultiBucket;

    public CardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        CardinalityAggregatorFactory.ExecutionMode executionMode,
        CardinalityUpperBound bucketCardinality
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        this.precision = precision;
        this.counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        this.valuesSourceConfig = valuesSourceConfig;
        this.executionMode = executionMode;
        this.bucketCardinality = bucketCardinality;
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
        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals source) {
            final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
            final long maxOrd = ordinalValues.getValueCount();
            if (maxOrd == 0) {
                emptyCollectorsUsed++;
                return new EmptyCollector();
            } else if (executionMode == CardinalityAggregatorFactory.ExecutionMode.DIRECT) {
                // Force DirectCollector via execution_hint
                stringHashingCollectorsUsed++;
                collector = new DirectCollector(counts, MurmurHash3Values.hash(source.bytesValues(ctx)));
            } else if (executionMode == CardinalityAggregatorFactory.ExecutionMode.ORDINALS) {
                // Force OrdinalsCollector via execution_hint
                ordinalsCollectorsUsed++;
                collector = new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
            } else if (executionMode == CardinalityAggregatorFactory.ExecutionMode.DEFERRED_ORDINALS) {
                // Force DeferredOrdinalsCollector via execution_hint
                deferredOrdinalsCollectorsUsed++;
                if (deferredCollector == null) {
                    deferredCollector = new DeferredOrdinalsCollector(
                        counts,
                        (ValuesSource.Bytes.WithOrdinals) valuesSource,
                        context.bigArrays(),
                        context.searcher()
                    );
                }
                return deferredCollector.leafCollector(ctx);
            } else {
                // No hint provided — use automatic decision tree
                collector = pickCollectorAutomatic(source, ordinalValues, maxOrd, ctx);
            }
        }

        if (collector == null) {
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

    /**
     * Automatic collector selection when no execution_hint is provided.
     * Picks based on parent bucket cardinality and global ordinals availability.
     */
    private Collector pickCollectorAutomatic(
        ValuesSource.Bytes.WithOrdinals source,
        SortedSetDocValues ordinalValues,
        long maxOrd,
        LeafReaderContext ctx
    ) throws IOException {
        CardinalityAggregationContext cardinalityContext = context.cardinalityAggregationContext();
        hasParentMultiBucket = bucketCardinality != CardinalityUpperBound.ONE && bucketCardinality != CardinalityUpperBound.NONE;
        long memoryBudget = cardinalityContext.getMemoryThreshold();

        // Legacy path: hybrid collector disabled
        if (!cardinalityContext.isHybridCollectorEnabled()) {
            final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
            final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
            if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
                ordinalsCollectorsUsed++;
                if (firstCollectorSelectionReason == null) firstCollectorSelectionReason = "legacy_ordinals_affordable";
                return new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
            }
            ordinalsCollectorsOverheadTooHigh++;
            if (firstCollectorSelectionReason == null) firstCollectorSelectionReason = "legacy_ordinals_overhead_too_high";
            return null; // caller will create DirectCollector
        }

        // No parent multi-bucket aggregator: single bucket, use hybrid for safety
        if (!hasParentMultiBucket) {
            hybridCollectorsUsed++;
            if (firstCollectorSelectionReason == null) firstCollectorSelectionReason = "no_parent_bucket_agg_hybrid";
            return new HybridCollector(
                counts,
                ordinalValues,
                MurmurHash3Values.hash(source.bytesValues(ctx)),
                context.bigArrays(),
                cardinalityContext,
                HybridStartingStrategy.BITARRAY
            );
        }

        // Multi-bucket: use DeferredOrdinalsCollector if global ordinals available, else Roaring
        globalOrdinalsAvailable = areGlobalOrdinalsAvailable(source);
        if (globalOrdinalsAvailable) {
            deferredOrdinalsCollectorsUsed++;
            if (deferredCollector == null) {
                long globalMaxOrd = source.globalOrdinalsValues(ctx).getValueCount();
                if (globalMaxOrd > Integer.MAX_VALUE) {
                    logger.debug("Global ordinals exceed Integer.MAX_VALUE, falling back to non-deferred");
                    if (firstCollectorSelectionReason == null) firstCollectorSelectionReason = "global_ords_exceed_int_max";
                } else {
                    deferredCollector = new DeferredOrdinalsCollector(counts, source, context.bigArrays(), context.searcher());
                    if (firstCollectorSelectionReason == null) firstCollectorSelectionReason = "deferred_global_ordinals";
                }
            }
            if (deferredCollector != null) {
                return deferredCollector.leafCollector(ctx);
            }
        }

        // Fallback: Roaring via HybridCollector (global ordinals not available)
        MurmurHash3Values hashValues = MurmurHash3Values.hash(source.bytesValues(ctx));
        hybridCollectorsUsed++;
        roaringOrdinalsCollectorsUsed++;
        if (firstCollectorSelectionReason == null) firstCollectorSelectionReason = "multi_bucket_roaring";
        return new HybridCollector(
            counts,
            ordinalValues,
            hashValues,
            context.bigArrays(),
            cardinalityContext,
            HybridStartingStrategy.ROARING
        );
    }

    /**
    /**
     * Checks if global ordinals are available for the cardinality field without
     * triggering an expensive build.
     *
     * Global ordinals are considered available when:
     * 1. Single segment reader — ordinals are already global (loadGlobal is a no-op).
     * Checks if global ordinals are available without triggering an expensive build.
     * Uses the field data cache to peek whether they've already been built.
     */
    private boolean areGlobalOrdinalsAvailable(ValuesSource.Bytes.WithOrdinals source) {
        if (valuesSourceConfig.fieldContext() == null) return false;
        IndexOrdinalsFieldData fieldData = (IndexOrdinalsFieldData) context.getQueryShardContext()
            .getForField(valuesSourceConfig.fieldContext().fieldType());
        return fieldData.isGlobalOrdinalsCached((DirectoryReader) context.searcher().getIndexReader());
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

    private long getBreakerUsed() {
        try {
            return context.bigArrays().breakerService().getBreaker(CircuitBreaker.REQUEST).getUsed();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (baselineBreakerAtStart < 0) baselineBreakerAtStart = getBreakerUsed();

        // Record peak before postCollect (this is the collect-phase peak)
        long used = getBreakerUsed();
        if (used > peakBreakerDuringCollect) peakBreakerDuringCollect = used;

        postCollectLastCollector();

        // Record peak after postCollect (this is the postCollect-phase peak)
        used = getBreakerUsed();
        if (used > peakBreakerDuringPostCollect) peakBreakerDuringPostCollect = used;

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
        long used = getBreakerUsed();
        if (used > peakBreakerDuringCollect) peakBreakerDuringCollect = used;

        postCollectLastCollector();

        used = getBreakerUsed();
        if (used > peakBreakerDuringPostCollect) peakBreakerDuringPostCollect = used;
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (deferredCollector != null) {
            return deferredCollector.ordinalCardinality(owningBucketOrd);
        }
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        if (deferredCollector != null) {
            if (deferredCollector.ordinalCardinality(owningBucketOrdinal) == 0) {
                return buildEmptyAggregation();
            }
            try (HyperLogLogPlusPlus singleHLL = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1)) {
                deferredCollector.materializeHLL(singleHLL, 0, owningBucketOrdinal);
                long used = getBreakerUsed();
                if (used > peakBreakerDuringPostCollect) peakBreakerDuringPostCollect = used;
                AbstractHyperLogLogPlusPlus copy = singleHLL.clone(0, BigArrays.NON_RECYCLING_INSTANCE);
                return new InternalCardinality(name, copy, metadata());
            }
        }
        if (counts == null || owningBucketOrdinal >= counts.maxOrd() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        AbstractHyperLogLogPlusPlus copy = counts.clone(owningBucketOrdinal, BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(name, copy, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, collector, deferredCollector);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("empty_collectors_used", emptyCollectorsUsed);
        add.accept("numeric_collectors_used", numericCollectorsUsed);
        add.accept("ordinals_collectors_used", ordinalsCollectorsUsed);
        add.accept("hybrid_collectors_used", hybridCollectorsUsed);
        add.accept("roaring_ordinals_collectors_used", roaringOrdinalsCollectorsUsed);
        add.accept("ordinals_collectors_overhead_too_high", ordinalsCollectorsOverheadTooHigh);
        add.accept("string_hashing_collectors_used", stringHashingCollectorsUsed);
        add.accept("dynamic_pruned_segments", dynamicPrunedSegments);
        add.accept("deferred_ordinals_collectors_used", deferredOrdinalsCollectorsUsed);
        if (firstCollectorSelectionReason != null) {
            add.accept("collector_selection_reason", firstCollectorSelectionReason);
        }
        add.accept("has_parent_multi_bucket_agg", hasParentMultiBucket);
        add.accept("global_ordinals_available", globalOrdinalsAvailable);
        // Breaker tracking
        long baseline = Math.max(baselineBreakerAtStart, 0);
        add.accept("peak_breaker_during_collect_bytes", peakBreakerDuringCollect);
        add.accept("peak_breaker_during_postcollect_bytes", peakBreakerDuringPostCollect);
        add.accept("breaker_baseline_bytes", baseline);
        add.accept("peak_breaker_delta_collect_bytes", peakBreakerDuringCollect - baseline);
        add.accept("peak_breaker_delta_postcollect_bytes", peakBreakerDuringPostCollect - baseline);
        if (deferredCollector != null) {
            add.accept("deferred_buckets_collected", deferredCollector.bucketsCollected);
            add.accept("deferred_buckets_materialized", deferredCollector.bucketsMaterialized);
            add.accept("deferred_roaring_bytes_tracked", deferredCollector.roaringBytesTracked);
        }
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
     * A collector that records ordinals per bucket using {@link RoaringBitmap} and defers
     * the expensive ordinal-to-value hashing until after top-N bucket selection.
     *
     * <p>Roaring bitmaps adapt their internal representation to data density: sparse buckets
     * use sorted arrays (~2 bytes per ordinal), dense buckets use bitmaps. This is dramatically
     * more memory-efficient than a flat {@link BitArray} when most buckets are sparse.
     *
     * <p>Cardinality per bucket is exact ({@code bitmap.getCardinality()}) — no HLL needed
     * for ranking. After top-N selection, call {@link #materializeHLL(long[])} to build
     * proper value-based HLL only for selected buckets.
     *
     * @opensearch.internal
     */
    /**
     * Collects <b>global</b> ordinals into per-bucket {@link RoaringBitmap}s and defers
     * HLL materialization until {@link #materializeHLL} is called — typically after the
     * parent terms aggregator has selected top-N buckets.
     *
     * <p>Because global ordinals share a single ordinal space across all segments, one
     * instance lives for the entire shard (not per-segment). {@link #leafCollector}
     * returns a lightweight per-segment collector that delegates into the shared bitmaps.
     *
     * <p>No {@code LongArray(maxOrd)} is ever allocated — hashing is done on-the-fly
     * per bucket during materialization.
     *
     * @opensearch.internal
     */
    static class DeferredOrdinalsCollector implements Releasable {

        private final BigArrays bigArrays;
        private final ValuesSource.Bytes.WithOrdinals valuesSource;
        private final HyperLogLogPlusPlus counts;
        private final IndexSearcher searcher;
        private ObjectArray<RoaringBitmap> visitedOrds;
        private long roaringBytesTracked; // bytes reported to circuit breaker
        long bucketsCollected; // total buckets with at least one ordinal
        long bucketsMaterialized; // buckets for which HLL was materialized (post top-N)

        DeferredOrdinalsCollector(
            HyperLogLogPlusPlus counts,
            ValuesSource.Bytes.WithOrdinals valuesSource,
            BigArrays bigArrays,
            IndexSearcher searcher
        ) {
            this.bigArrays = bigArrays;
            this.valuesSource = valuesSource;
            this.counts = counts;
            this.searcher = searcher;
            this.visitedOrds = bigArrays.newObjectArray(1);
        }

        /**
         * Returns a per-segment leaf collector that records global ordinals into the
         * shared bitmaps.
         */
        Collector leafCollector(LeafReaderContext ctx) throws IOException {
            final SortedSetDocValues globalOrds = valuesSource.globalOrdinalsValues(ctx);
            if (globalOrds.getValueCount() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                    "DeferredOrdinalsCollector does not support more than Integer.MAX_VALUE global ordinals, got: "
                        + globalOrds.getValueCount()
                );
            }
            return new Collector() {
                private long cachedBucket = -1;
                private RoaringBitmap cachedBitmap;
                private long collectsSinceLastCheck;
                private long lastKnownBytes;
                private static final long CHECK_INTERVAL = 1024;

                @Override
                public void collect(int doc, long bucketOrd) throws IOException {
                    if (globalOrds.advanceExact(doc) == false) return;
                    if (bucketOrd != cachedBucket) {
                        cachedBitmap = getOrCreateBitmap(bucketOrd);
                        cachedBucket = bucketOrd;
                    }
                    int count = globalOrds.docValueCount();
                    long ord;
                    while ((count-- > 0) && (ord = globalOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                        cachedBitmap.add((int) ord);
                    }
                    collectsSinceLastCheck++;
                    if (collectsSinceLastCheck >= CHECK_INTERVAL) {
                        flushBreakerDelta();
                    }
                }

                private void flushBreakerDelta() {
                    collectsSinceLastCheck = 0;
                    long currentBytes = cachedBitmap != null ? cachedBitmap.getSizeInBytes() : 0;
                    long delta = currentBytes - lastKnownBytes;
                    if (delta > 0) {
                        updateBreaker(delta);
                        lastKnownBytes = currentBytes;
                    }
                }

                @Override
                public void postCollect() {
                    flushBreakerDelta();
                }

                @Override
                public void close() {
                    // no-op — DeferredOrdinalsCollector owns the lifecycle
                }
            };
        }

        private RoaringBitmap getOrCreateBitmap(long bucket) {
            visitedOrds = bigArrays.grow(visitedOrds, bucket + 1);
            RoaringBitmap bitmap = visitedOrds.get(bucket);
            if (bitmap == null) {
                bitmap = new RoaringBitmap();
                visitedOrds.set(bucket, bitmap);
                bucketsCollected++;
            }
            return bitmap;
        }

        private void updateBreaker(long additionalBytes) {
            if (additionalBytes > 0) {
                bigArrays.breakerService()
                    .getBreaker(CircuitBreaker.REQUEST)
                    .addEstimateBytesAndMaybeBreak(additionalBytes, "roaring-cardinality");
                roaringBytesTracked += additionalBytes;
            }
        }

        /**
         * Returns the exact cardinality for a bucket (number of distinct ordinals).
         */
        public long ordinalCardinality(long bucketOrd) {
            if (bucketOrd >= visitedOrds.size()) return 0;
            RoaringBitmap bitmap = visitedOrds.get(bucketOrd);
            return bitmap == null ? 0 : bitmap.getLongCardinality();
        }

        /**
         * Populate HLL for the given buckets only. Uses global ordinals to look up
         * byte values and hashes on-the-fly — no maxOrd-sized array needed.
         */
        /**
         * Materialize HLL for a single source bucket into the given target HLL at targetBucket.
         * This avoids growing the shared {@code counts} array to fit large bucket ordinals.
         */
        public void materializeHLL(HyperLogLogPlusPlus targetHLL, long targetBucket, long sourceBucket) throws IOException {
            if (sourceBucket >= visitedOrds.size()) return;
            RoaringBitmap bitmap = visitedOrds.get(sourceBucket);
            if (bitmap == null) return;
            bucketsMaterialized++;
            // Use the first non-empty leaf for global ordinals lookup.
            // Global ordinals share a unified term dictionary across all segments,
            // so lookupOrd works correctly from any leaf's global ordinals view.
            SortedSetDocValues globalOrds = null;
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                globalOrds = valuesSource.globalOrdinalsValues(leaf);
                if (globalOrds.getValueCount() > 0) break;
            }
            if (globalOrds == null || globalOrds.getValueCount() == 0) return;
            final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
            PeekableIntIterator it = bitmap.getIntIterator();
            while (it.hasNext()) {
                final BytesRef value = globalOrds.lookupOrd(it.next());
                MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                targetHLL.collect(targetBucket, hash.h1);
            }
        }

        @Override
        public void close() {
            if (roaringBytesTracked > 0) {
                bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).addWithoutBreaking(-roaringBytesTracked);
                roaringBytesTracked = 0;
            }
            Releasables.close(visitedOrds);
        }
    }

    /**
     * A collector that uses {@link RoaringBitmap} per bucket with segment-local ordinals.
     * More memory-efficient than {@link OrdinalsCollector} when buckets are sparse (each bucket
     * touches a small fraction of the ordinal space).
     *
     * <p>Unlike {@link DeferredOrdinalsCollector}, this does NOT require global ordinals.
     * It works with segment-local ordinals and hashes all visited ordinals in {@link #postCollect()},
     * similar to {@link OrdinalsCollector} but without allocating a {@code maxOrd}-sized array.
     *
     * <p>Memory monitoring: tracks roaring bitmap bytes and throws {@link MemoryLimitExceededException}
     * when the threshold is exceeded, allowing {@link HybridCollector} to fall back to {@link DirectCollector}.
     *
     * @opensearch.internal
     */
    static class RoaringOrdinalsCollector extends Collector {

        private final BigArrays bigArrays;
        private final SortedSetDocValues values;
        private final HyperLogLogPlusPlus counts;
        private final long memoryThreshold;
        private ObjectArray<RoaringBitmap> visitedOrds;
        private long trackedBytes;
        private long collectsSinceLastCheck;
        private static final long CHECK_INTERVAL = 1024;

        RoaringOrdinalsCollector(HyperLogLogPlusPlus counts, SortedSetDocValues values, BigArrays bigArrays, long memoryThreshold) {
            this.bigArrays = bigArrays;
            this.values = values;
            this.counts = counts;
            this.memoryThreshold = memoryThreshold;
            this.visitedOrds = bigArrays.newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (values.advanceExact(doc)) {
                RoaringBitmap bitmap = getOrCreateBitmap(bucketOrd);
                int count = values.docValueCount();
                long ord;
                while ((count-- > 0) && (ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                    bitmap.add((int) ord);
                }
                collectsSinceLastCheck++;
                if (collectsSinceLastCheck >= CHECK_INTERVAL) {
                    checkMemory();
                }
            }
        }

        private RoaringBitmap getOrCreateBitmap(long bucket) {
            visitedOrds = bigArrays.grow(visitedOrds, bucket + 1);
            RoaringBitmap bitmap = visitedOrds.get(bucket);
            if (bitmap == null) {
                bitmap = new RoaringBitmap();
                visitedOrds.set(bucket, bitmap);
                checkMemory();
            }
            return bitmap;
        }

        private void checkMemory() {
            collectsSinceLastCheck = 0;
            long currentBytes = computeCurrentBytes();
            long delta = currentBytes - trackedBytes;
            if (delta > 0) {
                bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).addEstimateBytesAndMaybeBreak(delta, "roaring-cardinality");
                trackedBytes = currentBytes;
            } else if (delta < 0) {
                bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).addWithoutBreaking(delta);
                trackedBytes = currentBytes;
            }
            if (trackedBytes > memoryThreshold) {
                throw MemoryLimitExceededException.INSTANCE;
            }
        }

        private long computeCurrentBytes() {
            long bytes = 0;
            for (long i = visitedOrds.size() - 1; i >= 0; --i) {
                RoaringBitmap bitmap = visitedOrds.get(i);
                if (bitmap != null) {
                    bytes += bitmap.getSizeInBytes();
                }
            }
            return bytes;
        }

        @Override
        public void postCollect() throws IOException {
            // Step 1: Union all per-bucket bitmaps to find all visited ordinals
            RoaringBitmap allVisited = new RoaringBitmap();
            for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                RoaringBitmap bm = visitedOrds.get(bucket);
                if (bm != null) {
                    allVisited.or(bm);
                }
            }

            int visitedCount = allVisited.getCardinality();
            if (visitedCount == 0) return;

            // Step 2: Hash only visited ordinals — array sized to actual cardinality, not maxOrd
            // Track this temporary allocation via circuit breaker
            long hashesBytes = (long) visitedCount * Long.BYTES;
            bigArrays.breakerService()
                .getBreaker(CircuitBreaker.REQUEST)
                .addEstimateBytesAndMaybeBreak(hashesBytes, "roaring-cardinality-hashes");
            try {
                long[] hashes = new long[visitedCount];
                final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                PeekableIntIterator it = allVisited.getIntIterator();
                for (int i = 0; i < visitedCount; i++) {
                    int ord = it.next();
                    final BytesRef value = values.lookupOrd(ord);
                    MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes[i] = hash.h1;
                }

                // Step 3: For each bucket, distribute hashes using lockstep iteration
                // Both allVisited and bucket bitmap are sorted, so we iterate in lockstep
                // to find the index without expensive rank() calls
                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    RoaringBitmap bm = visitedOrds.get(bucket);
                    if (bm != null) {
                        PeekableIntIterator allIt = allVisited.getIntIterator();
                        PeekableIntIterator bucketIt = bm.getIntIterator();
                        int idx = 0;
                        while (bucketIt.hasNext()) {
                            int target = bucketIt.next();
                            // Advance allIt to match target (both are sorted)
                            while (allIt.hasNext()) {
                                int allOrd = allIt.next();
                                if (allOrd == target) {
                                    counts.collect(bucket, hashes[idx]);
                                    idx++;
                                    break;
                                }
                                idx++;
                            }
                        }
                    }
                }
            } finally {
                bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).addWithoutBreaking(-hashesBytes);
            }
        }

        @Override
        public void close() {
            if (trackedBytes > 0) {
                bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST).addWithoutBreaking(-trackedBytes);
                trackedBytes = 0;
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

    /**
     * Hybrid Collector that starts with OrdinalsCollector and switches to DirectCollector
     * when memory consumption exceeds a threshold. Uses exception-based control flow for
     * optimal performance - zero overhead when memory is under threshold.
     */
    /**
     * Determines the starting strategy for the {@link HybridCollector}.
     */
    enum HybridStartingStrategy {
        /** Start with {@link OrdinalsCollector} (BitArray per bucket). Best when dense. */
        BITARRAY,
        /** Start with {@link RoaringOrdinalsCollector}. Best when sparse, no global ordinals. */
        ROARING
    }

    /**
     * Hybrid Collector that starts with an ordinal-based collector (BitArray or Roaring)
     * and switches to DirectCollector when memory consumption exceeds a threshold.
     * Uses exception-based control flow for optimal performance — zero overhead when
     * memory is under threshold.
     */
    static class HybridCollector extends Collector {
        private final HyperLogLogPlusPlus counts;
        private final MurmurHash3Values hashValues;

        private Collector activeCollector;
        private final Collector startingCollector;

        /**
         * Create a HybridCollector that starts with BitArray (existing behavior).
         */
        HybridCollector(
            HyperLogLogPlusPlus counts,
            SortedSetDocValues ordinalValues,
            MurmurHash3Values hashValues,
            BigArrays bigArrays,
            CardinalityAggregationContext cardinalityContext
        ) {
            this(counts, ordinalValues, hashValues, bigArrays, cardinalityContext, HybridStartingStrategy.BITARRAY);
        }

        /**
         * Create a HybridCollector with an explicit starting strategy.
         */
        HybridCollector(
            HyperLogLogPlusPlus counts,
            SortedSetDocValues ordinalValues,
            MurmurHash3Values hashValues,
            BigArrays bigArrays,
            CardinalityAggregationContext cardinalityContext,
            HybridStartingStrategy strategy
        ) {
            this.counts = counts;
            this.hashValues = hashValues;

            if (strategy == HybridStartingStrategy.ROARING) {
                this.startingCollector = new RoaringOrdinalsCollector(
                    counts,
                    ordinalValues,
                    bigArrays,
                    cardinalityContext.getMemoryThreshold()
                );
            } else {
                this.startingCollector = new OrdinalsCollector(
                    counts,
                    ordinalValues,
                    bigArrays,
                    cardinalityContext.getMemoryThreshold(),
                    true
                );
            }
            this.activeCollector = startingCollector;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            try {
                activeCollector.collect(doc, bucketOrd);
            } catch (MemoryLimitExceededException e) {
                switchToDirectCollector();
                activeCollector.collect(doc, bucketOrd);
            }
        }

        @Override
        public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
            try {
                activeCollector.collect(stream, owningBucketOrd);
            } catch (MemoryLimitExceededException e) {
                switchToDirectCollector();
                activeCollector.collect(stream, owningBucketOrd);
            }
        }

        @Override
        public void collectRange(int minDoc, int maxDoc) throws IOException {
            try {
                activeCollector.collectRange(minDoc, maxDoc);
            } catch (MemoryLimitExceededException e) {
                switchToDirectCollector();
                activeCollector.collectRange(minDoc, maxDoc);
            }
        }

        private void switchToDirectCollector() throws IOException {
            startingCollector.postCollect();
            startingCollector.close();
            logger.debug("Hybrid collector switching to DirectCollector due to memory threshold exceeded");
            activeCollector = new DirectCollector(counts, hashValues);
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
