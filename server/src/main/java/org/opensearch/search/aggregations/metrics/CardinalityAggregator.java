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
import org.apache.lucene.index.IndexReader;
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
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
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

    /**
     * Below this precision a {@link HyperLogLogPlusPlus} sketch is so small that {@link CompactHyperLogLogPlusPlus} would not
     * save memory.
     */
    static final int COMPACT_SKETCHES_MIN_PRECISION = 10;

    /**
     * Setting to keep the sketches of cardinality under a parent with many buckets in {@link CompactHyperLogLogPlusPlus}, so
     * that each bucket only pays for the values it holds. Disabling it goes back to {@link HyperLogLogPlusPlus}, which reserves
     * {@code 2^precision} bytes for every bucket.
     */
    public static final Setting<Boolean> CARDINALITY_AGGREGATION_COMPACT_SKETCHES_ENABLED = Setting.boolSetting(
        "search.aggregations.cardinality.compact_sketches.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    final CardinalityAggregatorFactory.ExecutionMode executionMode;
    final int precision;
    final ValuesSource valuesSource;

    private final ValuesSourceConfig valuesSourceConfig;
    private final boolean compactSketches;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    AbstractHyperLogLogPlusPlus counts;

    Collector collector;

    /**
     * Global ordinals of each bucket while collection is deferred, see {@link #startDeferral}. {@code null} when collection
     * isn't deferred or stopped deferring.
     */
    @Nullable
    DeferredOrdinals deferred;
    private boolean deferralDecided;
    /** Whether collection was deferred, or why not, for the profile. */
    String deferredOrdinals = "not_started";
    int deferredSegments;
    int deferredBucketsBuilt;
    /** Buckets that {@link #prepareBuildAggregations} hashed out of {@link #deferred}, sorted. */
    private long[] deferredBucketsHashed = new long[0];
    /** Whether deferred ordinals moved from bits to bitmaps, once collection stopped deferring. */
    private boolean deferredUsedBitmaps;

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
        this(name, valuesSourceConfig, precision, context, parent, CardinalityUpperBound.ONE, metadata, executionMode);
    }

    public CardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata,
        CardinalityAggregatorFactory.ExecutionMode executionMode
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        this.precision = precision;
        final CardinalityAggregationContext cardinalityContext = context.cardinalityAggregationContext();
        this.compactSketches = (cardinalityContext == null || cardinalityContext.isCompactSketchesEnabled())
            && useCompactSketches(cardinality, precision);
        this.counts = valuesSource == null ? null : newCounts();
        this.valuesSourceConfig = valuesSourceConfig;
        this.executionMode = executionMode;
    }

    /**
     * Whether to keep the sketches in a {@link CompactHyperLogLogPlusPlus}: worth it when the parent can collect into more
     * than one bucket, as {@link HyperLogLogPlusPlus} reserves {@code 2^precision} bytes for every bucket.
     */
    static boolean useCompactSketches(CardinalityUpperBound cardinality, int precision) {
        return precision >= COMPACT_SKETCHES_MIN_PRECISION && cardinality.map(buckets -> buckets > 1);
    }

    /**
     * Creates the sketches that the collectors hash values into.
     */
    final AbstractHyperLogLogPlusPlus newCounts() {
        if (compactSketches) {
            return new CompactHyperLogLogPlusPlus(precision, context.bigArrays(), this::addRequestCircuitBreakerBytes);
        }
        return new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
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
            if (deferralDecided == false) {
                deferralDecided = true;
                deferred = startDeferral(source, ctx);
            }
            if (deferred != null) {
                deferredSegments++;
                return new DeferringCollector(source, ctx, deferred.leafCollector(ctx));
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

    /**
     * Picks the collector for the ordinals of one segment, honouring {@code execution_hint} and the hybrid collector settings.
     */
    Collector ordinalsCollector(ValuesSource.Bytes.WithOrdinals source, SortedSetDocValues ordinalValues, LeafReaderContext ctx)
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
                // Open the doc values before allocating the bitmaps so that a failure can't leak them
                SortedSetDocValues values = source.ordinalsValues(ctx);
                BucketOrdinalBitmaps bitmaps = new BucketOrdinalBitmaps(context.bigArrays(), this::addRequestCircuitBreakerBytes);
                return new RoaringOrdinalsCollector(counts, values, bitmaps, memoryThreshold);
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

    /**
     * Starts collecting the global ordinals of each bucket for the whole shard instead of hashing segment ordinals into
     * {@link #counts}, so that the parent ranks its buckets by their exact number of distinct values and only the buckets it
     * keeps are hashed, in {@link #buildAggregation}. That needs compact sketches, a field with ordinals and global ordinals
     * that fit in an {@code int}. Without {@code execution_hint: deferred_ordinals} it is only worth it when a {@code terms}
     * parent orders its buckets by this aggregation and global ordinals are already loaded, as building them can cost more
     * than it saves. Returns {@code null}, recording why in {@link #deferredOrdinals}, when collection isn't deferred.
     */
    private DeferredOrdinals startDeferral(ValuesSource.Bytes.WithOrdinals source, LeafReaderContext ctx) throws IOException {
        final boolean forced = executionMode == CardinalityAggregatorFactory.ExecutionMode.DEFERRED_ORDINALS;
        if (executionMode == CardinalityAggregatorFactory.ExecutionMode.DIRECT
            || executionMode == CardinalityAggregatorFactory.ExecutionMode.ORDINALS) {
            deferredOrdinals = "execution_hint";
            return null;
        }
        if (compactSketches == false) {
            deferredOrdinals = "no_compact_sketches";
            return null;
        }
        if (valuesSourceConfig.fieldContext() == null
            || valuesSourceConfig.script() != null
            || valuesSourceConfig.missing() != null
            || source.supportsGlobalOrdinalsMapping() == false) {
            deferredOrdinals = "unsupported_values_source";
            return null;
        }
        if (forced == false && (parent instanceof TermsAggregator terms && terms.isOrderedBySubAggregation(name)) == false) {
            deferredOrdinals = "not_ordered_by_parent";
            return null;
        }
        final IndexReader reader = ctx.parent.reader();
        if (reader instanceof DirectoryReader == false) {
            deferredOrdinals = "unsupported_reader";
            return null;
        }
        if (forced == false && globalOrdinalsLoaded((DirectoryReader) reader) == false) {
            deferredOrdinals = "global_ordinals_not_loaded";
            return null;
        }
        final long maxOrd = source.globalOrdinalsValues(ctx).getValueCount();
        if (maxOrd > Integer.MAX_VALUE) {
            deferredOrdinals = "too_many_ordinals";
            return null;
        }
        final long memoryThreshold = context.cardinalityAggregationContext().getMemoryThreshold();
        final DeferredOrdinals deferred = new DeferredOrdinals(
            source,
            ctx,
            maxOrd,
            context.bigArrays(),
            this::addRequestCircuitBreakerBytes,
            memoryThreshold
        );
        deferredOrdinals = "used";
        return deferred;
    }

    private boolean globalOrdinalsLoaded(DirectoryReader reader) {
        final IndexFieldData<?> fieldData = context.getQueryShardContext().getForField(valuesSourceConfig.fieldContext().fieldType());
        return fieldData instanceof IndexOrdinalsFieldData ordinals && ordinals.isGlobalOrdinalsLoaded(reader);
    }

    /**
     * Hashes the global ordinals collected so far into {@link #counts} and goes back to collecting segment ordinals.
     */
    private void stopDeferring() throws IOException {
        final DeferredOrdinals stopped = deferred;
        deferred = null;
        deferredOrdinals = "stopped_over_memory_threshold";
        deferredUsedBitmaps = stopped.usesBitmaps();
        try {
            stopped.hashAllInto(counts);
        } finally {
            stopped.close();
        }
    }

    /**
     * Collects a segment into {@link #deferred} until its bitmaps exceed the memory threshold, then hands everything they
     * hold to {@link #counts} and finishes the segment, and the following ones, with the collector that would have been used
     * without deferring. The document that went over the threshold is collected again, which doesn't change the sketches.
     */
    private class DeferringCollector extends Collector {
        private final ValuesSource.Bytes.WithOrdinals source;
        private final LeafReaderContext ctx;
        private Collector active;

        DeferringCollector(ValuesSource.Bytes.WithOrdinals source, LeafReaderContext ctx, Collector deferredCollector) {
            this.source = source;
            this.ctx = ctx;
            this.active = deferredCollector;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            try {
                active.collect(doc, owningBucketOrd);
            } catch (MemoryLimitExceededException e) {
                final Collector previous = active;
                active = null;
                previous.close();
                stopDeferring();
                active = ordinalsCollector(source, source.ordinalsValues(ctx), ctx);
                logger.debug("Cardinality aggregation [{}] stopped deferring collection: global ordinals exceed the threshold", name);
                active.collect(doc, owningBucketOrd);
            }
        }

        @Override
        public void postCollect() throws IOException {
            active.postCollect();
        }

        @Override
        public void close() {
            Releasables.close(active);
        }
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
        if (deferred != null) {
            return deferred.cardinality(owningBucketOrd);
        }
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    /**
     * Hashes the values of the deferred buckets about to be built all at once, so that values they share are looked up and
     * hashed once.
     */
    @Override
    protected void prepareBuildAggregations(long[] owningBucketOrds) throws IOException {
        if (deferred == null) {
            return;
        }
        final long[] buckets = Arrays.stream(owningBucketOrds)
            .filter(bucket -> deferred.cardinality(bucket) > 0)
            .sorted()
            .distinct()
            .toArray();
        deferred.hashBucketsInto(counts, buckets);
        deferredBucketsBuilt += buckets.length;
        deferredBucketsHashed = buckets;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        if (deferred != null) {
            if (deferred.cardinality(owningBucketOrdinal) == 0) {
                return buildEmptyAggregation();
            }
            if (Arrays.binarySearch(deferredBucketsHashed, owningBucketOrdinal) < 0) {
                deferredBucketsBuilt++;
                deferred.hashBucketsInto(counts, new long[] { owningBucketOrdinal });
            }
        }
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
        Releasables.close(counts, collector, deferred);
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
        add.accept("compact_sketches", compactSketches);
        add.accept("deferred_ordinals", deferredOrdinals);
        add.accept("deferred_bitmaps", deferred != null ? deferred.usesBitmaps() : deferredUsedBitmaps);
        add.accept("deferred_segments", deferredSegments);
        add.accept("deferred_buckets_built", deferredBucketsBuilt);
        if (counts instanceof CompactHyperLogLogPlusPlus compact) {
            add.accept("hyperloglog_buckets", compact.hyperLogLogBuckets());
            add.accept("linear_counting_bytes", compact.linearCountingBytes());
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
        private final AbstractHyperLogLogPlusPlus counts;

        DirectCollector(AbstractHyperLogLogPlusPlus counts, MurmurHash3Values values) {
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

        private final SortedSetDocValues values;
        private final AbstractHyperLogLogPlusPlus counts;
        private final BucketOrdinalBits visitedOrds;

        OrdinalsCollector(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values, BigArrays bigArrays) {
            this(counts, values, bigArrays, Long.MAX_VALUE, false);
        }

        OrdinalsCollector(
            AbstractHyperLogLogPlusPlus counts,
            SortedSetDocValues values,
            BigArrays bigArrays,
            long memoryThreshold,
            boolean memoryMonitoringEnabled
        ) {
            this.counts = counts;
            this.values = values;
            this.visitedOrds = new BucketOrdinalBits(
                bigArrays,
                values.getValueCount(),
                memoryMonitoringEnabled ? memoryThreshold : Long.MAX_VALUE,
                false
            );
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
            final BitArray bits = visitedOrds.bucket(bucket);
            if (bits == null) {
                // Throw singleton exception for efficient control flow to HybridCollector
                throw MemoryLimitExceededException.INSTANCE;
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
            visitedOrds.collectInto(counts, values);
        }

        public long getCurrentMemoryUsage() {
            return visitedOrds.memoryUsed();
        }

        @Override
        public void close() {
            visitedOrds.close();
        }
    }

    /**
     * A {@link BitArray} of ordinals per bucket. Adding an ordinal only sets a bit, but every bucket costs {@code maxOrd / 8}
     * bytes however few ordinals it holds, so a bucket only gets its bits while they all fit under a memory threshold.
     * {@link OrdinalsCollector} keeps the segment ordinals of each bucket here, and {@link DeferredOrdinals} the global
     * ordinals of each bucket of a shard, with their number when asked to count them.
     * <p>
     * When counting, the bits also stop growing past {@code 1 / }{@value #SPARSE_SHARE} of the threshold while fewer than one
     * bit in {@value #SPARSE_DENSITY} is set: below that density {@link RoaringBitmap} keeps ordinals in sorted arrays of 16 bits
     * each, which take less memory than the bits. Dense buckets keep their bits up to the threshold, as roaring bitmaps would
     * be about as big and slower to add to. The density is the one seen so far, so buckets that only get dense later may stop
     * early, which costs speed but not correctness.
     *
     * @opensearch.internal
     */
    static final class BucketOrdinalBits implements Releasable {
        /** Ordinals hashed at a time when hashing some of the buckets. */
        static final int HASH_CHUNK = 8192;
        static final int SPARSE_SHARE = 8;
        static final int SPARSE_DENSITY = 16;

        private final BigArrays bigArrays;
        private final int maxOrd;
        private final long memoryThreshold;
        private ObjectArray<BitArray> bits;
        /** Number of ordinals of each bucket, {@code null} when not counting them. */
        @Nullable
        private IntArray ordinalCounts;
        /** Total of {@link #ordinalCounts}. */
        private long ordinals;
        private long bucketsWithBits;
        private long memoryUsed;

        BucketOrdinalBits(BigArrays bigArrays, long maxOrd, long memoryThreshold, boolean countOrdinals) {
            if (maxOrd > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            this.bigArrays = bigArrays;
            this.maxOrd = (int) maxOrd;
            this.memoryThreshold = memoryThreshold;
            boolean success = false;
            try {
                bits = bigArrays.newObjectArray(1);
                if (countOrdinals) {
                    ordinalCounts = bigArrays.newIntArray(1, true);
                }
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        /**
         * The bits of {@code bucket}, or {@code null} if the bucket has none and giving it some would take the memory used over
         * the threshold, or over its sparse share when counting sparse ordinals.
         */
        @Nullable
        BitArray bucket(long bucket) {
            if (bucket >= bits.size()) {
                bits = bigArrays.grow(bits, bucket + 1);
            }
            BitArray bucketBits = bits.get(bucket);
            if (bucketBits == null) {
                final long overhead = OrdinalsCollector.memoryOverhead(maxOrd);
                if (memoryUsed + overhead > memoryThreshold) {
                    return null;
                }
                if (ordinalCounts != null) {
                    if (memoryUsed + overhead > memoryThreshold / SPARSE_SHARE && ordinals * SPARSE_DENSITY < bucketsWithBits * maxOrd) {
                        return null;
                    }
                    ordinalCounts = bigArrays.grow(ordinalCounts, bucket + 1);
                }
                bucketBits = new BitArray(maxOrd, bigArrays);
                bits.set(bucket, bucketBits);
                bucketsWithBits++;
                memoryUsed += overhead;
            }
            return bucketBits;
        }

        /**
         * Adds {@code ord} to {@code bucketBits}, the bits of {@code bucket}, counting it if it is new. Only when counting.
         */
        void addCounted(long bucket, BitArray bucketBits, long ord) {
            if (bucketBits.get(ord) == false) {
                bucketBits.set(ord);
                ordinalCounts.increment(bucket, 1);
                ordinals++;
            }
        }

        /**
         * Number of distinct ordinals of {@code bucket}. Only when counting.
         */
        long cardinality(long bucket) {
            return bucket < ordinalCounts.size() ? ordinalCounts.get(bucket) : 0;
        }

        /**
         * Memory used by the bits, to compare with the threshold. {@link BigArrays} charges them to the circuit breaker.
         */
        long memoryUsed() {
            return memoryUsed;
        }

        private long nextOrd(BitArray bucketBits, long ord) {
            return ord + 1 < maxOrd ? bucketBits.nextSetBit(ord + 1) : Long.MAX_VALUE;
        }

        /**
         * Hashes every distinct ordinal once, then adds the hashes of each bucket's ordinals to that bucket of {@code counts}.
         *
         * @param values resolves the ordinals to their values
         */
        void collectInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values) throws IOException {
            try (BitArray allVisitedOrds = new BitArray(maxOrd, bigArrays)) {
                for (long bucket = bits.size() - 1; bucket >= 0; --bucket) {
                    final BitArray bucketBits = bits.get(bucket);
                    if (bucketBits != null) {
                        allVisitedOrds.or(bucketBits);
                    }
                }

                try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                    final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                    for (long ord = allVisitedOrds.nextSetBit(0); ord < Long.MAX_VALUE; ord = nextOrd(allVisitedOrds, ord)) {
                        final BytesRef value = values.lookupOrd(ord);
                        MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                        hashes.set(ord, hash.h1);
                    }

                    for (long bucket = bits.size() - 1; bucket >= 0; --bucket) {
                        final BitArray bucketBits = bits.get(bucket);
                        if (bucketBits != null) {
                            for (long ord = bucketBits.nextSetBit(0); ord < Long.MAX_VALUE; ord = nextOrd(bucketBits, ord)) {
                                counts.collect(bucket, hashes.get(ord));
                            }
                        }
                    }
                }
            }
        }

        /**
         * Hashes the ordinals of {@code buckets} into the same buckets of {@code counts}. Every distinct ordinal is looked up once
         * and in ascending order, which is the cheapest way to read a terms dictionary, {@value #HASH_CHUNK} ordinals at a time
         * so that only the hashes of one chunk are held.
         *
         * @param values resolves the ordinals to their values
         * @param buckets distinct buckets
         */
        void collectInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values, long[] buckets) throws IOException {
            final BitArray[] selected = new BitArray[buckets.length];
            final long[] selectedBuckets = new long[buckets.length];
            // the next ordinal of each selected bucket to hash
            final long[] next = new long[buckets.length];
            int n = 0;
            for (long bucket : buckets) {
                final BitArray bucketBits = bucket < bits.size() ? bits.get(bucket) : null;
                final long first = bucketBits == null ? Long.MAX_VALUE : bucketBits.nextSetBit(0);
                if (first < maxOrd) {
                    selected[n] = bucketBits;
                    selectedBuckets[n] = bucket;
                    next[n] = first;
                    n++;
                }
            }
            final long[] chunk = new long[HASH_CHUNK / Long.SIZE];
            final long[] hashes = new long[HASH_CHUNK];
            final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
            while (true) {
                long min = Long.MAX_VALUE;
                for (int i = 0; i < n; i++) {
                    min = Math.min(min, next[i]);
                }
                if (min == Long.MAX_VALUE) {
                    return;
                }
                final long start = min - min % HASH_CHUNK;
                final long end = start + HASH_CHUNK;
                Arrays.fill(chunk, 0L);
                for (int i = 0; i < n; i++) {
                    for (long ord = next[i]; ord < end; ord = nextOrd(selected[i], ord)) {
                        final int offset = (int) (ord - start);
                        chunk[offset >>> 6] |= 1L << offset;
                    }
                }
                for (int w = 0; w < chunk.length; w++) {
                    for (long word = chunk[w]; word != 0; word &= word - 1) {
                        final int offset = (w << 6) | Long.numberOfTrailingZeros(word);
                        final BytesRef value = values.lookupOrd(start + offset);
                        MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                        hashes[offset] = hash.h1;
                    }
                }
                for (int i = 0; i < n; i++) {
                    long ord = next[i];
                    for (; ord < end; ord = nextOrd(selected[i], ord)) {
                        counts.collect(selectedBuckets[i], hashes[(int) (ord - start)]);
                    }
                    next[i] = ord;
                }
            }
        }

        /**
         * Adds the ordinals of every bucket to the same bucket of {@code bitmaps}.
         */
        void copyTo(BucketOrdinalBitmaps bitmaps) {
            for (long bucket = 0; bucket < bits.size(); bucket++) {
                final BitArray bucketBits = bits.get(bucket);
                if (bucketBits != null) {
                    for (long ord = bucketBits.nextSetBit(0); ord < Long.MAX_VALUE; ord = nextOrd(bucketBits, ord)) {
                        bitmaps.add(bucket, (int) ord);
                    }
                }
            }
            bitmaps.flushBreaker();
        }

        @Override
        public void close() {
            if (bits != null) {
                for (long i = 0; i < bits.size(); i++) {
                    Releasables.close(bits.get(i));
                }
            }
            Releasables.close(bits, ordinalCounts);
        }
    }

    /**
     * Per-bucket sets of ordinals with an estimate of their heap usage that is charged to the request circuit breaker as they
     * grow. A bucket keeps its first ordinal inline and only gets a {@link RoaringBitmap} for its second one: under a parent
     * with many buckets most buckets hold a single value, and a bitmap costs about a hundred bytes of objects.
     * <p>
     * {@link RoaringBitmap#getSizeInBytes()} leaves out object headers, which dominate small bitmaps, so a fixed overhead per
     * bitmap and per container is added on top. A value that opens a new container is charged right away. Values added to
     * existing containers cost at most 2 bytes each and are picked up by re-measuring the bitmap whenever its number of values
     * doubles, and every {@value #MAX_VALUES_BETWEEN_MEASUREMENTS} values once it is large. This keeps the estimate within 2x of
     * the bitmap's size at an amortized constant cost per value. The inline ordinals are in a {@link BigArrays} array, which
     * accounts for itself.
     *
     * @opensearch.internal
     */
    static final class BucketOrdinalBitmaps implements Releasable {
        /**
         * A bitmap that also keeps the number of values added to it and the bytes charged for it.
         */
        static final class CountedBitmap extends RoaringBitmap {
            private int values;
            private long accountedBytes;
        }

        static final long BITMAP_OVERHEAD_BYTES = RamUsageEstimator.shallowSizeOfInstance(CountedBitmap.class) + RamUsageEstimator
            .shallowSizeOfInstance(RoaringArray.class) + 2L * RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        static final long CONTAINER_OVERHEAD_BYTES = RamUsageEstimator.shallowSizeOfInstance(ArrayContainer.class)
            + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        // A new container holding one value adds 8 bytes to RoaringBitmap#getSizeInBytes: its key, header and the value
        static final long NEW_CONTAINER_BYTES = CONTAINER_OVERHEAD_BYTES + 8;
        static final int MAX_VALUES_BETWEEN_MEASUREMENTS = 4096;
        /** Buckets with up to this many ordinals look each of them up by rank rather than with a rank iterator. */
        static final int RANK_LOOKUP_MAX_VALUES = 64;
        private static final long BREAKER_FLUSH_BYTES = 64 * 1024;

        private final BigArrays bigArrays;
        private final LongConsumer breaker;
        /** {@code ordinal + 1} of a bucket's only ordinal, {@code 0} for a bucket without ordinals or with a bitmap. */
        private IntArray firstOrds;
        /** Bitmap of each bucket with at least two ordinals, grown to the highest such bucket. */
        private ObjectArray<CountedBitmap> bitmaps;
        private long maxBucket = -1;
        private long totalBytes;
        private long breakerBytes;

        BucketOrdinalBitmaps(BigArrays bigArrays, LongConsumer breaker) {
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            boolean success = false;
            try {
                firstOrds = bigArrays.newIntArray(1, true);
                bitmaps = bigArrays.newObjectArray(1);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        /**
         * The bitmap of {@code bucket}, or {@code null} if it holds fewer than two ordinals.
         */
        RoaringBitmap get(long bucket) {
            return bucket < bitmaps.size() ? bitmaps.get(bucket) : null;
        }

        /**
         * Adds {@code ord} to {@code bucket}.
         */
        void add(long bucket, int ord) {
            final CountedBitmap bitmap = bucket < bitmaps.size() ? bitmaps.get(bucket) : null;
            if (bitmap != null) {
                addToBitmap(bitmap, ord);
                return;
            }
            if (bucket >= firstOrds.size()) {
                firstOrds = bigArrays.grow(firstOrds, bucket + 1);
            }
            final int first = firstOrds.get(bucket);
            if (first == 0) {
                firstOrds.set(bucket, ord + 1);
                maxBucket = Math.max(maxBucket, bucket);
            } else if (first != ord + 1) {
                // A second distinct ordinal: the bucket moves to a bitmap
                bitmaps = bigArrays.grow(bitmaps, bucket + 1);
                final CountedBitmap created = new CountedBitmap();
                created.add(first - 1);
                created.add(ord);
                created.values = 2;
                bitmaps.set(bucket, created);
                firstOrds.set(bucket, 0);
                account(created);
            }
        }

        private void addToBitmap(CountedBitmap bitmap, int ord) {
            final int containers = bitmap.getContainerCount();
            if (bitmap.checkedAdd(ord)) {
                if (shouldMeasure(++bitmap.values)) {
                    account(bitmap);
                } else if (bitmap.getContainerCount() != containers) {
                    charge(bitmap, NEW_CONTAINER_BYTES);
                }
            }
        }

        private static boolean shouldMeasure(int values) {
            if (values <= MAX_VALUES_BETWEEN_MEASUREMENTS) {
                return (values & (values - 1)) == 0; // power of two
            }
            return values % MAX_VALUES_BETWEEN_MEASUREMENTS == 0;
        }

        /**
         * Number of distinct ordinals in {@code bucket}.
         */
        long cardinality(long bucket) {
            final RoaringBitmap bitmap = get(bucket);
            if (bitmap != null) {
                return bitmap.getLongCardinality();
            }
            return bucket < firstOrds.size() && firstOrds.get(bucket) != 0 ? 1 : 0;
        }

        /**
         * The only ordinal of {@code bucket}, or {@code -1} if it holds none or a bitmap.
         */
        int singleOrd(long bucket) {
            return bucket < firstOrds.size() ? firstOrds.get(bucket) - 1 : -1;
        }

        /**
         * Estimated heap used by {@code bitmap}.
         */
        static long ramBytesUsed(RoaringBitmap bitmap) {
            return BITMAP_OVERHEAD_BYTES + bitmap.getSizeInBytes() + (long) bitmap.getContainerCount() * CONTAINER_OVERHEAD_BYTES;
        }

        private void account(CountedBitmap bitmap) {
            charge(bitmap, ramBytesUsed(bitmap) - bitmap.accountedBytes);
        }

        private void charge(CountedBitmap bitmap, long bytes) {
            bitmap.accountedBytes += bytes;
            totalBytes += bytes;
            // The estimate only shrinks by a few bytes when an array container turns into a bitmap container, and close()
            // releases whatever was charged, so only growth needs flushing.
            if (totalBytes - breakerBytes >= BREAKER_FLUSH_BYTES) {
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

        /**
         * Estimated heap used by the bitmaps, charged to the circuit breaker by {@link #flushBreaker()}.
         */
        long bytesUsed() {
            return totalBytes;
        }

        /**
         * Heap used by the bitmaps and the per-bucket arrays, to compare with a memory threshold.
         */
        long memoryUsed() {
            return totalBytes + firstOrds.ramBytesUsed() + bitmaps.ramBytesUsed();
        }

        /**
         * Hashes every distinct ordinal once, then adds the hashes of each bucket's ordinals to that bucket of {@code counts}.
         * <p>
         * The hash of an ordinal is found by the ordinal itself when the buckets hold at least half of the ordinals, and by its
         * rank among the ordinals they hold otherwise. Ranks come from a rank iterator for buckets with many ordinals, which only
         * moves forward, and from {@link FastRankRoaringBitmap#rank} for the others, so that millions of tiny buckets don't
         * create an iterator each.
         *
         * @param values resolves the ordinals held by the bitmaps to their values
         */
        void collectInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values) throws IOException {
            collectInto(counts, values, null);
        }

        /**
         * Like {@link #collectInto(AbstractHyperLogLogPlusPlus, SortedSetDocValues)} for {@code buckets} only, or every bucket
         * if {@code null}.
         *
         * @param buckets distinct buckets
         */
        void collectInto(AbstractHyperLogLogPlusPlus counts, SortedSetDocValues values, @Nullable long[] buckets) throws IOException {
            final long bucketCount = buckets == null ? maxBucket + 1 : buckets.length;
            if (bucketCount <= 0) {
                return;
            }
            final FastRankRoaringBitmap visited = new FastRankRoaringBitmap();
            long singles = 0;
            for (long i = 0; i < bucketCount; i++) {
                final long bucket = buckets == null ? i : buckets[(int) i];
                final RoaringBitmap bitmap = get(bucket);
                if (bitmap != null) {
                    visited.or(bitmap);
                } else if (singleOrd(bucket) >= 0) {
                    singles++;
                }
            }
            if (singles > 0) {
                addSingles(visited, Math.toIntExact(singles), buckets, bucketCount);
            }
            final int visitedCount = visited.getCardinality();
            if (visitedCount == 0) {
                return;
            }
            final long maxOrd = values.getValueCount();
            final boolean byOrdinal = maxOrd <= 2L * visitedCount;
            final long visitedBytes = ramBytesUsed(visited);
            breaker.accept(visitedBytes);
            try (LongArray hashes = bigArrays.newLongArray(byOrdinal ? maxOrd : visitedCount, false)) {
                final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                final PeekableIntIterator ords = visited.getIntIterator();
                for (long i = 0; ords.hasNext(); i++) {
                    final int ord = ords.next();
                    final BytesRef value = values.lookupOrd(ord);
                    MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(byOrdinal ? ord : i, hash.h1);
                }
                final BucketHasher hasher = new BucketHasher(counts, hashes, byOrdinal ? null : visited);
                for (long i = 0; i < bucketCount; i++) {
                    final long bucket = buckets == null ? i : buckets[(int) i];
                    final RoaringBitmap bitmap = get(bucket);
                    hasher.bucket = bucket;
                    if (bitmap == null) {
                        final int single = singleOrd(bucket);
                        if (single >= 0) {
                            hasher.accept(single);
                        }
                    } else if (byOrdinal || bitmap.getCardinality() <= RANK_LOOKUP_MAX_VALUES) {
                        bitmap.forEach(hasher);
                    } else {
                        // A bucket's ordinals are a sorted subset of the visited ones, so the rank iterator only moves forward
                        final PeekableIntRankIterator ranks = visited.getIntRankIterator();
                        final PeekableIntIterator bucketOrds = bitmap.getIntIterator();
                        while (bucketOrds.hasNext()) {
                            ranks.advanceIfNeeded(bucketOrds.next());
                            counts.collect(bucket, hashes.get(ranks.peekNextRank() - 1));
                        }
                    }
                }
            } finally {
                breaker.accept(-visitedBytes);
            }
        }

        /**
         * Adds the inline ordinals to {@code visited} in one sorted batch, which is much cheaper than adding them one by one.
         */
        private void addSingles(FastRankRoaringBitmap visited, int singles, @Nullable long[] buckets, long bucketCount) {
            final long bytes = RamUsageEstimator.sizeOf(new int[0]) + (long) Integer.BYTES * singles;
            breaker.accept(bytes);
            try {
                final int[] ords = new int[singles];
                int n = 0;
                for (long i = 0; i < bucketCount; i++) {
                    final long bucket = buckets == null ? i : buckets[(int) i];
                    if (get(bucket) != null) {
                        continue;
                    }
                    final int single = singleOrd(bucket);
                    if (single >= 0) {
                        ords[n++] = single;
                    }
                }
                Arrays.sort(ords);
                visited.addN(ords, 0, n);
            } finally {
                breaker.accept(-bytes);
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
                Releasables.close(bitmaps, firstOrds);
            }
        }
    }

    /**
     * Adds the hash of each ordinal it is given to one bucket of the sketches, looking hashes up by ordinal, or by rank among
     * {@code visited} when it is set. Reused for every bucket so that no object is created per bucket.
     */
    private static final class BucketHasher implements org.roaringbitmap.IntConsumer {
        private final AbstractHyperLogLogPlusPlus counts;
        private final LongArray hashes;
        @Nullable
        private final FastRankRoaringBitmap visited;
        private long bucket;

        BucketHasher(AbstractHyperLogLogPlusPlus counts, LongArray hashes, @Nullable FastRankRoaringBitmap visited) {
            this.counts = counts;
            this.hashes = hashes;
            this.visited = visited;
        }

        @Override
        public void accept(int ord) {
            counts.collect(bucket, hashes.get(visited == null ? ord : visited.rank(ord) - 1));
        }
    }

    /**
     * The global ordinals of each bucket for a whole shard. Global ordinals are the same in every segment, so the sets keep
     * growing across segments and give the exact number of distinct values of each bucket, and a bucket's values only need
     * hashing once the parent picked it.
     * <p>
     * The ordinals go through the same stages as the hybrid collector, but they are kept rather than hashed when moving on: a
     * {@link BucketOrdinalBits} while it fits in {@code memoryThreshold}, or a share of it for sparse ordinals, then
     * {@link BucketOrdinalBitmaps}. Its segment collectors throw {@link MemoryLimitExceededException} once the bitmaps exceed
     * {@code memoryThreshold} too.
     *
     * @opensearch.internal
     */
    static final class DeferredOrdinals implements Releasable {
        private final ValuesSource.Bytes.WithOrdinals source;
        /** Any segment of the shard, to look the global ordinals up from. */
        private final LeafReaderContext lookupLeaf;
        private final BigArrays bigArrays;
        private final LongConsumer breaker;
        private final long memoryThreshold;
        /** The ordinals until they no longer fit in bits, then {@code null}. */
        @Nullable
        private BucketOrdinalBits bits;
        /** The ordinals once they no longer fit in bits, {@code null} until then. */
        @Nullable
        private BucketOrdinalBitmaps bitmaps;
        private SortedSetDocValues lookup;

        DeferredOrdinals(
            ValuesSource.Bytes.WithOrdinals source,
            LeafReaderContext lookupLeaf,
            long maxOrd,
            BigArrays bigArrays,
            LongConsumer breaker,
            long memoryThreshold
        ) {
            this.source = source;
            this.lookupLeaf = lookupLeaf;
            this.bigArrays = bigArrays;
            this.breaker = breaker;
            this.memoryThreshold = memoryThreshold;
            this.bits = new BucketOrdinalBits(bigArrays, maxOrd, memoryThreshold, true);
        }

        Collector leafCollector(LeafReaderContext ctx) throws IOException {
            final SortedSetDocValues globalOrds = source.globalOrdinalsValues(ctx);
            return new Collector() {
                @Override
                public void collect(int doc, long bucketOrd) throws IOException {
                    if (globalOrds.advanceExact(doc) == false) {
                        return;
                    }
                    int count = globalOrds.docValueCount();
                    long ord;
                    if (bits != null) {
                        final BitArray bucketBits = bits.bucket(bucketOrd);
                        if (bucketBits != null) {
                            while ((count-- > 0) && (ord = globalOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                                bits.addCounted(bucketOrd, bucketBits, ord);
                            }
                            return;
                        }
                        switchToBitmaps();
                    }
                    while ((count-- > 0) && (ord = globalOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                        bitmaps.add(bucketOrd, (int) ord);
                    }
                    if (bitmaps.memoryUsed() > memoryThreshold) {
                        throw MemoryLimitExceededException.INSTANCE;
                    }
                }

                @Override
                public void postCollect() {
                    if (bitmaps != null) {
                        bitmaps.flushBreaker();
                    }
                }

                @Override
                public void close() {
                    // the ordinals live for the whole shard
                }
            };
        }

        private void switchToBitmaps() {
            final BucketOrdinalBitmaps created = new BucketOrdinalBitmaps(bigArrays, breaker);
            boolean success = false;
            try {
                bits.copyTo(created);
                success = true;
            } finally {
                if (success == false) {
                    created.close();
                }
            }
            bitmaps = created;
            final BucketOrdinalBits copied = bits;
            bits = null;
            copied.close();
        }

        /**
         * Whether the ordinals moved from bits to bitmaps.
         */
        boolean usesBitmaps() {
            return bitmaps != null;
        }

        /**
         * Exact number of distinct values collected into {@code bucketOrd}.
         */
        long cardinality(long bucketOrd) {
            return bits != null ? bits.cardinality(bucketOrd) : bitmaps.cardinality(bucketOrd);
        }

        private SortedSetDocValues lookup() throws IOException {
            if (lookup == null) {
                lookup = source.globalOrdinalsValues(lookupLeaf);
            }
            return lookup;
        }

        /**
         * Hashes the values of {@code buckets} into the same buckets of {@code counts}, each distinct value once.
         *
         * @param buckets distinct buckets
         */
        void hashBucketsInto(AbstractHyperLogLogPlusPlus counts, long[] buckets) throws IOException {
            if (bits != null) {
                bits.collectInto(counts, lookup(), buckets);
            } else {
                bitmaps.collectInto(counts, lookup(), buckets);
            }
        }

        /**
         * Hashes the values of every bucket into {@code counts}, each distinct value once.
         */
        void hashAllInto(AbstractHyperLogLogPlusPlus counts) throws IOException {
            if (bits != null) {
                bits.collectInto(counts, lookup());
            } else {
                bitmaps.collectInto(counts, lookup());
            }
        }

        @Override
        public void close() {
            Releasables.close(bits, bitmaps);
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

        private final AbstractHyperLogLogPlusPlus counts;
        private final SortedSetDocValues values;
        private final BucketOrdinalBitmaps bitmaps;
        private final long memoryThreshold;

        RoaringOrdinalsCollector(
            AbstractHyperLogLogPlusPlus counts,
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
            int count = values.docValueCount();
            long ord;
            while ((count-- > 0) && (ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                bitmaps.add(bucketOrd, (int) ord);
            }
            if (bitmaps.memoryUsed() > memoryThreshold) {
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
