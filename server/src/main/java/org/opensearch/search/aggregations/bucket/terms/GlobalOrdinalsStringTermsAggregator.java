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

package org.opensearch.search.aggregations.bucket.terms;

import joptsimple.internal.Strings;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.LongHash;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.bucket.terms.SignificanceLookup.BackgroundFrequencyForBytes;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.MatchAllFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;

import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;
import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_DOCS;

/**
 * An aggregator of string values that relies on global ordinals in order to build buckets.
 *
 * @opensearch.internal
 */
public class GlobalOrdinalsStringTermsAggregator extends AbstractStringTermsAggregator implements StarTreePreComputeCollector {
    protected final ResultStrategy<?, ?, ?> resultStrategy;
    protected final ValuesSource.Bytes.WithOrdinals valuesSource;

    private final LongPredicate acceptedGlobalOrdinals;
    private final long valueCount;
    protected final String fieldName;
    private Weight weight;
    protected CollectionStrategy collectionStrategy;
    private final SetOnce<SortedSetDocValues> dvs = new SetOnce<>();
    protected int segmentsWithSingleValuedOrds = 0;
    protected int segmentsWithMultiValuedOrds = 0;
    protected CardinalityUpperBound cardinalityUpperBound;
    private String resultSelectionStrategy;

    /*
     * Segment-ordinal counting: with dense global ords, doc counts are kept per segment ordinal during collection and
     * mapped to global ordinals once per segment, instead of incrementing the global doc counts for every collected
     * document. Sub-aggregations, if any, still get every doc with its global ordinal. See canCountSegmentOrds.
     */
    private SegmentOrdCounts segmentOrdCounts;
    protected int segmentsWithSegmentOrdCounting = 0;
    /**
     * Set once the counts couldn't be sized within the breakers: from then on this aggregator, which serves one shard or
     * one concurrent-search slice, uses the original path.
     */
    private boolean segmentOrdCountingDisabledByBreaker = false;
    /** The flush reports new buckets to the bucket consumer in batches of this size, see {@link #checkMemoryForNewBuckets}. */
    private static final int NEW_BUCKETS_PER_CONSUMER_CALL = 1024;

    public GlobalOrdinalsStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<GlobalOrdinalsStringTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        IncludeExclude.OrdinalsFilter includeExclude,
        SearchContext context,
        Aggregator parent,
        boolean remapGlobalOrds,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.cardinalityUpperBound = cardinality;
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        final IndexReader reader = context.searcher().getIndexReader();
        final SortedSetDocValues values = !reader.leaves().isEmpty()
            ? valuesSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0))
            : DocValues.emptySortedSet();
        this.valueCount = values.getValueCount();
        this.acceptedGlobalOrdinals = includeExclude == null ? ALWAYS_TRUE : includeExclude.acceptedGlobalOrdinals(values)::get;
        if (remapGlobalOrds) {
            this.collectionStrategy = new RemapGlobalOrds(cardinality);
        } else {
            this.collectionStrategy = cardinality.map(estimate -> {
                if (estimate > 1) {
                    throw new AggregationExecutionException("Dense ords don't know how to collect from many buckets");
                }
                return new DenseGlobalOrds();
            });
        }
        this.fieldName = (valuesSource instanceof ValuesSource.Bytes.WithOrdinals.FieldData)
            ? ((ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource).getIndexFieldName()
            : null;
        this.resultSelectionStrategy = Strings.EMPTY;
    }

    String descriptCollectionStrategy() {
        return collectionStrategy.describe();
    }

    public void setWeight(Weight weight) {
        this.weight = weight;
    }

    SortedSetDocValues getGlobalOrds(LeafReaderContext ctx) throws IOException {
        return valuesSource.globalOrdinalsValues(ctx);
    }

    /**
     Read doc frequencies directly from indexed terms in the segment to skip iterating through individual documents
     @param ctx The LeafReaderContext to collect terms from
     @param ordCountConsumer A consumer to accept collected term frequencies
     @return A LeafBucketCollector implementation with collection termination, since collection is complete
     @throws IOException If an I/O error occurs during reading
     */
    boolean tryCollectFromTermFrequencies(LeafReaderContext ctx, BiConsumer<Long, Integer> ordCountConsumer) throws IOException {
        if (weight == null) {
            // Weight not assigned - cannot use this optimization
            return false;
        } else {
            if (weight.count(ctx) == 0) {
                // No documents matches top level query on this segment, we can skip the segment entirely
                return true;
            } else if (weight.count(ctx) != ctx.reader().maxDoc()) {
                // weight.count(ctx) == ctx.reader().maxDoc() implies there are no deleted documents and
                // top-level query matches all docs in the segment
                return false;
            }
        }

        Terms segmentTerms = ctx.reader().terms(this.fieldName);
        if (segmentTerms == null) {
            // Field is not indexed.
            return false;
        }

        long termCount = segmentTerms.size();
        if (termCount == -1 || termCount > context.termsAggregationMaxPrecomputeCardinality()) {
            return false;
        }

        NumericDocValues docCountValues = DocValues.getNumeric(ctx.reader(), DocCountFieldMapper.NAME);
        if (docCountValues.nextDoc() != NO_MORE_DOCS) {
            // This segment has at least one document with the _doc_count field.
            return false;
        }

        final TermsEnum segmentTermsEnum = segmentTerms.iterator();
        final LongUnaryOperator globalOrdsMapping = valuesSource.globalOrdinalsMapping(ctx);

        // Iterate over the ordinals in the segment, look for matches in the global ordinal,
        // and increment bucket count when segment ordinal is contained in global ordinals.
        for (long segmentOrd = 0; segmentOrd < termCount; segmentOrd++) {
            segmentTermsEnum.next();
            long globalOrd = globalOrdsMapping.applyAsLong(segmentOrd);
            if (acceptedGlobalOrdinals.test(globalOrd)) {
                ordCountConsumer.accept(globalOrd, segmentTermsEnum.docFreq());
            }
        }

        // Segment terms enum should be fully exhausted after iterating over all the ordinals
        assert segmentTermsEnum.next() == null : "Segment terms enum must be fully exhausted";
        return true;
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        // Add the previous segment's counts first, so buckets become non-empty in segment order, as in the original path.
        flushSegmentOrdCounts();
        if (tryStarTreePrecompute(ctx) == true) {
            return true;
        }
        if (collectionStrategy instanceof DenseGlobalOrds
            && this.resultStrategy instanceof StandardTermsResults
            && subAggregators.length == 0) {
            return tryCollectFromTermFrequencies(
                ctx,
                (ord, docCount) -> incrementBucketDocCount(collectionStrategy.globalOrdToBucketOrd(0, ord), docCount)
            );
        }
        return false;
    }

    protected boolean tryStarTreePrecompute(LeafReaderContext ctx) throws IOException {
        CompositeIndexFieldInfo supportedStarTree = StarTreeQueryHelper.getSupportedStarTree(this.context.getQueryShardContext());
        if (supportedStarTree != null) {
            StarTreeBucketCollector starTreeBucketCollector = getStarTreeBucketCollector(ctx, supportedStarTree, null);
            StarTreeQueryHelper.preComputeBucketsWithStarTree(starTreeBucketCollector);
            return true;
        }
        return false;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Adds the previous segment's counts to docCounts. This also zeroes every entry it read, so the reused arrays are
        // all zeros when the next segment starts.
        flushSegmentOrdCounts();
        if (canCountSegmentOrds(ctx)) {
            final LeafBucketCollector segmentOrdCollector = segmentOrdCountingCollector(ctx, sub);
            if (segmentOrdCollector != null) {
                return segmentOrdCollector;
            }
        }
        SortedSetDocValues globalOrds = this.getGlobalOrds(ctx);
        collectionStrategy.globalOrdsReady(globalOrds);
        SortedDocValues singleValues = DocValues.unwrapSingleton(globalOrds);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            if (acceptedGlobalOrdinals == ALWAYS_TRUE) {
                /*
                 * Optimize when there isn't a filter because that is very
                 * common and marginally faster.
                 */
                return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                    @Override
                    public void collect(int doc, long owningBucketOrd) throws IOException {
                        if (false == singleValues.advanceExact(doc)) {
                            return;
                        }
                        int globalOrd = singleValues.ordValue();
                        collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                    }

                    @Override
                    public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                        super.collect(stream, owningBucketOrd);
                    }

                    @Override
                    public void collectRange(int min, int max) throws IOException {
                        super.collectRange(min, max);
                    }
                });
            }
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int globalOrd = singleValues.ordValue();
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        return;
                    }
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                }

                @Override
                public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                    super.collect(stream, owningBucketOrd);
                }

                @Override
                public void collectRange(int min, int max) throws IOException {
                    super.collectRange(min, max);
                }
            });
        }
        segmentsWithMultiValuedOrds++;
        if (acceptedGlobalOrdinals == ALWAYS_TRUE) {
            /*
             * Optimize when there isn't a filter because that is very
             * common and marginally faster.
             */
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == globalOrds.advanceExact(doc)) {
                        return;
                    }
                    int count = globalOrds.docValueCount();
                    long globalOrd;
                    while ((count-- > 0) && (globalOrd = globalOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                        collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                    }
                }

                @Override
                public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                    super.collect(stream, owningBucketOrd);
                }

                @Override
                public void collectRange(int min, int max) throws IOException {
                    super.collectRange(min, max);
                }
            });
        }
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, globalOrds) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (false == globalOrds.advanceExact(doc)) {
                    return;
                }
                int count = globalOrds.docValueCount();
                long globalOrd;
                while ((count-- > 0) && (globalOrd = globalOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        continue;
                    }
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                }
            }

            @Override
            public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                super.collect(stream, owningBucketOrd);
            }

            @Override
            public void collectRange(int min, int max) throws IOException {
                super.collectRange(min, max);
            }
        });
    }

    @Override
    public List<DimensionFilter> getDimensionFilters() {
        return StarTreeQueryHelper.collectDimensionFilters(new MatchAllFilter(fieldName), subAggregators);
    }

    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parent
    ) throws IOException {
        StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(ctx, starTree);
        SortedSetStarTreeValuesIterator valuesIterator = (SortedSetStarTreeValuesIterator) starTreeValues.getDimensionValuesIterator(
            fieldName
        );
        SortedNumericStarTreeValuesIterator docCountsIterator = StarTreeQueryHelper.getDocCountsIterator(starTreeValues, starTree);

        /* For nested aggregations, we require the RemapGlobalOrdsStarTree strategy to properly
        handle global ordinal remapping. This check ensures we don't reinitialize the
        collectionStrategy again if it's already correctly set. */
        if (parent != null && !(collectionStrategy instanceof RemapGlobalOrdsStarTree)) {
            collectionStrategy.close();
            collectionStrategy = new RemapGlobalOrdsStarTree(this.cardinalityUpperBound);
            SortedSetDocValues globalOrds = valuesSource.globalOrdinalsValues(ctx);
            collectionStrategy.globalOrdsReady(globalOrds);
        }

        LongUnaryOperator globalOperator = valuesSource.globalOrdinalsMapping(ctx);
        return new StarTreeBucketCollector(
            starTreeValues,
            parent == null ? StarTreeQueryHelper.getStarTreeResult(starTreeValues, context, getDimensionFilters()) : null
        ) {
            @Override
            public void setSubCollectors() throws IOException {
                for (Aggregator aggregator : subAggregators) {
                    this.subCollectors.add(
                        ((StarTreePreComputeCollector) aggregator.unwrapAggregator()).getStarTreeBucketCollector(ctx, starTree, this)
                    );
                }
            }

            @Override
            public void collectStarTreeEntry(int starTreeEntry, long owningBucketOrd) throws IOException {
                if (valuesIterator.advanceExact(starTreeEntry) == false) {
                    return;
                }
                for (int i = 0, count = valuesIterator.entryValueCount(); i < count; i++) {
                    long dimensionValue = valuesIterator.value();
                    long ord = globalOperator.applyAsLong(dimensionValue);
                    if (docCountsIterator.advanceExact(starTreeEntry)) {
                        long metricValue = docCountsIterator.nextValue();
                        if (collectionStrategy instanceof RemapGlobalOrdsStarTree rangeSTGlobalOrds) {
                            rangeSTGlobalOrds.collectGlobalOrdsForStarTree(owningBucketOrd, starTreeEntry, ord, this, metricValue);
                        } else {
                            long bucketOrd = collectionStrategy.getOrAddBucketOrd(owningBucketOrd, ord);
                            collectStarTreeBucket(this, metricValue, bucketOrd, starTreeEntry);
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        flushSegmentOrdCounts();
        return resultStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    protected void doPostCollection() throws IOException {
        super.doPostCollection();
        flushSegmentOrdCounts();
        // Nothing is pending after the last flush, so release the counts before the buckets are built. If collection
        // resumes, for example with the next batch of a streaming request, getLeafCollector allocates them again.
        releaseSegmentOrdCounts();
    }

    /**
     * Whether this segment can be counted by segment ordinal: dense global ords (one owning bucket, and the bucket ordinal
     * is the global ordinal), plain {@code terms} results, no include/exclude, a mapping from segment to global
     * ordinals, and no {@code _doc_count} field in the segment, so every doc counts once.
     */
    private boolean canCountSegmentOrds(LeafReaderContext ctx) {
        return segmentOrdCountingDisabledByBreaker == false
            && collectionStrategy instanceof DenseGlobalOrds
            && resultStrategy instanceof StandardTermsResults
            && acceptedGlobalOrdinals == ALWAYS_TRUE
            && valuesSource.supportsGlobalOrdinalsMapping()
            && ctx.reader().getFieldInfos().fieldInfo(DocCountFieldMapper.NAME) == null;
    }

    /**
     * Returns a collector that counts segment ordinals, or {@code null} if the segment should use the original path:
     * when its ordinals already are the global ordinals and the query isn't known to collect many docs, or when the
     * counts don't fit in the request breaker.
     */
    private LeafBucketCollector segmentOrdCountingCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final SortedSetDocValues segmentOrds = valuesSource.ordinalsValues(ctx);
        final long segmentValueCount = segmentOrds.getValueCount();
        if (segmentValueCount == valueCount && collectsManyDocs(ctx, segmentValueCount) == false) {
            // The segment has every term, so its ordinals are the global ordinals, e.g. on a single-segment shard, and the
            // original path reads them without a mapping. Counting them separately still pays off when the query collects
            // many docs, but for a selective query it would mostly allocate counts for every global ordinal.
            return null;
        }
        // All that DenseGlobalOrds#globalOrdsReady does. The global ordinals doc values the original path reads aren't needed.
        grow(valueCount);
        if (segmentOrdCounts == null) {
            segmentOrdCounts = new SegmentOrdCounts();
        }
        final LongUnaryOperator mapping = valuesSource.globalOrdinalsMapping(ctx);
        if (segmentOrdCounts.startSegment(segmentValueCount, mapping) == false) {
            // From now on this aggregator uses the original path, which needs no extra memory.
            segmentOrdCountingDisabledByBreaker = true;
            releaseSegmentOrdCounts();
            return null;
        }
        segmentsWithSegmentOrdCounting++;
        final SortedDocValues singleValues = DocValues.unwrapSingleton(segmentOrds);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            if (sub == LeafBucketCollector.NO_OP_COLLECTOR) {
                return new SingleValuedSegmentOrdCollector(singleValues, segmentOrdCounts);
            }
            return new SingleValuedSegmentOrdCollectorWithSub(sub, singleValues, segmentOrdCounts, mapping);
        }
        segmentsWithMultiValuedOrds++;
        if (sub == LeafBucketCollector.NO_OP_COLLECTOR) {
            return new MultiValuedSegmentOrdCollector(segmentOrds, segmentOrdCounts);
        }
        return new MultiValuedSegmentOrdCollectorWithSub(sub, segmentOrds, segmentOrdCounts, mapping);
    }

    /**
     * Whether the query is known to match at least a quarter as many docs in the segment as it has ordinals. Below that,
     * allocating and scanning the counts costs about as much as counting segment ordinals saves.
     */
    private boolean collectsManyDocs(LeafReaderContext ctx, long segmentValueCount) throws IOException {
        if (weight == null) {
            return false;
        }
        final int count = weight.count(ctx); // -1 if it can't be computed cheaply
        return count >= 0 && count >= segmentValueCount / 4;
    }

    /**
     * Adds the pending segment's counts to the doc counts. Safe to call more than once.
     */
    private void flushSegmentOrdCounts() {
        if (segmentOrdCounts != null) {
            segmentOrdCounts.flush();
        }
    }

    private void releaseSegmentOrdCounts() {
        if (segmentOrdCounts != null) {
            segmentOrdCounts.close();
            segmentOrdCounts = null;
        }
    }

    /**
     * At least what BigArrays adds to the request breaker when it grows an array of {@code currentSize} elements (0: no
     * array yet) to {@code newSize}: the added pages when it grows a paged array in place, otherwise the whole new array,
     * which it charges before releasing the old one, in whole pages plus the wrapper of an array that fits on one page.
     */
    private static long growthBytes(long currentSize, long newSize, int pageSize, int bytesPerElement) {
        final long pageBytes = (long) pageSize * bytesPerElement;
        final long newPages = (BigArrays.overSize(newSize, pageSize, bytesPerElement) + pageSize - 1) / pageSize;
        if (currentSize > pageSize) {
            return (newPages - (currentSize + pageSize - 1) / pageSize) * pageBytes;
        }
        return Math.max(newPages, 1) * pageBytes + 256; // 256: more than the wrapper object and array header
    }

    /**
     * Doc counts by segment ordinal for the segment being collected, reused across segments and sized to the largest
     * segment seen so far. {@code touched} has a bit for every ordinal with a non-zero count, so a flush costs
     * O(segment value count / 64 + distinct ordinals) and resets exactly the entries it reads. Both arrays come from
     * {@link BigArrays}, so they are paged, recycled and charged to the request breaker.
     */
    private final class SegmentOrdCounts implements Releasable {
        private IntArray counts;
        private LongArray touched;
        private long segmentWords;
        /** Maps the pending segment's ordinals to global ordinals; {@code null} when nothing is pending. */
        private LongUnaryOperator mapping;

        /**
         * Sizes the arrays for a segment. Returns {@code false} if they can't be grown within the breakers. They may then
         * be partly grown, and the caller releases them.
         */
        boolean startSegment(long segmentValueCount, LongUnaryOperator mapping) {
            assert this.mapping == null : "the previous segment wasn't flushed";
            final long words = (segmentValueCount + 63) >>> 6;
            final long countsSize = counts == null ? 0 : counts.size();
            final long touchedSize = touched == null ? 0 : touched.size();
            final boolean growCounts = counts == null || countsSize < segmentValueCount;
            final boolean growTouched = touched == null || touchedSize < words;
            final long bytes = (growCounts ? growthBytes(countsSize, segmentValueCount, PageCacheRecycler.INT_PAGE_SIZE, Integer.BYTES) : 0)
                + (growTouched ? growthBytes(touchedSize, words, PageCacheRecycler.LONG_PAGE_SIZE, Long.BYTES) : 0);
            if (bytes > 0 && wouldTripRequestBreaker(bytes)) {
                return false;
            }
            try {
                if (growCounts) {
                    counts = counts == null
                        ? context.bigArrays().newIntArray(segmentValueCount, true)
                        : context.bigArrays().grow(counts, segmentValueCount);
                }
                if (growTouched) {
                    touched = touched == null ? context.bigArrays().newLongArray(words, true) : context.bigArrays().grow(touched, words);
                }
            } catch (CircuitBreakingException e) {
                // Backstop for the check above, e.g. when the parent breaker trips or another slice used the headroom. The
                // caller releases whatever was allocated.
                return false;
            }
            this.segmentWords = words;
            this.mapping = mapping;
            return true;
        }

        /**
         * Whether adding {@code bytes} could trip the request breaker, by the breaker's own formula. Checked before
         * allocating, so that falling back doesn't count as a tripped breaker, except when the parent breaker or another
         * thread trips it first.
         */
        private boolean wouldTripRequestBreaker(long bytes) {
            final CircuitBreakerService breakerService = context.bigArrays().breakerService();
            if (breakerService == null) {
                return false;
            }
            final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
            final long limit = breaker.getLimit();
            if (limit < 0) {
                return false; // no limit
            }
            return (long) ((breaker.getUsed() + bytes) * breaker.getOverhead()) > limit;
        }

        void count(long ord) {
            if (counts.increment(ord, 1) == 1) {
                final long word = ord >>> 6;
                touched.set(word, touched.get(word) | (1L << (ord & 63)));
            }
        }

        /**
         * Adds the pending segment's counts to the doc counts and resets every entry it reads. No-op if nothing is pending.
         */
        void flush() {
            final LongUnaryOperator mapping = this.mapping;
            if (mapping == null) {
                return;
            }
            this.mapping = null;
            final LongArray docCounts = GlobalOrdinalsStringTermsAggregator.this.docCounts;
            boolean flushed = false;
            try {
                int newBuckets = 0;
                for (long word = 0; word < segmentWords; word++) {
                    long bits = touched.get(word);
                    if (bits == 0) {
                        continue;
                    }
                    touched.set(word, 0);
                    do {
                        final long ord = (word << 6) | Long.numberOfTrailingZeros(bits);
                        bits &= bits - 1;
                        final int count = counts.set(ord, 0);
                        assert count > 0 : "touched ordinal " + ord + " has no count";
                        // Dense global ords: the bucket ordinal is the global ordinal, and docCounts has a slot for every one.
                        if (docCounts.increment(mapping.applyAsLong(ord), count) == count) {
                            if (++newBuckets == NEW_BUCKETS_PER_CONSUMER_CALL) {
                                checkMemoryForNewBuckets(newBuckets);
                                newBuckets = 0;
                            }
                        }
                    } while (bits != 0);
                }
                if (newBuckets > 0) {
                    checkMemoryForNewBuckets(newBuckets);
                }
                flushed = true;
            } finally {
                if (flushed == false) {
                    // The bucket consumer threw, e.g. the parent breaker tripped. Drop the half-reset arrays so that
                    // nothing can count them again; the request fails anyway.
                    close();
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(counts, touched);
            counts = null;
            touched = null;
            mapping = null;
        }
    }

    private static final class SingleValuedSegmentOrdCollector extends LeafBucketCollectorBase {
        private final SortedDocValues ords;
        private final SegmentOrdCounts counts;

        SingleValuedSegmentOrdCollector(SortedDocValues ords, SegmentOrdCounts counts) {
            super(LeafBucketCollector.NO_OP_COLLECTOR, ords);
            this.ords = ords;
            this.counts = counts;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            assert owningBucketOrd == 0;
            if (ords.advanceExact(doc)) {
                counts.count(ords.ordValue());
            }
        }

        @Override
        public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
            super.collect(stream, owningBucketOrd);
        }

        @Override
        public void collectRange(int min, int max) throws IOException {
            super.collectRange(min, max);
        }
    }

    private static final class MultiValuedSegmentOrdCollector extends LeafBucketCollectorBase {
        private final SortedSetDocValues ords;
        private final SegmentOrdCounts counts;

        MultiValuedSegmentOrdCollector(SortedSetDocValues ords, SegmentOrdCounts counts) {
            super(LeafBucketCollector.NO_OP_COLLECTOR, ords);
            this.ords = ords;
            this.counts = counts;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            assert owningBucketOrd == 0;
            if (ords.advanceExact(doc)) {
                // Read the count once: for some field data the count decreases as ordinals are read.
                for (int i = ords.docValueCount(); i > 0; i--) {
                    counts.count(ords.nextOrd());
                }
            }
        }

        @Override
        public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
            super.collect(stream, owningBucketOrd);
        }

        @Override
        public void collectRange(int min, int max) throws IOException {
            super.collectRange(min, max);
        }
    }

    /**
     * Like {@link SingleValuedSegmentOrdCollector}, and passes every doc to the sub-aggregations with its global
     * ordinal, which is the bucket ordinal for dense global ords.
     */
    private static final class SingleValuedSegmentOrdCollectorWithSub extends LeafBucketCollectorBase {
        private final LeafBucketCollector sub;
        private final SortedDocValues ords;
        private final SegmentOrdCounts counts;
        private final LongUnaryOperator mapping;

        SingleValuedSegmentOrdCollectorWithSub(
            LeafBucketCollector sub,
            SortedDocValues ords,
            SegmentOrdCounts counts,
            LongUnaryOperator mapping
        ) {
            super(sub, ords);
            this.sub = sub;
            this.ords = ords;
            this.counts = counts;
            this.mapping = mapping;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            assert owningBucketOrd == 0;
            if (ords.advanceExact(doc)) {
                final int ord = ords.ordValue();
                counts.count(ord);
                sub.collect(doc, mapping.applyAsLong(ord));
            }
        }

        @Override
        public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
            super.collect(stream, owningBucketOrd);
        }

        @Override
        public void collectRange(int min, int max) throws IOException {
            super.collectRange(min, max);
        }
    }

    /**
     * Like {@link MultiValuedSegmentOrdCollector}, and passes every doc to the sub-aggregations with each of its global
     * ordinals, which are the bucket ordinals for dense global ords.
     */
    private static final class MultiValuedSegmentOrdCollectorWithSub extends LeafBucketCollectorBase {
        private final LeafBucketCollector sub;
        private final SortedSetDocValues ords;
        private final SegmentOrdCounts counts;
        private final LongUnaryOperator mapping;

        MultiValuedSegmentOrdCollectorWithSub(
            LeafBucketCollector sub,
            SortedSetDocValues ords,
            SegmentOrdCounts counts,
            LongUnaryOperator mapping
        ) {
            super(sub, ords);
            this.sub = sub;
            this.ords = ords;
            this.counts = counts;
            this.mapping = mapping;
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            assert owningBucketOrd == 0;
            if (ords.advanceExact(doc)) {
                for (int i = ords.docValueCount(); i > 0; i--) {
                    final long ord = ords.nextOrd();
                    counts.count(ord);
                    sub.collect(doc, mapping.applyAsLong(ord));
                }
            }
        }

        @Override
        public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
            super.collect(stream, owningBucketOrd);
        }

        @Override
        public void collectRange(int min, int max) throws IOException {
            super.collectRange(min, max);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("collection_strategy", collectionStrategy.describe());
        collectionStrategy.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("segments_with_single_valued_ords", segmentsWithSingleValuedOrds);
        add.accept("segments_with_multi_valued_ords", segmentsWithMultiValuedOrds);
        add.accept("has_filter", acceptedGlobalOrdinals != ALWAYS_TRUE);
        add.accept("result_selection_strategy", resultSelectionStrategy);
        add.accept("segments_with_segment_ord_counting", segmentsWithSegmentOrdCounting);
        add.accept("segment_ord_counting_disabled_by_breaker", segmentOrdCountingDisabledByBreaker);
    }

    public String getResultSelectionStrategy() {
        return resultSelectionStrategy;
    }

    /**
     * This is used internally only, just for compare using global ordinal instead of term bytes in the PQ
     *
     * @opensearch.internal
     */
    static class OrdBucket extends InternalTerms.Bucket<OrdBucket> {
        long globalOrd;

        OrdBucket(boolean showDocCountError, DocValueFormat format) {
            super(0, null, showDocCountError, 0, format);
        }

        @Override
        public int compareKey(OrdBucket other) {
            return Long.compare(globalOrd, other.globalOrd);
        }

        @Override
        public String getKeyAsString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Number getKeyAsNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(resultStrategy, collectionStrategy, segmentOrdCounts);
    }

    /**
     * Variant of {@link GlobalOrdinalsStringTermsAggregator} that
     * resolves global ordinals post segment collection instead of on the fly
     * for each match.This is beneficial for low cardinality fields, because
     * it can reduce the amount of look-ups significantly.
     * <p>
     * This is only supported for the standard {@code terms} aggregation and
     * doesn't support {@code significant_terms} so this forces
     * {@link StandardTermsResults}.
     *
     * @opensearch.internal
     */
    static class LowCardinality extends GlobalOrdinalsStringTermsAggregator {

        private LongUnaryOperator mapping;
        private LongArray segmentDocCounts;

        LowCardinality(
            String name,
            AggregatorFactories factories,
            Function<GlobalOrdinalsStringTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
            ValuesSource.Bytes.WithOrdinals valuesSource,
            BucketOrder order,
            DocValueFormat format,
            BucketCountThresholds bucketCountThresholds,
            SearchContext context,
            Aggregator parent,
            boolean remapGlobalOrds,
            SubAggCollectionMode collectionMode,
            boolean showTermDocCountError,
            Map<String, Object> metadata
        ) throws IOException {
            super(
                name,
                factories,
                resultStrategy,
                valuesSource,
                order,
                format,
                bucketCountThresholds,
                null,
                context,
                parent,
                remapGlobalOrds,
                collectionMode,
                showTermDocCountError,
                CardinalityUpperBound.ONE,
                metadata
            );
            assert factories == null || factories.countAggregators() == 0;
            this.segmentDocCounts = context.bigArrays().newLongArray(1, true);
        }

        @Override
        protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
            if (subAggregators.length == 0) {
                return tryCollectFromTermFrequencies(
                    ctx,
                    (globalOrd, docCount) -> incrementBucketDocCount(collectionStrategy.globalOrdToBucketOrd(0, globalOrd), docCount)
                );
            }
            return tryStarTreePrecompute(ctx);
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
            if (mapping != null) {
                mapSegmentCountsToGlobalCounts(mapping);
            }
            final SortedSetDocValues segmentOrds = this.getGlobalOrds(ctx);
            segmentDocCounts = context.bigArrays().grow(segmentDocCounts, 1 + segmentOrds.getValueCount());
            assert sub == LeafBucketCollector.NO_OP_COLLECTOR;
            mapping = valuesSource.globalOrdinalsMapping(ctx);
            segmentsWithSegmentOrdCounting++;

            final SortedDocValues singleValues = DocValues.unwrapSingleton(segmentOrds);
            if (singleValues != null) {
                segmentsWithSingleValuedOrds++;
                return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, segmentOrds) {
                    @Override
                    public void collect(int doc, long owningBucketOrd) throws IOException {
                        assert owningBucketOrd == 0;
                        if (false == singleValues.advanceExact(doc)) {
                            return;
                        }
                        int ord = singleValues.ordValue();
                        long docCount = docCountProvider.getDocCount(doc);
                        segmentDocCounts.increment(ord + 1, docCount);
                    }

                    @Override
                    public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                        super.collect(stream, owningBucketOrd);
                    }

                    @Override
                    public void collectRange(int min, int max) throws IOException {
                        super.collectRange(min, max);
                    }
                });
            }
            segmentsWithMultiValuedOrds++;
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, segmentOrds) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    assert owningBucketOrd == 0;
                    if (false == segmentOrds.advanceExact(doc)) {
                        return;
                    }
                    int count = segmentOrds.docValueCount();
                    long segmentOrd;
                    while ((count-- > 0) && (segmentOrd = segmentOrds.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                        long docCount = docCountProvider.getDocCount(doc);
                        segmentDocCounts.increment(segmentOrd + 1, docCount);
                    }
                }

                @Override
                public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                    super.collect(stream, owningBucketOrd);
                }

                @Override
                public void collectRange(int min, int max) throws IOException {
                    super.collectRange(min, max);
                }
            });
        }

        @Override
        protected void doPostCollection() throws IOException {
            if (mapping != null) {
                mapSegmentCountsToGlobalCounts(mapping);
                mapping = null;
            }
        }

        @Override
        protected void doClose() {
            Releasables.close(resultStrategy, segmentDocCounts, collectionStrategy);
        }

        private void mapSegmentCountsToGlobalCounts(LongUnaryOperator mapping) throws IOException {
            for (long i = 1; i < segmentDocCounts.size(); i++) {
                // We use set(...) here, because we need to reset the slow to 0.
                // segmentDocCounts get reused over the segments and otherwise counts would be too high.
                long inc = segmentDocCounts.set(i, 0);
                if (inc == 0) {
                    continue;
                }
                long ord = i - 1; // remember we do +1 when counting
                long globalOrd = mapping.applyAsLong(ord);
                incrementBucketDocCount(collectionStrategy.globalOrdToBucketOrd(0, globalOrd), inc);
            }
        }

        @Override
        SortedSetDocValues getGlobalOrds(LeafReaderContext ctx) throws IOException {
            return valuesSource.ordinalsValues(ctx);
        }
    }

    /**
     * Strategy for collecting global ordinals.
     * <p>
     * The {@link GlobalOrdinalsStringTermsAggregator} uses one of these
     * to collect the global ordinals by calling
     * {@link CollectionStrategy#collectGlobalOrd} for each global ordinal
     * that it hits and then calling {@link CollectionStrategy#forEach}
     * once to iterate on the results.
     */
    abstract class CollectionStrategy implements Releasable {
        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * Collect debug information to add to the profiling results. This will
         * only be called if the aggregation is being profiled.
         */
        abstract void collectDebugInfo(BiConsumer<String, Object> add);

        /**
         * Called when the global ordinals are ready.
         */
        abstract void globalOrdsReady(SortedSetDocValues globalOrds);

        /**
         * Called once per unique document, global ordinal combination to
         * collect the bucket.
         *
         * @param owningBucketOrd the ordinal of the bucket that owns this collection
         * @param doc the doc id in to collect
         * @param globalOrd the global ordinal to collect
         * @param sub the sub-aggregators that will collect the bucket data
         */
        abstract void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector sub) throws IOException;

        /**
         * Convert a global ordinal into a bucket ordinal.
         */
        abstract long globalOrdToBucketOrd(long owningBucketOrd, long globalOrd);

        /**
         * Iterate all the buckets. Implementations take into account
         * the {@link BucketCountThresholds}. In particular,
         * if the {@link BucketCountThresholds#getMinDocCount()} is 0 then
         * they'll make sure to iterate a bucket even if it was never
         * {{@link #collectGlobalOrd collected}.
         * If {@link BucketCountThresholds#getMinDocCount()} is not 0 then
         * they'll skip all global ords that weren't collected.
         */
        abstract void forEach(long owningBucketOrd, BucketInfoConsumer consumer) throws IOException;

        /**
         * Add a global ordinal if it hasn't been seen before.
         * Convert the global ordinal into a bucket ordinal.
         */
        abstract long getOrAddBucketOrd(long owningBucketOrd, long globalOrd) throws IOException;
    }

    interface BucketInfoConsumer {
        void accept(long globalOrd, long bucketOrd, long docCount) throws IOException;
    }

    /**
     * {@linkplain CollectionStrategy} that just uses the global ordinal as the
     * bucket ordinal.
     */
    class DenseGlobalOrds extends CollectionStrategy {
        @Override
        String describe() {
            return "dense";
        }

        @Override
        void collectDebugInfo(BiConsumer<String, Object> add) {}

        @Override
        void globalOrdsReady(SortedSetDocValues globalOrds) {
            grow(globalOrds.getValueCount());
        }

        @Override
        void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector sub) throws IOException {
            assert owningBucketOrd == 0;
            collectExistingBucket(sub, doc, globalOrd);
        }

        @Override
        long globalOrdToBucketOrd(long owningBucketOrd, long globalOrd) {
            assert owningBucketOrd == 0;
            return globalOrd;
        }

        @Override
        void forEach(long owningBucketOrd, BucketInfoConsumer consumer) throws IOException {
            assert owningBucketOrd == 0;
            for (long globalOrd = 0; globalOrd < valueCount; globalOrd++) {
                if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                    continue;
                }
                long docCount = bucketDocCount(globalOrd);
                if (bucketCountThresholds.getMinDocCount() == 0 || docCount > 0) {
                    consumer.accept(globalOrd, globalOrd, docCount);
                }
            }
        }

        @Override
        long getOrAddBucketOrd(long owningBucketOrd, long globalOrd) {
            return globalOrdToBucketOrd(owningBucketOrd, globalOrd);
        }

        @Override
        public void close() {}
    }

    /**
     * {@linkplain CollectionStrategy} that uses a {@link LongHash} to map the
     * global ordinal into bucket ordinals. This uses more memory than
     * {@link DenseGlobalOrds} when collecting every ordinal, but significantly
     * less when collecting only a few.
     */
    private class RemapGlobalOrds extends CollectionStrategy {
        protected final LongKeyedBucketOrds bucketOrds;

        private RemapGlobalOrds(CardinalityUpperBound cardinality) {
            bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
        }

        @Override
        String describe() {
            return "remap";
        }

        @Override
        void collectDebugInfo(BiConsumer<String, Object> add) {
            add.accept("total_buckets", bucketOrds.size());
        }

        @Override
        void globalOrdsReady(SortedSetDocValues globalOrds) {}

        @Override
        void collectGlobalOrd(long owningBucketOrd, int doc, long globalOrd, LeafBucketCollector sub) throws IOException {
            long bucketOrd = bucketOrds.add(owningBucketOrd, globalOrd);
            if (bucketOrd < 0) {
                bucketOrd = -1 - bucketOrd;
                collectExistingBucket(sub, doc, bucketOrd);
            } else {
                collectBucket(sub, doc, bucketOrd);
            }
        }

        @Override
        long globalOrdToBucketOrd(long owningBucketOrd, long globalOrd) {
            return bucketOrds.find(owningBucketOrd, globalOrd);
        }

        @Override
        void forEach(long owningBucketOrd, BucketInfoConsumer consumer) throws IOException {
            if (bucketCountThresholds.getMinDocCount() == 0) {
                for (long globalOrd = 0; globalOrd < valueCount; globalOrd++) {
                    if (false == acceptedGlobalOrdinals.test(globalOrd)) {
                        continue;
                    }
                    /*
                     * Use `add` instead of `find` here to assign an ordinal
                     * even if the global ord wasn't found so we can build
                     * sub-aggregations without trouble even though we haven't
                     * hit any documents for them. This is wasteful, but
                     * settings minDocCount == 0 is wasteful in general.....
                     */
                    long bucketOrd = bucketOrds.add(owningBucketOrd, globalOrd);
                    long docCount;
                    if (bucketOrd < 0) {
                        bucketOrd = -1 - bucketOrd;
                        docCount = bucketDocCount(bucketOrd);
                    } else {
                        docCount = 0;
                    }
                    consumer.accept(globalOrd, bucketOrd, docCount);
                }
            } else {
                LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
                while (ordsEnum.next()) {
                    if (false == acceptedGlobalOrdinals.test(ordsEnum.value())) {
                        continue;
                    }
                    consumer.accept(ordsEnum.value(), ordsEnum.ord(), bucketDocCount(ordsEnum.ord()));
                }
            }
        }

        @Override
        long getOrAddBucketOrd(long owningBucketOrd, long globalOrd) {
            return bucketOrds.add(owningBucketOrd, globalOrd);
        }

        @Override
        public void close() {
            bucketOrds.close();
        }
    }

    private class RemapGlobalOrdsStarTree extends RemapGlobalOrds {
        private RemapGlobalOrdsStarTree(CardinalityUpperBound cardinality) {
            super(cardinality);
        }

        @Override
        String describe() {
            return "remapStarTree";
        }

        void collectGlobalOrdsForStarTree(
            long owningBucketOrd,
            int starTreeEntry,
            long globalOrd,
            StarTreeBucketCollector collector,
            long docCount
        ) throws IOException {
            long bucketOrd = bucketOrds.add(owningBucketOrd, globalOrd);
            collectStarTreeBucket(collector, docCount, bucketOrd, starTreeEntry);
        }
    }

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<
        R extends InternalAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket,
        TB extends InternalMultiBucketAggregation.InternalBucket> implements Releasable {

        private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(bucketCountThresholds);
            if (valueCount == 0) { // no context in this reader
                InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
                for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                    results[ordIdx] = buildNoValuesResult(owningBucketOrds[ordIdx]);
                }
                return results;
            }

            B[][] topBucketsPerOwningOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCount = new long[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                // processing each owning bucket
                checkCancelled();
                final int size;
                if (localBucketCountThresholds.getMinDocCount() == 0) {
                    // if minDocCount == 0 then we can end up with more buckets then maxBucketOrd() returns
                    size = (int) Math.min(valueCount, localBucketCountThresholds.getRequiredSize());
                } else {
                    size = (int) Math.min(maxBucketOrd(), localBucketCountThresholds.getRequiredSize());
                }

                // partiallyBuiltBucketComparator is null for significantTerm Aggregations use case and the way buckets sorted in the
                // priority queue is based on the significanceScore
                BucketSelectionStrategy strategy = BucketSelectionStrategy.determine(
                    size,
                    valueCount,
                    order,
                    partiallyBuiltBucketComparator,
                    context.bucketSelectionStrategyFactor()
                );

                if (strategy.equals(BucketSelectionStrategy.PRIORITY_QUEUE)) {
                    PriorityQueue<TB> ordered = buildPriorityQueue(size);
                    final int finalOrdIdx = ordIdx;
                    BucketUpdater<TB> updater = bucketUpdater(owningBucketOrds[ordIdx]);
                    // for each provides the bucket ord and key value for the owning bucket
                    collectionStrategy.forEach(owningBucketOrds[ordIdx], new BucketInfoConsumer() {
                        TB spare = null;

                        @Override
                        public void accept(long globalOrd, long bucketOrd, long docCount) throws IOException {
                            otherDocCount[finalOrdIdx] += docCount;
                            if (docCount >= localBucketCountThresholds.getMinDocCount()) {
                                if (spare == null) {
                                    spare = buildEmptyTemporaryBucket();
                                }
                                updater.updateBucket(spare, globalOrd, bucketOrd, docCount);
                                spare = ordered.insertWithOverflow(spare);
                            }
                        }
                    });

                    // Get the top buckets
                    topBucketsPerOwningOrd[ordIdx] = buildBuckets(ordered.size());
                    if (isKeyOrder(order)) {
                        for (int i = ordered.size() - 1; i >= 0; --i) {
                            topBucketsPerOwningOrd[ordIdx][i] = convertTempBucketToRealBucket(ordered.pop());
                            otherDocCount[ordIdx] -= topBucketsPerOwningOrd[ordIdx][i].getDocCount();
                        }
                    } else {
                        // sorted buckets not needed as they will be sorted by key in buildResult() which is different from
                        // order in priority queue ordered
                        Iterator<TB> itr = ordered.iterator();
                        for (int i = ordered.size() - 1; i >= 0; --i) {
                            topBucketsPerOwningOrd[ordIdx][i] = convertTempBucketToRealBucket(itr.next());
                            otherDocCount[ordIdx] -= topBucketsPerOwningOrd[ordIdx][i].getDocCount();
                        }
                    }
                    resultSelectionStrategy = "priority_queue";
                } else {
                    final int finalOrdIdx = ordIdx;
                    Object[] bucketsForOrdArr = new Object[(int) valueCount];
                    int[] tot = { 0 };
                    BucketUpdater<TB> updater = bucketUpdater(owningBucketOrds[ordIdx]);
                    collectionStrategy.forEach(owningBucketOrds[ordIdx], new BucketInfoConsumer() {
                        TB spare = null;

                        @Override
                        public void accept(long globalOrd, long bucketOrd, long docCount) throws IOException {
                            otherDocCount[finalOrdIdx] += docCount;
                            if (docCount >= localBucketCountThresholds.getMinDocCount()) {
                                spare = buildEmptyTemporaryBucket();
                                updater.updateBucket(spare, globalOrd, bucketOrd, docCount);
                                bucketsForOrdArr[tot[0]++] = spare;
                            }
                        }
                    });
                    // If total collected buckets greater than request size, use quickSelect to get topN buckets
                    if (tot[0] > size) {
                        ArrayUtil.select(
                            bucketsForOrdArr,
                            0,
                            tot[0],
                            size,
                            ((b1, b2) -> partiallyBuiltBucketComparator.compare((InternalTerms.Bucket<?>) b1, (InternalTerms.Bucket<?>) b2))
                        );
                        // Get the top buckets and update the otherDocCount
                        topBucketsPerOwningOrd[ordIdx] = buildBuckets(size);
                        for (int i = 0; i < size; i++) {
                            topBucketsPerOwningOrd[ordIdx][i] = convertTempBucketToRealBucket((TB) bucketsForOrdArr[i]);
                            otherDocCount[ordIdx] -= topBucketsPerOwningOrd[ordIdx][i].getDocCount();
                        }
                        resultSelectionStrategy = "quick_select";
                    } else {
                        // All buckets fit within the required size, no selection needed
                        topBucketsPerOwningOrd[ordIdx] = buildBuckets(tot[0]);
                        for (int i = 0; i < tot[0]; i++) {
                            topBucketsPerOwningOrd[ordIdx][i] = convertTempBucketToRealBucket((TB) bucketsForOrdArr[i]);
                        }
                        otherDocCount[ordIdx] = 0;
                        resultSelectionStrategy = "select_all";
                    }
                }
            }

            buildSubAggs(topBucketsPerOwningOrd);

            InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                results[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCount[ordIdx], topBucketsPerOwningOrd[ordIdx]);
            }
            return results;
        }

        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * Wrap the "standard" numeric terms collector to collect any more
         * information that this result type may need.
         */
        abstract LeafBucketCollector wrapCollector(LeafBucketCollector primary);

        /**
         * Build an empty temporary bucket.
         */
        abstract TB buildEmptyTemporaryBucket();

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract BucketUpdater<TB> bucketUpdater(long owningBucketOrd) throws IOException;

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract PriorityQueue<TB> buildPriorityQueue(int size);

        /**
         * Build an array to hold the "top" buckets for each ordinal.
         */
        abstract B[][] buildTopBucketsPerOrd(int size);

        /**
         * Build an array of buckets for a particular ordinal to collect the
         * results. The populated list is passed to {@link #buildResult}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Convert a temporary bucket into a real bucket.
         */
        abstract B convertTempBucketToRealBucket(TB temp) throws IOException;

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[][] topBucketsPreOrd) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(long owningBucketOrd, long otherDocCount, B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();

        /**
         * Build an "empty" result for a particular bucket ordinal. Called when
         * there aren't any values for the field on this shard.
         */
        abstract R buildNoValuesResult(long owningBucketOrdinal);
    }

    interface BucketUpdater<TB extends InternalMultiBucketAggregation.InternalBucket> {
        void updateBucket(TB spare, long globalOrd, long bucketOrd, long docCount) throws IOException;
    }

    /**
     * Builds results for the standard {@code terms} aggregation.
     */
    class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket, OrdBucket> {
        @Override
        String describe() {
            return "terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        StringTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new StringTerms.Bucket[size][];
        }

        @Override
        StringTerms.Bucket[] buildBuckets(int size) {
            return new StringTerms.Bucket[size];
        }

        @Override
        OrdBucket buildEmptyTemporaryBucket() {
            return new OrdBucket(showTermDocCountError, format);
        }

        @Override
        BucketUpdater<OrdBucket> bucketUpdater(long owningBucketOrd) throws IOException {
            return (spare, globalOrd, bucketOrd, docCount) -> {
                spare.globalOrd = globalOrd;
                spare.bucketOrd = bucketOrd;
                spare.docCount = docCount;
            };
        }

        @Override
        PriorityQueue<OrdBucket> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        }

        StringTerms.Bucket convertTempBucketToRealBucket(OrdBucket temp) throws IOException {
            // Recreate DocValues as needed for concurrent segment search
            SortedSetDocValues values = getDocValues();
            BytesRef term = BytesRef.deepCopyOf(values.lookupOrd(temp.globalOrd));

            StringTerms.Bucket result = new StringTerms.Bucket(term, temp.docCount, null, showTermDocCountError, 0, format);
            result.bucketOrd = temp.bucketOrd;
            result.docCountError = 0;
            return result;
        }

        @Override
        void buildSubAggs(StringTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        StringTerms buildResult(long owningBucketOrd, long otherDocCount, StringTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new StringTerms(
                name,
                reduceOrder,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                Arrays.asList(topBuckets),
                0,
                bucketCountThresholds
            );
        }

        @Override
        StringTerms buildEmptyResult() {
            return buildEmptyTermsAggregation();
        }

        @Override
        StringTerms buildNoValuesResult(long owningBucketOrdinal) {
            return buildEmptyResult();
        }

        @Override
        public void close() {}
    }

    /**
     * Builds results for the {@code significant_terms} aggregation.
     */
    class SignificantTermsResults extends ResultStrategy<
        SignificantStringTerms,
        SignificantStringTerms.Bucket,
        SignificantStringTerms.Bucket> {

        private final BackgroundFrequencyForBytes backgroundFrequencies;
        private final long supersetSize;
        private final SignificanceHeuristic significanceHeuristic;

        private LongArray subsetSizes = context.bigArrays().newLongArray(1, true);

        SignificantTermsResults(
            SignificanceLookup significanceLookup,
            SignificanceHeuristic significanceHeuristic,
            CardinalityUpperBound cardinality
        ) {
            backgroundFrequencies = significanceLookup.bytesLookup(context.bigArrays(), cardinality);
            supersetSize = significanceLookup.supersetSize();
            this.significanceHeuristic = significanceHeuristic;
        }

        @Override
        String describe() {
            return "significant_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return new LeafBucketCollectorBase(primary, null) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    super.collect(doc, owningBucketOrd);
                    subsetSizes = context.bigArrays().grow(subsetSizes, owningBucketOrd + 1);
                    subsetSizes.increment(owningBucketOrd, 1);
                }

                @Override
                public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
                    super.collect(stream, owningBucketOrd);
                }

                @Override
                public void collectRange(int min, int max) throws IOException {
                    super.collectRange(min, max);
                }
            };
        }

        @Override
        SignificantStringTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new SignificantStringTerms.Bucket[size][];
        }

        @Override
        SignificantStringTerms.Bucket[] buildBuckets(int size) {
            return new SignificantStringTerms.Bucket[size];
        }

        @Override
        SignificantStringTerms.Bucket buildEmptyTemporaryBucket() {
            return new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null, format, 0);
        }

        private long subsetSize(long owningBucketOrd) {
            // if the owningBucketOrd is not in the array that means the bucket is empty so the size has to be 0
            return owningBucketOrd < subsetSizes.size() ? subsetSizes.get(owningBucketOrd) : 0;
        }

        @Override
        BucketUpdater<SignificantStringTerms.Bucket> bucketUpdater(long owningBucketOrd) throws IOException {
            long subsetSize = subsetSize(owningBucketOrd);
            return (spare, globalOrd, bucketOrd, docCount) -> {
                spare.bucketOrd = bucketOrd;
                // Recreate DocValues as needed for concurrent segment search
                SortedSetDocValues values = getDocValues();
                oversizedCopy(values.lookupOrd(globalOrd), spare.termBytes);
                spare.subsetDf = docCount;
                spare.subsetSize = subsetSize;
                spare.supersetDf = backgroundFrequencies.freq(spare.termBytes);
                spare.supersetSize = supersetSize;
                /*
                 * During shard-local down-selection we use subset/superset stats
                 * that are for this shard only. Back at the central reducer these
                 * properties will be updated with global stats.
                 */
                spare.updateScore(significanceHeuristic);
            };
        }

        @Override
        PriorityQueue<SignificantStringTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size);
        }

        @Override
        SignificantStringTerms.Bucket convertTempBucketToRealBucket(SignificantStringTerms.Bucket temp) throws IOException {
            return temp;
        }

        @Override
        void buildSubAggs(SignificantStringTerms.Bucket[][] topBucketsPreOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPreOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        SignificantStringTerms buildResult(long owningBucketOrd, long otherDocCount, SignificantStringTerms.Bucket[] topBuckets) {
            return new SignificantStringTerms(
                name,
                metadata(),
                format,
                subsetSize(owningBucketOrd),
                supersetSize,
                significanceHeuristic,
                Arrays.asList(topBuckets),
                bucketCountThresholds
            );
        }

        @Override
        SignificantStringTerms buildEmptyResult() {
            return buildEmptySignificantTermsAggregation(0, significanceHeuristic);
        }

        @Override
        SignificantStringTerms buildNoValuesResult(long owningBucketOrdinal) {
            return buildEmptySignificantTermsAggregation(subsetSizes.get(owningBucketOrdinal), significanceHeuristic);
        }

        @Override
        public void close() {
            Releasables.close(backgroundFrequencies, subsetSizes);
        }

        /**
         * Copies the bytes from {@code from} into {@code to}, oversizing
         * the destination array if the bytes won't fit into the array.
         * <p>
         * This is fairly similar in spirit to
         * {@link BytesRef#deepCopyOf(BytesRef)} in that it is a way to read
         * bytes from a mutable {@link BytesRef} into
         * <strong>something</strong> that won't mutate out from under you.
         * Unlike {@linkplain BytesRef#deepCopyOf(BytesRef)} its designed to
         * be run over and over again into the same destination. In particular,
         * oversizing the destination bytes helps to keep from allocating
         * a bunch of little arrays over and over and over again.
         */
        private void oversizedCopy(BytesRef from, BytesRef to) {
            if (to.bytes.length < from.length) {
                to.bytes = new byte[ArrayUtil.oversize(from.length, 1)];
            }
            to.offset = 0;
            to.length = from.length;
            System.arraycopy(from.bytes, from.offset, to.bytes, 0, from.length);
        }
    }

    /**
     * Predicate used for {@link #acceptedGlobalOrdinals} if there is no filter.
     */
    private static final LongPredicate ALWAYS_TRUE = l -> true;

    /**
     * If DocValues have not been initialized yet for reduce phase, create and set them.
     */
    private SortedSetDocValues getDocValues() throws IOException {
        if (dvs.get() == null) {
            dvs.set(
                !context.searcher().getIndexReader().leaves().isEmpty()
                    ? valuesSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0))
                    : DocValues.emptySortedSet()
            );
        }
        return dvs.get();
    }
}
