/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.Numbers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.support.AggregationPath;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.MatchAllFilter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;
import static org.opensearch.search.aggregations.bucket.terms.TermsAggregator.descendsFromNestedAggregator;
import static org.opensearch.search.startree.StarTreeQueryHelper.getSupportedStarTree;

/**
 * An aggregator that aggregate with multi_terms.
 *
 * @opensearch.internal
 */
public class MultiTermsAggregator extends DeferableBucketAggregator implements StarTreePreComputeCollector {

    private final BytesKeyedBucketOrds bucketOrds;
    private final MultiTermsBucketOrds ordinalBucketOrds;
    private final List<ValuesSource.Bytes.WithOrdinals> ordinalSources;
    private final MultiTermsValuesSource multiTermsValue;
    private final boolean showTermDocCountError;
    private final List<DocValueFormat> formats;
    private final List<String> fields;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final BucketOrder order;
    private final Comparator<InternalMultiTerms.Bucket> partiallyBuiltBucketComparator;
    private final SubAggCollectionMode collectMode;
    private final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    private final BytesStreamOutput starTreeScratch = new BytesStreamOutput();

    public MultiTermsAggregator(
        String name,
        AggregatorFactories factories,
        boolean showTermDocCountError,
        List<ValuesSource> rawValuesSources,
        List<InternalValuesSource> internalValuesSources,
        List<String> fields,
        List<DocValueFormat> formats,
        BucketOrder order,
        SubAggCollectionMode collectMode,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata,
        MultiTermsBucketOrds ordinalBucketOrds
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.ordinalBucketOrds = ordinalBucketOrds;
        if (ordinalBucketOrds != null) {
            List<ValuesSource.Bytes.WithOrdinals> ordSources = new ArrayList<>(rawValuesSources.size());
            for (ValuesSource vs : rawValuesSources) {
                ordSources.add((ValuesSource.Bytes.WithOrdinals) vs);
            }
            this.ordinalSources = ordSources;
        } else {
            this.ordinalSources = null;
        }
        this.bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
        this.multiTermsValue = new MultiTermsValuesSource(rawValuesSources, internalValuesSources);
        this.showTermDocCountError = showTermDocCountError;
        this.formats = formats;
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = order;
        this.partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
        // Todo, copy from TermsAggregator. need to remove duplicate code.
        if (subAggsNeedScore() && descendsFromNestedAggregator(parent)) {
            /*
              Force the execution to depth_first because we need to access the score of
              nested documents in a sub-aggregation and we are not able to generate this score
              while replaying deferred documents.
             */
            this.collectMode = SubAggCollectionMode.DEPTH_FIRST;
        } else {
            this.collectMode = collectMode;
        }
        this.fields = fields;
        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof InternalOrder.Aggregation) {
            AggregationPath path = ((InternalOrder.Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
        } else if (order instanceof InternalOrder.CompoundOrder compoundOrder) {
            for (BucketOrder orderElement : compoundOrder.orderElements()) {
                if (orderElement instanceof InternalOrder.Aggregation) {
                    AggregationPath path = ((InternalOrder.Aggregation) orderElement).path();
                    aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
                }
            }
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(bucketCountThresholds);
        InternalMultiTerms.Bucket[][] topBucketsPerOrd = new InternalMultiTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];

        // Resolve SortedSetDocValues for ordinal lookup if using ordinal path
        SortedSetDocValues[] globalOrdsForLookup = null;
        if (ordinalBucketOrds != null) {
            globalOrdsForLookup = new SortedSetDocValues[ordinalSources.size()];
            List<LeafReaderContext> leaves = context.searcher().getTopReaderContext().leaves();
            if (leaves.isEmpty() == false) {
                LeafReaderContext leafCtx = leaves.get(0);
                for (int i = 0; i < ordinalSources.size(); i++) {
                    globalOrdsForLookup[i] = ordinalSources.get(i).globalOrdinalsValues(leafCtx);
                }
            }
        }

        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            checkCancelled();
            collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);

            if (ordinalBucketOrds != null) {
                topBucketsPerOrd[ordIdx] = buildOrdinalBuckets(
                    owningBucketOrds[ordIdx],
                    localBucketCountThresholds,
                    otherDocCounts,
                    ordIdx,
                    globalOrdsForLookup
                );
            } else {
                topBucketsPerOrd[ordIdx] = buildBytesBuckets(owningBucketOrds[ordIdx], localBucketCountThresholds, otherDocCounts, ordIdx);
            }
        }

        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCounts[ordIdx], topBucketsPerOrd[ordIdx]);
        }
        return result;
    }

    /**
     * Build top buckets from the ordinal-based path by resolving packed ordinals
     * back to term values via {@code lookupOrd()}.
     */
    private InternalMultiTerms.Bucket[] buildOrdinalBuckets(
        long owningBucketOrd,
        LocalBucketCountThresholds localBucketCountThresholds,
        long[] otherDocCounts,
        int ordIdx,
        SortedSetDocValues[] globalOrdsForLookup
    ) throws IOException {
        long bucketsInOrd = ordinalBucketOrds.bucketsInOrd(owningBucketOrd);
        int size = (int) Math.min(bucketsInOrd, localBucketCountThresholds.getRequiredSize());
        PriorityQueue<InternalMultiTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        InternalMultiTerms.Bucket spare = null;
        CheckedSupplier<InternalMultiTerms.Bucket, IOException> emptyBucketBuilder = () -> InternalMultiTerms.Bucket.EMPTY(
            showTermDocCountError,
            formats
        );
        MultiTermsBucketOrds.BucketOrdsEnum ordsEnum = ordinalBucketOrds.ordsEnum(owningBucketOrd);
        while (ordsEnum.next()) {
            long docCount = bucketDocCount(ordsEnum.ord());
            otherDocCounts[ordIdx] += docCount;
            if (docCount < localBucketCountThresholds.getMinDocCount()) {
                continue;
            }
            if (spare == null) {
                spare = emptyBucketBuilder.get();
            }

            long[] ordinals = ordsEnum.ordinals();
            List<Object> termValues = new ArrayList<>(ordinals.length);
            for (int i = 0; i < ordinals.length; i++) {
                termValues.add(BytesRef.deepCopyOf(globalOrdsForLookup[i].lookupOrd(ordinals[i])));
            }

            spare.termValues = termValues;
            spare.docCount = docCount;
            spare.bucketOrd = ordsEnum.ord();
            spare = ordered.insertWithOverflow(spare);
        }

        InternalMultiTerms.Bucket[] bucketsForOrd = new InternalMultiTerms.Bucket[ordered.size()];
        for (int b = ordered.size() - 1; b >= 0; --b) {
            bucketsForOrd[b] = ordered.pop();
            otherDocCounts[ordIdx] -= bucketsForOrd[b].getDocCount();
        }
        return bucketsForOrd;
    }

    /**
     * Build top buckets from the existing bytes-based path.
     */
    private InternalMultiTerms.Bucket[] buildBytesBuckets(
        long owningBucketOrd,
        LocalBucketCountThresholds localBucketCountThresholds,
        long[] otherDocCounts,
        int ordIdx
    ) throws IOException {
        long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrd);
        int size = (int) Math.min(bucketsInOrd, localBucketCountThresholds.getRequiredSize());
        PriorityQueue<InternalMultiTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        InternalMultiTerms.Bucket spare = null;
        BytesRef dest = null;
        BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
        CheckedSupplier<InternalMultiTerms.Bucket, IOException> emptyBucketBuilder = () -> InternalMultiTerms.Bucket.EMPTY(
            showTermDocCountError,
            formats
        );
        while (ordsEnum.next()) {
            long docCount = bucketDocCount(ordsEnum.ord());
            otherDocCounts[ordIdx] += docCount;
            if (docCount < localBucketCountThresholds.getMinDocCount()) {
                continue;
            }
            if (spare == null) {
                spare = emptyBucketBuilder.get();
                dest = new BytesRef();
            }

            ordsEnum.readValue(dest);

            spare.termValues = decode(dest);
            spare.docCount = docCount;
            spare.bucketOrd = ordsEnum.ord();
            spare = ordered.insertWithOverflow(spare);
        }

        InternalMultiTerms.Bucket[] bucketsForOrd = new InternalMultiTerms.Bucket[ordered.size()];
        for (int b = ordered.size() - 1; b >= 0; --b) {
            bucketsForOrd[b] = ordered.pop();
            otherDocCounts[ordIdx] -= bucketsForOrd[b].getDocCount();
        }
        return bucketsForOrd;
    }

    InternalMultiTerms buildResult(long owningBucketOrd, long otherDocCount, InternalMultiTerms.Bucket[] topBuckets) {
        BucketOrder reduceOrder;
        if (isKeyOrder(order) == false) {
            reduceOrder = InternalOrder.key(true);
            Arrays.sort(topBuckets, reduceOrder.comparator());
        } else {
            reduceOrder = order;
        }
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            metadata(),
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            otherDocCount,
            0,
            formats,
            List.of(topBuckets),
            bucketCountThresholds
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMultiTerms(
            name,
            order,
            order,
            metadata(),
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            0,
            0,
            formats,
            Collections.emptyList(),
            bucketCountThresholds
        );
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (ordinalBucketOrds != null) {
            return getOrdinalLeafCollector(ctx, sub);
        }
        MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx, bucketOrds, this, sub);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                collector.apply(doc, owningBucketOrd);
            }
        };
    }

    /**
     * Creates a leaf collector that collects global ordinals directly instead of
     * serialized term values. Generates the cartesian product of ordinal tuples
     * across all fields for each document.
     */
    private LeafBucketCollector getOrdinalLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        int numFields = ordinalSources.size();
        SortedSetDocValues[] globalOrds = new SortedSetDocValues[numFields];
        for (int i = 0; i < numFields; i++) {
            globalOrds[i] = ordinalSources.get(i).globalOrdinalsValues(ctx);
        }
        return new LeafBucketCollector() {
            private final long[] ordTuple = new long[numFields];

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                long[][] ordinalSets = new long[numFields][];
                for (int i = 0; i < numFields; i++) {
                    if (false == globalOrds[i].advanceExact(doc)) {
                        return; // missing value in any field → skip doc
                    }
                    int count = globalOrds[i].docValueCount();
                    ordinalSets[i] = new long[count];
                    for (int j = 0; j < count; j++) {
                        ordinalSets[i][j] = globalOrds[i].nextOrd();
                    }
                }
                generateOrdinalCombinations(ordinalSets, 0, ordTuple, owningBucketOrd, doc, sub);
            }

            private void generateOrdinalCombinations(
                long[][] ordinalSets,
                int depth,
                long[] current,
                long owningBucketOrd,
                int doc,
                LeafBucketCollector sub
            ) throws IOException {
                if (depth == ordinalSets.length) {
                    long bucketOrd = ordinalBucketOrds.add(owningBucketOrd, current);
                    if (bucketOrd < 0) {
                        collectExistingBucket(sub, doc, -1 - bucketOrd);
                    } else {
                        collectBucket(sub, doc, bucketOrd);
                    }
                    return;
                }
                for (long ord : ordinalSets[depth]) {
                    current[depth] = ord;
                    generateOrdinalCombinations(ordinalSets, depth + 1, current, owningBucketOrd, doc, sub);
                }
            }
        };
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        CompositeIndexFieldInfo supportedStarTree = getSupportedStarTree(this.context.getQueryShardContext());
        if (supportedStarTree != null) {
            preComputeWithStarTree(ctx, supportedStarTree);
            return true;
        }
        return false;
    }

    private void preComputeWithStarTree(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException {
        StarTreeBucketCollector starTreeBucketCollector = getStarTreeBucketCollector(ctx, starTree, null);
        StarTreeQueryHelper.preComputeBucketsWithStarTree(starTreeBucketCollector);
    }

    /**
     * Creates a {@link StarTreeBucketCollector} for pre-aggregating with a star-tree index.
     * This collector generates the cartesian product of dimension values within a single star-tree entry
     * to form the composite keys for the multi-terms aggregation.
     */
    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parent
    ) throws IOException {
        StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(ctx, starTree);
        assert starTreeValues != null;
        SortedNumericStarTreeValuesIterator docCountsIterator = StarTreeQueryHelper.getDocCountsIterator(starTreeValues, starTree);

        // Get an iterator for each field (dimension) in the multi-terms aggregation.
        final List<StarTreeValuesIterator> dimensionIterators = new ArrayList<>();
        // We also need a way to convert the raw long values from the iterators into the correct TermValue type.
        final List<Function<Long, TermValue<?>>> termValueBuilders = new ArrayList<>();

        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i);
            dimensionIterators.add(starTreeValues.getDimensionValuesIterator(fieldName));
            ValuesSource vs = multiTermsValue.rawValueSources.get(i);

            if (vs instanceof ValuesSource.Bytes.WithOrdinals vsBytes) {
                termValueBuilders.add(ord -> {
                    try {
                        return TermValue.of(vsBytes.globalOrdinalsValues(ctx).lookupOrd(ord));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                });
            } else if (vs instanceof ValuesSource.Numeric numericSource) {
                if (numericSource.isFloatingPoint()) {
                    NumberFieldMapper.NumberFieldType numberFieldType = ((NumberFieldMapper.NumberFieldType) context.mapperService()
                        .fieldType(fieldName));
                    termValueBuilders.add(val -> TermValue.of(numberFieldType.toDoubleValue(val)));
                } else {
                    termValueBuilders.add(TermValue::of);
                }
            } else {
                throw new IllegalStateException("Unsupported ValuesSource type for star-tree: " + vs.getClass().getName());
            }

        }

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
                if (docCountsIterator.advanceExact(starTreeEntry) == false) {
                    return; // No documents in this star-tree entry.
                }
                long docCountMetric = docCountsIterator.nextValue();

                List<List<TermValue<?>>> collectedValues = new ArrayList<>();
                for (int i = 0; i < dimensionIterators.size(); i++) {
                    StarTreeValuesIterator dimIterator = dimensionIterators.get(i);
                    if (!dimIterator.advanceExact(starTreeEntry)) {
                        // If any dimension is missing for this entry, the cartesian product is empty.
                        return;
                    }

                    List<TermValue<?>> valuesForDim = new ArrayList<>();
                    Function<Long, TermValue<?>> builder = termValueBuilders.get(i);
                    for (int j = 0; j < dimIterator.entryValueCount(); j++) {
                        valuesForDim.add(builder.apply(dimIterator.value()));
                    }
                    collectedValues.add(valuesForDim);
                }

                starTreeScratch.seek(0);
                starTreeScratch.writeVInt(dimensionIterators.size());
                generateAndCollectFromStarTree(collectedValues, 0, owningBucketOrd, starTreeEntry, docCountMetric);
            }

            private void generateAndCollectFromStarTree(
                List<List<TermValue<?>>> collectedValues,
                int index,
                long owningBucketOrd,
                int starTreeEntry,
                long docCountMetric
            ) throws IOException {
                if (index == collectedValues.size()) {
                    // A full composite key is in the buffer, add it to bucketOrds.
                    long bucketOrd = bucketOrds.add(owningBucketOrd, starTreeScratch.bytes().toBytesRef());
                    collectStarTreeBucket(this, docCountMetric, bucketOrd, starTreeEntry);
                    return;
                }

                long position = starTreeScratch.position();
                List<TermValue<?>> values = collectedValues.get(index);
                for (TermValue<?> value : values) {
                    value.writeTo(starTreeScratch);
                    generateAndCollectFromStarTree(collectedValues, index + 1, owningBucketOrd, starTreeEntry, docCountMetric);
                    starTreeScratch.seek(position);
                }
            }
        };
    }

    @Override
    public List<DimensionFilter> getDimensionFilters() {
        return StarTreeQueryHelper.collectDimensionFilters(
            fields.stream().map(a -> (DimensionFilter) new MatchAllFilter(a)).toList(),
            subAggregators
        );
    }

    /**
     * Returns the ordinal-based bucket ords, or {@code null} if the bytes-based path is active.
     * Package-private for testing.
     */
    MultiTermsBucketOrds getOrdinalBucketOrds() {
        return ordinalBucketOrds;
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds, ordinalBucketOrds, multiTermsValue);
    }

    private static List<Object> decode(BytesRef bytesRef) {
        try (StreamInput input = new BytesArray(bytesRef).streamInput()) {
            return input.readList(StreamInput::readGenericValue);
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private boolean subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return collectMode == Aggregator.SubAggCollectionMode.BREADTH_FIRST && !aggsUsedForSorting.contains(aggregator);
    }

    private void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {
        if (bucketCountThresholds.getMinDocCount() != 0) {
            return;
        }
        if (ordinalBucketOrds != null) {
            if (InternalOrder.isCountDesc(order)
                && ordinalBucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                collectZeroDocOrdinals(ctx, owningBucketOrd);
            }
        } else {
            if (InternalOrder.isCountDesc(order) && bucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                // brute force
                MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx, bucketOrds, null, null);
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    collector.apply(docId, owningBucketOrd);
                }
            }
        }
    }

    /**
     * Adds all ordinal tuples from a leaf to the ordinal bucket ords without
     * collecting sub-aggregators or incrementing doc counts. Used for zero-doc
     * entry filling when minDocCount is 0.
     */
    private void collectZeroDocOrdinals(LeafReaderContext ctx, long owningBucketOrd) throws IOException {
        int numFields = ordinalSources.size();
        SortedSetDocValues[] globalOrds = new SortedSetDocValues[numFields];
        for (int i = 0; i < numFields; i++) {
            globalOrds[i] = ordinalSources.get(i).globalOrdinalsValues(ctx);
        }
        long[] ordTuple = new long[numFields];
        for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
            long[][] ordinalSets = new long[numFields][];
            boolean skip = false;
            for (int i = 0; i < numFields; i++) {
                if (false == globalOrds[i].advanceExact(docId)) {
                    skip = true;
                    break;
                }
                int count = globalOrds[i].docValueCount();
                ordinalSets[i] = new long[count];
                for (int j = 0; j < count; j++) {
                    ordinalSets[i][j] = globalOrds[i].nextOrd();
                }
            }
            if (skip) {
                continue;
            }
            addOrdinalCombinations(ordinalSets, 0, ordTuple, owningBucketOrd);
        }
    }

    private void addOrdinalCombinations(long[][] ordinalSets, int depth, long[] current, long owningBucketOrd) {
        if (depth == ordinalSets.length) {
            ordinalBucketOrds.add(owningBucketOrd, current);
            return;
        }
        for (long ord : ordinalSets[depth]) {
            current[depth] = ord;
            addOrdinalCombinations(ordinalSets, depth + 1, current, owningBucketOrd);
        }
    }

    /**
     * A multi_terms collector which collect values on each doc,
     */
    @FunctionalInterface
    interface MultiTermsValuesSourceCollector {
        /**
         * Generates the cartesian product of all fields used in aggregation and
         * collects them in buckets using the composite key of their field values.
         */
        void apply(int doc, long owningBucketOrd) throws IOException;

    }

    @FunctionalInterface
    interface InternalValuesSource {
        /**
         * Create {@link InternalValuesSourceCollector} from existing {@link LeafReaderContext}.
         */
        InternalValuesSourceCollector apply(LeafReaderContext ctx) throws IOException;
    }

    /**
     * A terms collector which collect values on each doc,
     */
    @FunctionalInterface
    interface InternalValuesSourceCollector {
        /**
         * Collect a list values of a term on specific doc.
         */
        List<TermValue<?>> apply(int doc) throws IOException;
    }

    /**
     * Represents an individual term value.
     */
    static class TermValue<T> implements Writeable {
        private static final Writer<BytesRef> BYTES_REF_WRITER = StreamOutput.getWriter(BytesRef.class);
        private static final Writer<Long> LONG_WRITER = StreamOutput.getWriter(Long.class);
        private static final Writer<BigInteger> BIG_INTEGER_WRITER = StreamOutput.getWriter(BigInteger.class);
        private static final Writer<Double> DOUBLE_WRITER = StreamOutput.getWriter(Double.class);

        private final T value;
        private final Writer<T> writer;

        private TermValue(T value, Writer<T> writer) {
            this.value = value;
            this.writer = writer;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            writer.write(out, value);
        }

        public static TermValue<BytesRef> of(BytesRef value) {
            return new TermValue<>(value, BYTES_REF_WRITER);
        }

        public static TermValue<Long> of(Long value) {
            return new TermValue<>(value, LONG_WRITER);
        }

        public static TermValue<BigInteger> of(BigInteger value) {
            return new TermValue<>(value, BIG_INTEGER_WRITER);
        }

        public static TermValue<Double> of(Double value) {
            return new TermValue<>(value, DOUBLE_WRITER);
        }
    }

    /**
     * Multi_Term ValuesSource, it is a collection of {@link InternalValuesSource}
     *
     * @opensearch.internal
     */
    static class MultiTermsValuesSource implements Releasable {
        private final List<ValuesSource> rawValueSources;
        private final List<InternalValuesSource> valuesSources;
        private final BytesStreamOutput scratch = new BytesStreamOutput();

        public MultiTermsValuesSource(List<ValuesSource> rawValueSources, List<InternalValuesSource> valuesSources) {
            this.rawValueSources = rawValueSources;
            this.valuesSources = valuesSources;
        }

        public MultiTermsValuesSourceCollector getValues(
            LeafReaderContext ctx,
            BytesKeyedBucketOrds bucketOrds,
            BucketsAggregator aggregator,
            LeafBucketCollector sub
        ) throws IOException {
            List<InternalValuesSourceCollector> collectors = new ArrayList<>();
            for (InternalValuesSource valuesSource : valuesSources) {
                collectors.add(valuesSource.apply(ctx));
            }
            boolean collectBucketOrds = aggregator != null && sub != null;
            return new MultiTermsValuesSourceCollector() {

                /**
                 * This method does the following : <br>
                 * <li>Fetches the values of every field present in the doc List<List<TermValue<?>>> via @{@link InternalValuesSourceCollector}</li>
                 * <li>Generates Composite keys from the fetched values for all fields present in the aggregation.</li>
                 * <li>Adds every composite key to the @{@link BytesKeyedBucketOrds} and Optionally collects them via @{@link BucketsAggregator#collectBucket(LeafBucketCollector, int, long)}</li>
                 */
                @Override
                public void apply(int doc, long owningBucketOrd) throws IOException {
                    // TODO A new list creation can be avoided for every doc.
                    List<List<TermValue<?>>> collectedValues = new ArrayList<>();
                    for (InternalValuesSourceCollector collector : collectors) {
                        collectedValues.add(collector.apply(doc));
                    }
                    scratch.seek(0);
                    scratch.writeVInt(collectors.size()); // number of fields per composite key
                    generateAndCollectCompositeKeys(collectedValues, 0, owningBucketOrd, doc);
                }

                /**
                 * This generates and collects all Composite keys in their buckets by performing a cartesian product <br>
                 * of all the values in all the fields ( used in agg ) for the given doc recursively.
                 * @param collectedValues : Values of all fields present in the aggregation for the @doc
                 * @param index : Points to the field being added to generate the composite key
                 */
                private void generateAndCollectCompositeKeys(
                    List<List<TermValue<?>>> collectedValues,
                    int index,
                    long owningBucketOrd,
                    int doc
                ) throws IOException {
                    if (collectedValues.size() == index) {
                        // Avoid performing a deep copy of the composite key by inlining.
                        long bucketOrd = bucketOrds.add(owningBucketOrd, scratch.bytes().toBytesRef());
                        if (collectBucketOrds) {
                            if (bucketOrd < 0) {
                                bucketOrd = -1 - bucketOrd;
                                aggregator.collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                aggregator.collectBucket(sub, doc, bucketOrd);
                            }
                        }
                        return;
                    }

                    long position = scratch.position();
                    List<TermValue<?>> values = collectedValues.get(index);
                    int numIterations = values.size();
                    // For each loop is not done to reduce the allocations done for Iterator objects
                    // once for every field in every doc.
                    for (TermValue<?> value : values) {
                        value.writeTo(scratch); // encode the value
                        generateAndCollectCompositeKeys(collectedValues, index + 1, owningBucketOrd, doc); // dfs
                        scratch.seek(position); // backtrack
                    }
                }
            };
        }

        @Override
        public void close() {
            scratch.close();
        }
    }

    /**
     * Factory for construct {@link InternalValuesSource}.
     *
     * @opensearch.internal
     */
    static class InternalValuesSourceFactory {
        static InternalValuesSource bytesValuesSource(ValuesSource valuesSource, IncludeExclude.StringFilter includeExclude) {
            return ctx -> {
                SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
                return doc -> {
                    if (false == values.advanceExact(doc)) {
                        return Collections.emptyList();
                    }
                    int valuesCount = values.docValueCount();
                    List<TermValue<?>> termValues = new ArrayList<>(valuesCount);

                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    BytesRef previous = null;
                    for (int i = 0; i < valuesCount; ++i) {
                        BytesRef bytes = values.nextValue();
                        if (includeExclude != null && false == includeExclude.accept(bytes)) {
                            continue;
                        }
                        if (i > 0 && bytes.equals(previous)) {
                            continue;
                        }
                        // Performing a deep copy is not required for field containing only one value.
                        if (valuesCount > 1) {
                            BytesRef copy = BytesRef.deepCopyOf(bytes);
                            termValues.add(TermValue.of(copy));
                            previous = copy;
                        } else {
                            termValues.add(TermValue.of(bytes));
                        }
                    }
                    return termValues;
                };
            };
        }

        static InternalValuesSource unsignedLongValuesSource(ValuesSource.Numeric valuesSource, IncludeExclude.LongFilter longFilter) {
            return ctx -> {
                SortedNumericDocValues values = valuesSource.longValues(ctx);
                return doc -> {
                    if (values.advanceExact(doc)) {
                        int valuesCount = values.docValueCount();

                        BigInteger previous = Numbers.MAX_UNSIGNED_LONG_VALUE;
                        List<TermValue<?>> termValues = new ArrayList<>(valuesCount);
                        for (int i = 0; i < valuesCount; ++i) {
                            BigInteger val = Numbers.toUnsignedBigInteger(values.nextValue());
                            if (previous.compareTo(val) != 0 || i == 0) {
                                if (longFilter == null || longFilter.accept(NumericUtils.doubleToSortableLong(val.doubleValue()))) {
                                    termValues.add(TermValue.of(val));
                                }
                                previous = val;
                            }
                        }
                        return termValues;
                    }
                    return Collections.emptyList();
                };
            };
        }

        static InternalValuesSource longValuesSource(ValuesSource.Numeric valuesSource, IncludeExclude.LongFilter longFilter) {
            return ctx -> {
                SortedNumericDocValues values = valuesSource.longValues(ctx);
                return doc -> {
                    if (values.advanceExact(doc)) {
                        int valuesCount = values.docValueCount();

                        long previous = Long.MAX_VALUE;
                        List<TermValue<?>> termValues = new ArrayList<>(valuesCount);
                        for (int i = 0; i < valuesCount; ++i) {
                            long val = values.nextValue();
                            if (previous != val || i == 0) {
                                if (longFilter == null || longFilter.accept(val)) {
                                    termValues.add(TermValue.of(val));
                                }
                                previous = val;
                            }
                        }
                        return termValues;
                    }
                    return Collections.emptyList();
                };
            };
        }

        static InternalValuesSource doubleValueSource(ValuesSource.Numeric valuesSource, IncludeExclude.LongFilter longFilter) {
            return ctx -> {
                SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
                return doc -> {
                    if (values.advanceExact(doc)) {
                        int valuesCount = values.docValueCount();

                        double previous = Double.MAX_VALUE;
                        List<TermValue<?>> termValues = new ArrayList<>(valuesCount);
                        for (int i = 0; i < valuesCount; ++i) {
                            double val = values.nextValue();
                            if (previous != val || i == 0) {
                                if (longFilter == null || longFilter.accept(NumericUtils.doubleToSortableLong(val))) {
                                    termValues.add(TermValue.of(val));
                                }
                                previous = val;
                            }
                        }
                        return termValues;
                    }
                    return Collections.emptyList();
                };
            };
        }
    }
}
