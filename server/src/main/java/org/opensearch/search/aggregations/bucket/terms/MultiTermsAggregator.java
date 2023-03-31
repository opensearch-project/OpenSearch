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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.opensearch.search.aggregations.support.AggregationPath;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;
import static org.opensearch.search.aggregations.bucket.terms.TermsAggregator.descendsFromNestedAggregator;

/**
 * An aggregator that aggregate with multi_terms.
 *
 * @opensearch.internal
 */
public class MultiTermsAggregator extends DeferableBucketAggregator {

    private final BytesKeyedBucketOrds bucketOrds;
    private final MultiTermsValuesSource multiTermsValue;
    private final boolean showTermDocCountError;
    private final List<DocValueFormat> formats;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final BucketOrder order;
    private final Comparator<InternalMultiTerms.Bucket> partiallyBuiltBucketComparator;
    private final SubAggCollectionMode collectMode;
    private final Set<Aggregator> aggsUsedForSorting = new HashSet<>();

    public MultiTermsAggregator(
        String name,
        AggregatorFactories factories,
        boolean showTermDocCountError,
        List<InternalValuesSource> internalValuesSources,
        List<DocValueFormat> formats,
        BucketOrder order,
        SubAggCollectionMode collectMode,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.bucketOrds = BytesKeyedBucketOrds.build(context.bigArrays(), cardinality);
        this.multiTermsValue = new MultiTermsValuesSource(internalValuesSources);
        this.showTermDocCountError = showTermDocCountError;
        this.formats = formats;
        this.bucketCountThresholds = bucketCountThresholds;
        this.order = order;
        this.partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
        // Todo, copy from TermsAggregator. need to remove duplicate code.
        if (subAggsNeedScore() && descendsFromNestedAggregator(parent)) {
            /**
             * Force the execution to depth_first because we need to access the score of
             * nested documents in a sub-aggregation and we are not able to generate this score
             * while replaying deferred documents.
             */
            this.collectMode = SubAggCollectionMode.DEPTH_FIRST;
        } else {
            this.collectMode = collectMode;
        }
        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof InternalOrder.Aggregation) {
            AggregationPath path = ((InternalOrder.Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
        } else if (order instanceof InternalOrder.CompoundOrder) {
            InternalOrder.CompoundOrder compoundOrder = (InternalOrder.CompoundOrder) order;
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
        InternalMultiTerms.Bucket[][] topBucketsPerOrd = new InternalMultiTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
            long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);

            int size = (int) Math.min(bucketsInOrd, bucketCountThresholds.getShardSize());
            PriorityQueue<InternalMultiTerms.Bucket> ordered = new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
            InternalMultiTerms.Bucket spare = null;
            BytesRef dest = null;
            BytesKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
            CheckedSupplier<InternalMultiTerms.Bucket, IOException> emptyBucketBuilder = () -> InternalMultiTerms.Bucket.EMPTY(
                showTermDocCountError,
                formats
            );
            while (ordsEnum.next()) {
                long docCount = bucketDocCount(ordsEnum.ord());
                otherDocCounts[ordIdx] += docCount;
                if (docCount < bucketCountThresholds.getShardMinDocCount()) {
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

            // Get the top buckets
            InternalMultiTerms.Bucket[] bucketsForOrd = new InternalMultiTerms.Bucket[ordered.size()];
            topBucketsPerOrd[ordIdx] = bucketsForOrd;
            for (int b = ordered.size() - 1; b >= 0; --b) {
                topBucketsPerOrd[ordIdx][b] = ordered.pop();
                otherDocCounts[ordIdx] -= topBucketsPerOrd[ordIdx][b].getDocCount();
            }
        }

        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);

        InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCounts[ordIdx], topBucketsPerOrd[ordIdx]);
        }
        return result;
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
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getMinDocCount(),
            metadata(),
            bucketCountThresholds.getShardSize(),
            showTermDocCountError,
            otherDocCount,
            0,
            formats,
            List.of(topBuckets)
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return null;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                for (List<Object> value : collector.apply(doc)) {
                    long bucketOrd = bucketOrds.add(owningBucketOrd, encode(value));
                    if (bucketOrd < 0) {
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                    } else {
                        collectBucket(sub, doc, bucketOrd);
                    }
                }
            }
        };
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }

    private static BytesRef encode(List<Object> values) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeCollection(values, StreamOutput::writeGenericValue);
            return output.bytes().toBytesRef();
        } catch (IOException e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
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
        if (InternalOrder.isCountDesc(order) && bucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
            return;
        }
        // we need to fill-in the blanks
        for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
            MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx);
            // brute force
            for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                for (List<Object> value : collector.apply(docId)) {
                    bucketOrds.add(owningBucketOrd, encode(value));
                }
            }
        }
    }

    /**
     * A multi_terms collector which collect values on each doc,
     */
    @FunctionalInterface
    interface MultiTermsValuesSourceCollector {
        /**
         * Collect a list values of multi_terms on each doc.
         * Each terms could have multi_values, so the result is the cartesian product of each term's values.
         */
        List<List<Object>> apply(int doc) throws IOException;
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
        List<Object> apply(int doc) throws IOException;
    }

    /**
     * Multi_Term ValuesSource, it is a collection of {@link InternalValuesSource}
     *
     * @opensearch.internal
     */
    static class MultiTermsValuesSource {
        private final List<InternalValuesSource> valuesSources;

        public MultiTermsValuesSource(List<InternalValuesSource> valuesSources) {
            this.valuesSources = valuesSources;
        }

        public MultiTermsValuesSourceCollector getValues(LeafReaderContext ctx) throws IOException {
            List<InternalValuesSourceCollector> collectors = new ArrayList<>();
            for (InternalValuesSource valuesSource : valuesSources) {
                collectors.add(valuesSource.apply(ctx));
            }
            return new MultiTermsValuesSourceCollector() {
                @Override
                public List<List<Object>> apply(int doc) throws IOException {
                    List<CheckedSupplier<List<Object>, IOException>> collectedValues = new ArrayList<>();
                    for (InternalValuesSourceCollector collector : collectors) {
                        collectedValues.add(() -> collector.apply(doc));
                    }
                    List<List<Object>> result = new ArrayList<>();
                    apply(0, collectedValues, new ArrayList<>(), result);
                    return result;
                }

                /**
                 * DFS traverse each term's values and add cartesian product to results lists.
                 */
                private void apply(
                    int index,
                    List<CheckedSupplier<List<Object>, IOException>> collectedValues,
                    List<Object> current,
                    List<List<Object>> results
                ) throws IOException {
                    if (index == collectedValues.size()) {
                        results.add(List.copyOf(current));
                    } else if (null != collectedValues.get(index)) {
                        for (Object value : collectedValues.get(index).get()) {
                            current.add(value);
                            apply(index + 1, collectedValues, current, results);
                            current.remove(current.size() - 1);
                        }
                    }
                }
            };
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
                    BytesRefBuilder previous = new BytesRefBuilder();

                    if (false == values.advanceExact(doc)) {
                        return Collections.emptyList();
                    }
                    int valuesCount = values.docValueCount();
                    List<Object> termValues = new ArrayList<>(valuesCount);

                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    previous.clear();
                    for (int i = 0; i < valuesCount; ++i) {
                        BytesRef bytes = values.nextValue();
                        if (includeExclude != null && false == includeExclude.accept(bytes)) {
                            continue;
                        }
                        if (i > 0 && previous.get().equals(bytes)) {
                            continue;
                        }
                        previous.copyBytes(bytes);
                        termValues.add(BytesRef.deepCopyOf(bytes));
                    }
                    return termValues;
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
                        List<Object> termValues = new ArrayList<>(valuesCount);
                        for (int i = 0; i < valuesCount; ++i) {
                            long val = values.nextValue();
                            if (previous != val || i == 0) {
                                if (longFilter == null || longFilter.accept(val)) {
                                    termValues.add(val);
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
                        List<Object> termValues = new ArrayList<>(valuesCount);
                        for (int i = 0; i < valuesCount; ++i) {
                            double val = values.nextValue();
                            if (previous != val || i == 0) {
                                if (longFilter == null || longFilter.accept(NumericUtils.doubleToSortableLong(val))) {
                                    termValues.add(val);
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
