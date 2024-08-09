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
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.support.AggregationPath;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

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
            /*
              Force the execution to depth_first because we need to access the score of
              nested documents in a sub-aggregation and we are not able to generate this score
              while replaying deferred documents.
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
        LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(bucketCountThresholds);
        InternalMultiTerms.Bucket[][] topBucketsPerOrd = new InternalMultiTerms.Bucket[owningBucketOrds.length][];
        long[] otherDocCounts = new long[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
            long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);

            int size = (int) Math.min(bucketsInOrd, localBucketCountThresholds.getRequiredSize());
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
        MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx, bucketOrds, this, sub);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                collector.apply(doc, owningBucketOrd);
            }
        };
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds, multiTermsValue);
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
            // brute force
            MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx, bucketOrds, null, null);
            for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                collector.apply(docId, owningBucketOrd);
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
        private final List<InternalValuesSource> valuesSources;
        private final BytesStreamOutput scratch = new BytesStreamOutput();

        public MultiTermsValuesSource(List<InternalValuesSource> valuesSources) {
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
                    for (int i = 0; i < numIterations; i++) {
                        TermValue<?> value = values.get(i);
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
