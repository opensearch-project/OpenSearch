/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Weight;
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
    private final List<ValuesSource> valuesSources;
    private final boolean showTermDocCountError;
    private final List<DocValueFormat> formats;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final BucketOrder order;
    private final Comparator<InternalMultiTerms.Bucket> partiallyBuiltBucketComparator;
    private final SubAggCollectionMode collectMode;
    private final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    private Weight weight;
    private static final Logger logger = LogManager.getLogger(MultiTermsAggregator.class);

    public MultiTermsAggregator(
        String name,
        AggregatorFactories factories,
        boolean showTermDocCountError,
        List<InternalValuesSource> internalValuesSources,
        List<ValuesSource> valuesSources,
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
        this.valuesSources = valuesSources;
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

    public void setWeight(Weight weight) {
        this.weight = weight;
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

    private LeafBucketCollector getTermFrequencies(LeafReaderContext ctx) throws IOException {
        // Instead of visiting doc values for each document, utilize posting data directly to get each composite bucket intersection
        // For example, if we have a composite key of (a, b) where a is from field1 & b is from field2
        // We can a find all the composite buckets by visiting both the posting lists
        // and counting all the documents that intersect for each composite bucket.
        // This is much faster than visiting the doc values for each document.

        if (weight == null || weight.count(ctx) != ctx.reader().maxDoc()) {
            // Weight not assigned - cannot use this optimization
            // weight.count(ctx) == ctx.reader().maxDoc() implies there are no deleted documents and
            // top-level query matches all docs in the segment
            return null;
        }

        String field1, field2;
        // Restricting the number of fields to 2 and only keyword fields with FieldData available
        if (this.valuesSources.size() == 2
            && this.valuesSources.get(0) instanceof ValuesSource.Bytes.WithOrdinals.FieldData
            && this.valuesSources.get(1) instanceof ValuesSource.Bytes.WithOrdinals.FieldData) {
            field1 = ((ValuesSource.Bytes.WithOrdinals.FieldData) valuesSources.get(0)).getIndexFieldName();
            field2 = ((ValuesSource.Bytes.WithOrdinals.FieldData) valuesSources.get(1)).getIndexFieldName();

        } else {
            return null;
        }

        Terms segmentTerms1 = ctx.reader().terms(field1);
        Terms segmentTerms2 = ctx.reader().terms(field2);

        // TODO in this PR itself in coming commits:
        // 1/ add check for fields cardinality - this might be ineffective for very high cardinality
        // 2/ check for filter applied or not as default implementation might be resolving it as part of aggregation

        TermsEnum segmentTermsEnum1 = segmentTerms1.iterator();

        while (segmentTermsEnum1.next() != null) {
            TermsEnum segmentTermsEnum2 = segmentTerms2.iterator();

            while (segmentTermsEnum2.next() != null) {

                PostingsEnum postings1 = segmentTermsEnum1.postings(null);
                postings1.nextDoc();

                PostingsEnum postings2 = segmentTermsEnum2.postings(null);
                postings2.nextDoc();

                int bucketCount = 0;

                while (postings1.docID() != PostingsEnum.NO_MORE_DOCS && postings2.docID() != PostingsEnum.NO_MORE_DOCS) {

                    // Count of intersecting docs to get number of docs in each bucket
                    if (postings1.docID() == postings2.docID()) {
                        bucketCount++;
                        postings1.nextDoc();
                        postings2.nextDoc();
                    } else if (postings1.docID() < postings2.docID()) {
                        postings1.advance(postings2.docID());
                    } else {
                        postings2.advance(postings1.docID());
                    }
                }

                // For a key formed by value of t1 & a value of t2, create a composite key, convert it to byte ref and then update the
                // ordinal data with count computed above
                // The ordinal data is used to collect the sub-aggregations for each composite key
                // The composite key is used to collect the buckets for each composite key
                BytesRef v1 = segmentTermsEnum1.term();
                BytesRef v2 = segmentTermsEnum2.term();

                TermValue<BytesRef> termValue1 = new TermValue<>(v1, TermValue.BYTES_REF_WRITER);
                TermValue<BytesRef> termValue2 = new TermValue<>(v2, TermValue.BYTES_REF_WRITER);

                final BytesStreamOutput scratch = new BytesStreamOutput();
                scratch.writeVInt(2); // number of fields per composite key
                termValue1.writeTo(scratch);
                termValue2.writeTo(scratch);
                BytesRef compositeKeyBytesRef = scratch.bytes().toBytesRef();  // composite key formed
                scratch.close();

                long bucketOrd = bucketOrds.add(0, compositeKeyBytesRef);
                if (bucketOrd < 0) {
                    bucketOrd = -1 - bucketOrd;
                }
                incrementBucketDocCount(bucketOrd, bucketCount);
            }
        }

        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                throw new CollectionTerminatedException();
            }
        };

    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {

        LeafBucketCollector optimizedCollector = this.getTermFrequencies(ctx);

        if (optimizedCollector != null) {
            logger.info("optimization used");
            return optimizedCollector;
        }

        logger.info("optimization not not used");

        MultiTermsValuesSourceCollector collector = multiTermsValue.getValues(ctx);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                for (BytesRef compositeKey : collector.apply(doc)) {
                    long bucketOrd = bucketOrds.add(owningBucketOrd, compositeKey);
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
        return collectMode == SubAggCollectionMode.BREADTH_FIRST && !aggsUsedForSorting.contains(aggregator);
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
                for (BytesRef compositeKey : collector.apply(docId)) {
                    bucketOrds.add(owningBucketOrd, compositeKey);
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
        List<BytesRef> apply(int doc) throws IOException;
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

        public MultiTermsValuesSourceCollector getValues(LeafReaderContext ctx) throws IOException {
            List<InternalValuesSourceCollector> collectors = new ArrayList<>();
            for (InternalValuesSource valuesSource : valuesSources) {
                collectors.add(valuesSource.apply(ctx));
            }
            return new MultiTermsValuesSourceCollector() {
                @Override
                public List<BytesRef> apply(int doc) throws IOException {
                    List<List<TermValue<?>>> collectedValues = new ArrayList<>();
                    for (InternalValuesSourceCollector collector : collectors) {
                        collectedValues.add(collector.apply(doc));
                    }
                    List<BytesRef> result = new ArrayList<>();
                    scratch.seek(0);
                    scratch.writeVInt(collectors.size()); // number of fields per composite key
                    cartesianProduct(result, scratch, collectedValues, 0);
                    return result;
                }

                /**
                 * Cartesian product using depth first search.
                 *
                 * <p>
                 * Composite keys are encoded to a {@link BytesRef} in a format compatible with {@link StreamOutput::writeGenericValue},
                 * but reuses the encoding of the shared prefixes from the previous levels to avoid wasteful work.
                 */
                private void cartesianProduct(
                    List<BytesRef> compositeKeys,
                    BytesStreamOutput scratch,
                    List<List<TermValue<?>>> collectedValues,
                    int index
                ) throws IOException {
                    if (collectedValues.size() == index) {
                        compositeKeys.add(BytesRef.deepCopyOf(scratch.bytes().toBytesRef()));
                        return;
                    }

                    long position = scratch.position();
                    for (TermValue<?> value : collectedValues.get(index)) {
                        value.writeTo(scratch); // encode the value
                        cartesianProduct(compositeKeys, scratch, collectedValues, index + 1); // dfs
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
                        BytesRef copy = BytesRef.deepCopyOf(bytes);
                        termValues.add(TermValue.of(copy));
                        previous = copy;
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
