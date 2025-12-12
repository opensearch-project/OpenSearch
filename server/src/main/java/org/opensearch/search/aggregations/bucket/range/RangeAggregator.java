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

package org.opensearch.search.aggregations.bucket.range;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.ObjectParser.ValueType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.NonCollectingAggregator;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.filterrewrite.RangeAggregatorBridge;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.MatchAllFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.search.aggregations.bucket.filterrewrite.AggregatorBridge.segmentMatchAll;
import static org.opensearch.search.startree.StarTreeQueryHelper.getSupportedStarTree;

/**
 * Aggregate all docs that match given ranges.
 *
 * @opensearch.internal
 */
public class RangeAggregator extends BucketsAggregator implements StarTreePreComputeCollector {

    public static final ParseField RANGES_FIELD = new ParseField("ranges");
    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public final String fieldName;

    /**
     * Range for the range aggregator
     *
     * @opensearch.internal
     */
    public static class Range implements Writeable, ToXContentObject {
        public static final ParseField KEY_FIELD = new ParseField("key");
        public static final ParseField FROM_FIELD = new ParseField("from");
        public static final ParseField TO_FIELD = new ParseField("to");

        protected final String key;
        protected final double from;
        protected final String fromAsStr;
        protected final double to;
        protected final String toAsStr;

        /**
         * Build the range. Generally callers should prefer
         * {@link Range#Range(String, Double, Double)} or
         * {@link Range#Range(String, String, String)}. If you
         * <strong>must</strong> call this know that consumers prefer
         * {@code from} and {@code to} parameters if they are non-null
         * and finite. Otherwise they parse from {@code fromrStr} and
         * {@code toStr}.
         */
        public Range(String key, Double from, String fromAsStr, Double to, String toAsStr) {
            this.key = key;
            this.from = from == null ? Double.NEGATIVE_INFINITY : from;
            this.fromAsStr = fromAsStr;
            this.to = to == null ? Double.POSITIVE_INFINITY : to;
            this.toAsStr = toAsStr;
        }

        public Range(String key, Double from, Double to) {
            this(key, from, null, to, null);
        }

        public Range(String key, String from, String to) {
            this(key, null, from, null, to);
        }

        /**
         * Read from a stream.
         */
        public Range(StreamInput in) throws IOException {
            key = in.readOptionalString();
            fromAsStr = in.readOptionalString();
            toAsStr = in.readOptionalString();
            from = in.readDouble();
            to = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeOptionalString(fromAsStr);
            out.writeOptionalString(toAsStr);
            out.writeDouble(from);
            out.writeDouble(to);
        }

        public double getFrom() {
            return this.from;
        }

        public double getTo() {
            return this.to;
        }

        public String getFromAsString() {
            return this.fromAsStr;
        }

        public String getToAsString() {
            return this.toAsStr;
        }

        public String getKey() {
            return this.key;
        }

        boolean matches(double value) {
            return value >= from && value < to;
        }

        @Override
        public String toString() {
            return "[" + from + " to " + to + ")";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(KEY_FIELD.getPreferredName(), key);
            }
            if (Double.isFinite(from)) {
                builder.field(FROM_FIELD.getPreferredName(), from);
            }
            if (Double.isFinite(to)) {
                builder.field(TO_FIELD.getPreferredName(), to);
            }
            if (fromAsStr != null) {
                builder.field(FROM_FIELD.getPreferredName(), fromAsStr);
            }
            if (toAsStr != null) {
                builder.field(TO_FIELD.getPreferredName(), toAsStr);
            }
            builder.endObject();
            return builder;
        }

        public static final ConstructingObjectParser<Range, Void> PARSER = new ConstructingObjectParser<>("range", arg -> {
            String key = (String) arg[0];
            Object from = arg[1];
            Object to = arg[2];
            Double fromDouble = from instanceof Number ? ((Number) from).doubleValue() : null;
            Double toDouble = to instanceof Number ? ((Number) to).doubleValue() : null;
            String fromStr = from instanceof String ? (String) from : null;
            String toStr = to instanceof String ? (String) to : null;
            return new Range(key, fromDouble, fromStr, toDouble, toStr);
        });

        static {
            PARSER.declareField(optionalConstructorArg(), (p, c) -> p.text(), KEY_FIELD, ValueType.DOUBLE); // DOUBLE supports string and
                                                                                                            // number
            ContextParser<Void, Object> fromToParser = (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return p.text();
                }
                if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.doubleValue();
                }
                return null;
            };
            // DOUBLE_OR_NULL accepts String, Number, and null
            PARSER.declareField(optionalConstructorArg(), fromToParser, FROM_FIELD, ValueType.DOUBLE_OR_NULL);
            PARSER.declareField(optionalConstructorArg(), fromToParser, TO_FIELD, ValueType.DOUBLE_OR_NULL);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, from, fromAsStr, to, toAsStr);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Range other = (Range) obj;
            return Objects.equals(key, other.key)
                && Objects.equals(from, other.from)
                && Objects.equals(fromAsStr, other.fromAsStr)
                && Objects.equals(to, other.to)
                && Objects.equals(toAsStr, other.toAsStr);
        }
    }

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;
    final Range[] ranges;
    final boolean keyed;
    final InternalRange.Factory rangeFactory;

    final double[] maxTo;

    private final FilterRewriteOptimizationContext filterRewriteOptimizationContext;

    public RangeAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Numeric valuesSource,
        DocValueFormat format,
        InternalRange.Factory rangeFactory,
        Range[] ranges,
        boolean keyed,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata,
        ValuesSourceConfig config
    ) throws IOException {
        super(name, factories, context, parent, cardinality.multiply(ranges.length), metadata);
        assert valuesSource != null;
        this.valuesSource = valuesSource;
        this.format = format;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        this.ranges = ranges; // already sorted by the range.from and range.to

        maxTo = new double[this.ranges.length];
        maxTo[0] = this.ranges[0].to;
        for (int i = 1; i < this.ranges.length; ++i) {
            maxTo[i] = Math.max(this.ranges[i].to, maxTo[i - 1]);
        }

        RangeAggregatorBridge bridge = new RangeAggregatorBridge() {
            @Override
            protected boolean canOptimize() {
                return canOptimize(config, ranges);
            }

            @Override
            protected void prepare() {
                buildRanges(ranges);
            }

            @Override
            protected Function<Object, Long> bucketOrdProducer() {
                return (activeIndex) -> subBucketOrdinal(0, (int) activeIndex);
            }
        };
        filterRewriteOptimizationContext = new FilterRewriteOptimizationContext(bridge, parent, subAggregators.length, context);
        this.fieldName = (valuesSource instanceof ValuesSource.Numeric.FieldData)
            ? ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName()
            : null;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        CompositeIndexFieldInfo supportedStarTree = getSupportedStarTree(this.context.getQueryShardContext());
        if (supportedStarTree != null) {
            preComputeWithStarTree(ctx, supportedStarTree);
            return true;
        }

        try {
            leafCollectorMode = LeafCollectionMode.FILTER_REWRITE;
            return segmentMatchAll(context, ctx)
                && filterRewriteOptimizationContext.tryOptimize(ctx, this::incrementBucketDocCount, true, collectableSubAggregators);
        } finally {
            leafCollectorMode = LeafCollectionMode.NORMAL;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0, lo = 0; i < valuesCount; ++i) {
                        final double value = values.nextValue();
                        lo = collect(doc, value, bucket, lo);
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

            private int collect(int doc, double value, long owningBucketOrdinal, int lowBound) throws IOException {
                MatchedRange range = new MatchedRange(ranges, lowBound, value, maxTo);
                for (int i = range.startLo; i <= range.endHi; ++i) {
                    if (ranges[i].matches(value)) {
                        collectBucket(sub, doc, subBucketOrdinal(owningBucketOrdinal, i));
                    }
                }
                return range.endHi + 1;
            }
        };
    }

    private void preComputeWithStarTree(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException {
        StarTreeBucketCollector starTreeBucketCollector = getStarTreeBucketCollector(ctx, starTree, null);
        FixedBitSet matchingDocsBitSet = starTreeBucketCollector.getMatchingDocsBitSet();

        int numBits = matchingDocsBitSet.length();

        if (numBits > 0) {
            for (int bit = matchingDocsBitSet.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = (bit + 1 < numBits)
                ? matchingDocsBitSet.nextSetBit(bit + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {
                starTreeBucketCollector.collectStarTreeEntry(bit, 0);
            }
        }
    }

    @Override
    public List<DimensionFilter> getDimensionFilters() {
        return StarTreeQueryHelper.collectDimensionFilters(new MatchAllFilter(fieldName), subAggregators);
    }

    @Override
    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException {
        StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(ctx, starTree);

        // TODO: Evaluate optimizing StarTree traversal filter with specific ranges instead of MATCH_ALL_DEFAULT
        return new StarTreeBucketCollector(
            starTreeValues,
            parentCollector == null ? StarTreeQueryHelper.getStarTreeResult(starTreeValues, context, getDimensionFilters()) : null
        ) {
            @Override
            public void setSubCollectors() throws IOException {
                for (Aggregator aggregator : subAggregators) {
                    this.subCollectors.add(
                        ((StarTreePreComputeCollector) aggregator.unwrapAggregator()).getStarTreeBucketCollector(ctx, starTree, this)
                    );
                }
            }

            SortedNumericStarTreeValuesIterator valuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
                .getDimensionValuesIterator(fieldName);

            String metricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                starTree.getField(),
                "_doc_count",
                MetricStat.DOC_COUNT.getTypeName()
            );

            SortedNumericStarTreeValuesIterator docCountsIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
                .getMetricValuesIterator(metricName);

            @Override
            public void collectStarTreeEntry(int starTreeEntry, long owningBucketOrd) throws IOException {
                if (!valuesIterator.advanceExact(starTreeEntry)) {
                    return;
                }

                for (int i = 0, count = valuesIterator.entryValueCount(); i < count; i++) {
                    long dimensionLongValue = valuesIterator.nextValue();
                    double dimensionValue;

                    // Only numeric & floating points are supported as of now in star-tree
                    // TODO: Add support for isBigInteger() when it gets supported in star-tree
                    if (valuesSource.isFloatingPoint()) {
                        dimensionValue = ((NumberFieldMapper.NumberFieldType) context.mapperService().fieldType(fieldName)).toDoubleValue(
                            dimensionLongValue
                        );
                    } else {
                        dimensionValue = dimensionLongValue;
                    }

                    MatchedRange matchedRange = new MatchedRange(ranges, 0, dimensionValue, maxTo);
                    if (matchedRange.startLo > matchedRange.endHi) {
                        continue; // No matching range
                    }

                    if (docCountsIterator.advanceExact(starTreeEntry)) {
                        long metricValue = docCountsIterator.nextValue();
                        for (int j = matchedRange.startLo; j <= matchedRange.endHi; ++j) {
                            if (ranges[j].matches(dimensionValue)) {
                                long bucketOrd = subBucketOrdinal(owningBucketOrd, j);
                                collectStarTreeBucket(this, metricValue, bucketOrd, starTreeEntry);
                            }
                        }
                    }
                }
            }
        };
    }

    private long subBucketOrdinal(long owningBucketOrdinal, int rangeOrd) {
        return owningBucketOrdinal * ranges.length + rangeOrd;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForFixedBucketCount(
            owningBucketOrds,
            ranges.length,
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                checkCancelled();
                Range range = ranges[offsetInOwningOrd];
                return rangeFactory.createBucket(range.key, range.from, range.to, docCount, subAggregationResults, keyed, format);
            },
            buckets -> rangeFactory.create(name, buckets, format, keyed, metadata())
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<org.opensearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            org.opensearch.search.aggregations.bucket.range.Range.Bucket bucket = rangeFactory.createBucket(
                range.key,
                range.from,
                range.to,
                0,
                subAggs,
                keyed,
                format
            );
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, format, keyed, metadata());
    }

    static class MatchedRange {
        int startLo, endHi;

        MatchedRange(RangeAggregator.Range[] ranges, int lowBound, double value, double[] maxTo) {
            computeMatchingRange(ranges, lowBound, value, maxTo);
        }

        private void computeMatchingRange(RangeAggregator.Range[] ranges, int lowBound, double value, double[] maxTo) {
            int lo = lowBound, hi = ranges.length - 1;
            int mid = (lo + hi) >>> 1;

            while (lo <= hi) {
                if (value < ranges[mid].from) {
                    hi = mid - 1;
                } else if (value >= maxTo[mid]) {
                    lo = mid + 1;
                } else {
                    break;
                }
                mid = (lo + hi) >>> 1;
            }
            if (lo > hi) {
                this.startLo = lo;
                this.endHi = lo - 1;
                return;
            }

            // binary search the lower bound
            int startLo = lo, startHi = mid;
            while (startLo <= startHi) {
                int startMid = (startLo + startHi) >>> 1;
                if (value >= maxTo[startMid]) {
                    startLo = startMid + 1;
                } else {
                    startHi = startMid - 1;
                }
            }

            // binary search the upper bound
            int endLo = mid, endHi = hi;
            while (endLo <= endHi) {
                int endMid = (endLo + endHi) >>> 1;
                if (value < ranges[endMid].from) {
                    endHi = endMid - 1;
                } else {
                    endLo = endMid + 1;
                }
            }

            assert startLo == lowBound || value >= maxTo[startLo - 1];
            assert endHi == ranges.length - 1 || value < ranges[endHi + 1].from;

            this.startLo = startLo;
            this.endHi = endHi;
        }
    }

    /**
     * Unmapped range
     *
     * @opensearch.internal
     */
    public static class Unmapped<R extends RangeAggregator.Range> extends NonCollectingAggregator {

        private final R[] ranges;
        private final boolean keyed;
        private final InternalRange.Factory factory;
        private final DocValueFormat format;

        public Unmapped(
            String name,
            AggregatorFactories factories,
            R[] ranges,
            boolean keyed,
            DocValueFormat format,
            SearchContext context,
            Aggregator parent,
            InternalRange.Factory factory,
            Map<String, Object> metadata
        ) throws IOException {

            super(name, context, parent, factories, metadata);
            this.ranges = ranges;
            this.keyed = keyed;
            this.format = format;
            this.factory = factory;
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            InternalAggregations subAggs = buildEmptySubAggregations();
            List<org.opensearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
            for (Range range : ranges) {
                buckets.add(factory.createBucket(range.key, range.from, range.to, 0, subAggs, keyed, format));
            }
            return factory.create(name, buckets, format, keyed, metadata());
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        filterRewriteOptimizationContext.populateDebugInfo(add);
    }
}
