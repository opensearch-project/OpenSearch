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

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Nullable;
import org.opensearch.common.Rounding;
import org.opensearch.common.lease.Releasables;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 *
 * @opensearch.internal
 */
class DateHistogramAggregator extends BucketsAggregator implements SizedBucketAggregator {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Rounding rounding;
    /**
     * The rounding prepared for rewriting the data in the shard.
     */
    private final Rounding.Prepared preparedRounding;
    private final BucketOrder order;
    private final boolean keyed;

    private final long minDocCount;
    private final LongBounds extendedBounds;
    private final LongBounds hardBounds;

    private Weight[] filters = null;

    private final LongKeyedBucketOrds bucketOrds;

    DateHistogramAggregator(
        String name,
        AggregatorFactories factories,
        Rounding rounding,
        Rounding.Prepared preparedRounding,
        BucketOrder order,
        boolean keyed,
        long minDocCount,
        @Nullable LongBounds extendedBounds,
        @Nullable LongBounds hardBounds,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggregationContext, parent, CardinalityUpperBound.MANY, metadata);
        this.rounding = rounding;
        this.preparedRounding = preparedRounding;
        this.order = order;
        order.validate(this);
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.hardBounds = hardBounds;
        // TODO: Stop using null here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.formatter = valuesSourceConfig.format();

        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);

        // Create the filters for fast aggregation only if the query is instance
        // of point range query and there aren't any parent/sub aggregations
        if (parent() == null && subAggregators.length == 0) {
            // TODO: Is there better way of getting aggregation field? Can this cause NPE?
            final String fieldName = (((ValuesSource.Numeric.FieldData) valuesSource).indexFieldData).getFieldName();
            if (context.query() instanceof IndexOrDocValuesQuery) {
                final IndexOrDocValuesQuery q = (IndexOrDocValuesQuery) context.query();
                if (q.getIndexQuery() instanceof PointRangeQuery) {
                    final PointRangeQuery prq = (PointRangeQuery) q.getIndexQuery();
                    // Ensure that the query and aggregation are on the same field
                    if (valuesSource != null && prq.getField().equals(fieldName)) {
                        long low = NumericUtils.sortableBytesToLong(prq.getLowerPoint(), 0);
                        long high = NumericUtils.sortableBytesToLong(prq.getUpperPoint(), 0);
                        createFilterForAggregations(fieldName, low, high);
                    }
                }
            }
        }
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        // Need to be declared as final and array for usage within the
        // LeafCollector subclass later
        final boolean[] useOpt = new boolean[1];
        useOpt[0] = true;

        SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                // Try fast filter aggregation if the filters have been created
                // Skip if tried before and gave incorrect/incomplete results
                if (useOpt[0] && filters != null) {
                    useOpt[0] = tryFastFilterAggregation(ctx, owningBucketOrd);
                }

                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRounding.round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        if (hardBounds == null || hardBounds.contain(rounded)) {
                            long bucketOrd = bucketOrds.add(owningBucketOrd, rounded);
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                        }
                        previousRounded = rounded;
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForVariableBuckets(owningBucketOrds, bucketOrds, (bucketValue, docCount, subAggregationResults) -> {
            return new InternalDateHistogram.Bucket(bucketValue, docCount, keyed, formatter, subAggregationResults);
        }, (owningBucketOrd, buckets) -> {
            // the contract of the histogram aggregation is that shards must return buckets ordered by key in ascending order
            CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

            // value source will be null for unmapped fields
            // Important: use `rounding` here, not `shardRounding`
            InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
                ? new InternalDateHistogram.EmptyBucketInfo(rounding.withoutOffset(), buildEmptySubAggregations(), extendedBounds)
                : null;
            return new InternalDateHistogram(
                name,
                buckets,
                order,
                minDocCount,
                rounding.offset(),
                emptyBucketInfo,
                formatter,
                keyed,
                metadata()
            );
        });
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalDateHistogram.EmptyBucketInfo emptyBucketInfo = minDocCount == 0
            ? new InternalDateHistogram.EmptyBucketInfo(rounding, buildEmptySubAggregations(), extendedBounds)
            : null;
        return new InternalDateHistogram(
            name,
            Collections.emptyList(),
            order,
            minDocCount,
            rounding.offset(),
            emptyBucketInfo,
            formatter,
            keyed,
            metadata()
        );
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("total_buckets", bucketOrds.size());
    }

    /**
     * @return the size of the bucket in specified units, or 1.0 if unitSize is null
     */
    @Override
    public double bucketSize(long bucket, Rounding.DateTimeUnit unitSize) {
        if (unitSize != null) {
            return preparedRounding.roundingSize(bucketOrds.get(bucket), unitSize);
        } else {
            return 1.0;
        }
    }

    private boolean tryFastFilterAggregation(LeafReaderContext ctx, long owningBucketOrd) throws IOException {
        final int[] counts = new int[filters.length];
        int i;
        for (i = 0; i < filters.length; i++) {
            counts[i] = filters[i].count(ctx);
            if (counts[i] == -1) {
                // Cannot use the optimization if any of the counts
                // is -1 indicating the segment might have deleted documents
                return false;
            }
        }

        for (i = 0; i < filters.length; i++) {
            long bucketOrd = bucketOrds.add(
                owningBucketOrd,
                preparedRounding.round(NumericUtils.sortableBytesToLong(((PointRangeQuery) filters[i].getQuery()).getLowerPoint(), 0))
            );
            if (bucketOrd < 0) { // already seen
                bucketOrd = -1 - bucketOrd;
            }
            incrementBucketDocCount(bucketOrd, counts[i]);
        }
        throw new CollectionTerminatedException();
    }

    private boolean isUTCTimeZone(final ZoneId zoneId) {
        return "Z".equals(zoneId.getDisplayName(TextStyle.FULL, Locale.ENGLISH));
    }

    private void createFilterForAggregations(String field, long low, long high) throws IOException {
        long interval = Long.MAX_VALUE;
        if (rounding instanceof Rounding.TimeUnitRounding) {
            interval = (((Rounding.TimeUnitRounding) rounding).unit).extraLocalOffsetLookup();
            if (!isUTCTimeZone(((Rounding.TimeUnitRounding) rounding).timeZone)) {
                // Fast filter aggregation cannot be used if it needs time zone rounding
                return;
            }
        } else if (rounding instanceof Rounding.TimeIntervalRounding) {
            interval = ((Rounding.TimeIntervalRounding) rounding).interval;
            if (!isUTCTimeZone(((Rounding.TimeIntervalRounding) rounding).timeZone)) {
                // Fast filter aggregation cannot be used if it needs time zone rounding
                return;
            }
        }

        // Return if the interval ratio could not be figured out correctly
        if (interval == Long.MAX_VALUE) return;

        // Calculate the number of buckets using range and interval
        long roundedLow = preparedRounding.round(low);
        int bucketCount = 0;
        while (roundedLow < high) {
            bucketCount++;
            roundedLow += interval;
            // Below rounding is needed as the interval could return in
            // non-rounded values for something like calendar month
            roundedLow = preparedRounding.round(roundedLow);
        }

        if (bucketCount > 0) {
            int i = 0;
            filters = new Weight[bucketCount];
            while (i < bucketCount) {
                byte[] lower = new byte[8];
                NumericUtils.longToSortableBytes(low, lower, 0);
                byte[] upper = new byte[8];
                // Calculate the upper bucket
                roundedLow = preparedRounding.round(low);
                roundedLow = preparedRounding.round(roundedLow + interval);
                // Subtract -1 if the minimum is roundedLow as roundedLow itself
                // is included in the next bucket
                NumericUtils.longToSortableBytes(Math.min(roundedLow - 1, high), upper, 0);
                filters[i++] = context.searcher().createWeight(new PointRangeQuery(field, lower, upper, 1) {
                    @Override
                    protected String toString(int dimension, byte[] value) {
                        return null;
                    }
                }, ScoreMode.COMPLETE_NO_SCORES, 1);
            }
        }
    }
}
