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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.common.Nullable;
import org.opensearch.common.Rounding;
import org.opensearch.common.lease.Releasables;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.filterrewrite.DateHistogramAggregatorBridge;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.StarTreeTraversalUtil;
import org.opensearch.search.startree.filter.DimensionFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.search.aggregations.bucket.filterrewrite.DateHistogramAggregatorBridge.segmentMatchAll;
import static org.opensearch.search.startree.StarTreeQueryHelper.getSupportedStarTree;

/**
 * An aggregator for date values. Every date is rounded down using a configured
 * {@link Rounding}.
 *
 * @see Rounding
 *
 * @opensearch.internal
 */
class DateHistogramAggregator extends BucketsAggregator implements SizedBucketAggregator, StarTreePreComputeCollector {
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
    private final LongKeyedBucketOrds bucketOrds;
    private final String starTreeDateDimension;
    private boolean starTreeDateRoundingRequired = true;

    private final FilterRewriteOptimizationContext filterRewriteOptimizationContext;
    public final String fieldName;

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

        DateHistogramAggregatorBridge bridge = new DateHistogramAggregatorBridge() {
            @Override
            protected boolean canOptimize() {
                return canOptimize(valuesSourceConfig, rounding);
            }

            @Override
            protected void prepare() throws IOException {
                buildRanges(context);
            }

            @Override
            protected Rounding getRounding(long low, long high) {
                return rounding;
            }

            @Override
            protected Rounding.Prepared getRoundingPrepared() {
                return preparedRounding;
            }

            @Override
            protected long[] processHardBounds(long[] bounds) {
                return super.processHardBounds(bounds, hardBounds);
            }

            @Override
            protected Function<Long, Long> bucketOrdProducer() {
                return (key) -> bucketOrds.add(0, preparedRounding.round((long) key));
            }
        };
        filterRewriteOptimizationContext = new FilterRewriteOptimizationContext(bridge, parent, subAggregators.length, context);
        this.fieldName = (valuesSource instanceof ValuesSource.Numeric.FieldData)
            ? ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName()
            : null;
        this.starTreeDateDimension = (context.getQueryShardContext().getStarTreeQueryContext() != null)
            ? fetchStarTreeCalendarUnit()
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
            StarTreeBucketCollector starTreeBucketCollector = getStarTreeBucketCollector(ctx, supportedStarTree, null);
            StarTreeQueryHelper.preComputeBucketsWithStarTree(starTreeBucketCollector);
            return true;
        }
        return filterRewriteOptimizationContext.tryOptimize(ctx, this::incrementBucketDocCount, segmentMatchAll(context, ctx));
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        SortedNumericDocValues values = valuesSource.longValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
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

    private String fetchStarTreeCalendarUnit() {
        if (this.rounding.unit() == null) {
            return null;
        }

        CompositeDataCubeFieldType compositeMappedFieldType = (CompositeDataCubeFieldType) context.mapperService()
            .getCompositeFieldTypes()
            .iterator()
            .next();
        DateDimension starTreeDateDimension = (DateDimension) compositeMappedFieldType.getDimensions()
            .stream()
            .filter(dim -> dim.getField().equals(fieldName))
            .findFirst() // Get the first matching time dimension
            .orElseThrow(() -> new AssertionError(String.format(Locale.ROOT, "Date dimension '%s' not found", fieldName)));

        DateTimeUnitAdapter dateTimeUnitRounding = new DateTimeUnitAdapter(this.rounding.unit());
        DateTimeUnitRounding rounding = starTreeDateDimension.findClosestValidInterval(dateTimeUnitRounding);
        String dimensionName = fieldName + "_" + rounding.shortName();
        if (rounding.shortName().equals(this.rounding.unit().shortName())) {
            this.starTreeDateRoundingRequired = false;
        }
        return dimensionName;
    }

    @Override
    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException {
        assert parentCollector == null;
        StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(ctx, starTree);
        SortedNumericStarTreeValuesIterator valuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
            .getDimensionValuesIterator(starTreeDateDimension);
        SortedNumericStarTreeValuesIterator docCountsIterator = StarTreeQueryHelper.getDocCountsIterator(starTreeValues, starTree);

        return new StarTreeBucketCollector(
            starTreeValues,
            StarTreeTraversalUtil.getStarTreeResult(
                starTreeValues,
                StarTreeQueryHelper.mergeDimensionFilterIfNotExists(
                    context.getQueryShardContext().getStarTreeQueryContext().getBaseQueryStarTreeFilter(),
                    starTreeDateDimension,
                    List.of(DimensionFilter.MATCH_ALL_DEFAULT)
                ),
                context
            )
        ) {
            @Override
            public void setSubCollectors() throws IOException {
                for (Aggregator aggregator : subAggregators) {
                    this.subCollectors.add(((StarTreePreComputeCollector) aggregator).getStarTreeBucketCollector(ctx, starTree, this));
                }
            }

            @Override
            public void collectStarTreeEntry(int starTreeEntry, long owningBucketOrd) throws IOException {
                if (!valuesIterator.advanceExact(starTreeEntry)) {
                    return;
                }

                for (int i = 0, count = valuesIterator.entryValueCount(); i < count; i++) {
                    long dimensionValue = starTreeDateRoundingRequired
                        ? preparedRounding.round(valuesIterator.nextValue())
                        : valuesIterator.nextValue();

                    if (docCountsIterator.advanceExact(starTreeEntry)) {
                        long metricValue = docCountsIterator.nextValue();
                        long bucketOrd = bucketOrds.add(owningBucketOrd, dimensionValue);
                        collectStarTreeBucket(this, metricValue, bucketOrd, starTreeEntry);
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
        filterRewriteOptimizationContext.populateDebugInfo(add);
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
}
