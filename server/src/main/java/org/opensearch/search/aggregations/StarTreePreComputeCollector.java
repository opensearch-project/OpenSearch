/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.Timer;
import org.opensearch.search.profile.aggregation.startree.StarTreeAggregationTimingType;
import org.opensearch.search.profile.aggregation.startree.StarTreeProfileBreakdown;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.DimensionFilter;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/**
 * This interface is used to pre-compute the star tree bucket collector for each segment/leaf.
 * It is utilized by parent aggregation to retrieve a StarTreeBucketCollector which can be used to
 * pre-compute the associated aggregation along with its parent pre-computation using star-tree
 *
 * @opensearch.internal
 */
public interface StarTreePreComputeCollector {
    /**
     * Get the star tree bucket collector for the specified segment/leaf
     */
    StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException;

    /**
     * Returns the list of dimensions filters involved in this aggregation, which are required for
     * merging dimension filters during StarTree precomputation. This is specifically needed
     * for nested bucket aggregations to ensure that the correct dimensions are considered when
     * constructing or merging filters during StarTree traversal.
     * For metric aggregations, there is no need to specify dimensions since they operate
     * purely on values within the buckets formed by parent bucket aggregations.
     *
     * @return List of dimension field names involved in the aggregation.
     */
    default List<DimensionFilter> getDimensionFilters() {
        return null;
    }

    /**
     * If this aggregator supports star tree precomputation, this method will represent the first phase of scanning
     * the star tree index and return the matching values to be added to the buckets. Can be overriden by subclasses
     * supporting star tree precomputation.
     * @param context the search context for the aggregator
     * @param valuesSource the data for values in this aggregator
     * @param ctx the context for the given segment
     * @param starTree field info details of composite index
     * @param metric type of metric used for aggregation (e.g. sum, max, min, etc...)
     * @return
     * @throws IOException
     */
    default FixedBitSet scanStarTree(
        SearchContext context,
        ValuesSource valuesSource,
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        String metric
    ) throws IOException {
        return StarTreeQueryHelper.scanStarTree(context, valuesSource, ctx, starTree, metric);
    }

    /**
     * If this aggregator supports star tree precomputation, this method will represent the first phase of scanning
     * the star tree index and return the matching values to be added to the buckets and also profile the time it
     * takes to complete this phase. Can be overriden by subclasses supporting star tree precomputation.
     * @param context the search context for the aggregator
     * @param valuesSource the data for values in this aggregator
     * @param ctx the context for the given segment
     * @param starTree field info details of composite index
     * @param metric type of metric used for aggregation (e.g. sum, max, min, etc...)
     * @param profileBreakdown the profiling breakdown to record the time taken for this phase
     * @return
     * @throws IOException
     */
    default FixedBitSet scanStarTreeProfiling(
        SearchContext context,
        ValuesSource valuesSource,
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        String metric,
        StarTreeProfileBreakdown profileBreakdown
    ) throws IOException {
        assert profileBreakdown != null;
        Timer timer = profileBreakdown.getTimer(StarTreeAggregationTimingType.SCAN_STAR_TREE_SEGMENTS);
        timer.start();
        try {
            return scanStarTree(context, valuesSource, ctx, starTree, metric);
        } finally {
            timer.stop();
        }
    }

    /**
     * For bucket aggregations, this method will return a StarTreeBucketCollector as part of the phase of
     * scanning the star tree for matching entries to be added to the buckets and defining which buckets the
     * collected documents should go into. Profiles the time it
     * takes to complete this phase. Can be overriden by subclasses
     * @param ctx the context for the given segment
     * @param starTree field info details of composite index
     * @param parent the {@link StarTreeBucketCollector} for the parent aggregator if any
     * @param profileBreakdown the profiling breakdown to record the time taken for this phase
     * @return
     * @throws IOException
     */
    default StarTreeBucketCollector getStarTreeBucketCollectorProfiling(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parent,
        StarTreeProfileBreakdown profileBreakdown
    ) throws IOException {
        assert profileBreakdown != null;
        Timer timer = profileBreakdown.getTimer(StarTreeAggregationTimingType.SCAN_STAR_TREE_SEGMENTS);
        timer.start();
        try {
            return getStarTreeBucketCollector(ctx, starTree, parent);
        } finally {
            timer.stop();
        }
    }

    /**
     * After scanning the star tree, this method will apply the aggregation operation to create the buckets.
     * Can be overriden by subclasses supporting star tree precomputation.
     * @param context the search context for the aggregator
     * @param valuesSource the data for values in this aggregator
     * @param ctx the context for the given segment
     * @param starTree field info details of composite index
     * @param metric type of metric used for aggregation (e.g. sum, max, min, etc...)
     * @param valueConsumer consumer to accept the values in documents matching the conditions
     * @param finalConsumer consumer to set the final answer after iterating over all values
     * @param filteredValues bitset for document ids matching the star tree query
     * @throws IOException
     */
    default void buildBucketsFromStarTree(
        SearchContext context,
        ValuesSource valuesSource,
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        String metric,
        Consumer<Long> valueConsumer,
        Runnable finalConsumer,
        FixedBitSet filteredValues
    ) throws IOException {
        StarTreeQueryHelper.buildBucketsFromStarTree(
            context,
            valuesSource,
            ctx,
            starTree,
            metric,
            valueConsumer,
            finalConsumer,
            filteredValues
        );
    }

    /**
     * After scanning the star tree, this method will apply the aggregation operation to create the buckets
     * and also profile the time it takes to complete this phase. Can be overriden by subclasses
     * supporting star tree precomputation.
     * @param context the search context for the aggregator
     * @param valuesSource the data for values in this aggregator
     * @param ctx the context for the given segment
     * @param starTree field info details of composite index
     * @param metric type of metric used for aggregation (e.g. sum, max, min, etc...)
     * @param valueConsumer consumer to accept the values in documents matching the conditions
     * @param finalConsumer consumer to set the final answer after iterating over all values
     * @param filteredValues bitset for document ids matching the star tree query
     * @param profileBreakdown the profiling breakdown to record the time taken for this phase
     * @throws IOException
     */
    default void buildBucketsFromStarTreeProfiling(
        SearchContext context,
        ValuesSource valuesSource,
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        String metric,
        Consumer<Long> valueConsumer,
        Runnable finalConsumer,
        FixedBitSet filteredValues,
        StarTreeProfileBreakdown profileBreakdown
    ) throws IOException {
        Timer timer = profileBreakdown.getTimer(StarTreeAggregationTimingType.BUILD_BUCKETS_FROM_STAR_TREE);
        timer.start();
        try {
            buildBucketsFromStarTree(context, valuesSource, ctx, starTree, metric, valueConsumer, finalConsumer, filteredValues);
        } finally {
            timer.stop();
        }
    }

    /**
     * This method applies the StarTreeBucketCollector to collect the documents for bucket aggregations
     * @param starTreeBucketCollector the star tree bucket collector for adding the documents
     * @throws IOException
     */
    default void preComputeBucketsWithStarTree(StarTreeBucketCollector starTreeBucketCollector) throws IOException {
        StarTreeQueryHelper.preComputeBucketsWithStarTree(starTreeBucketCollector);
    }

    /**
     * This method applies the StarTreeBucketCollector to collect the documents for bucket aggregations
     * @param starTreeBucketCollector the star tree bucket collector for adding the documents
     * @param profileBreakdown the profiling breakdown to record the time taken for this phase
     * @throws IOException
     */
    default void preComputeBucketsWithStarTreeProfiling(
        StarTreeBucketCollector starTreeBucketCollector,
        StarTreeProfileBreakdown profileBreakdown
    ) throws IOException {
        Timer timer = profileBreakdown.getTimer(StarTreeAggregationTimingType.BUILD_BUCKETS_FROM_STAR_TREE);
        timer.start();
        try {
            preComputeBucketsWithStarTree(starTreeBucketCollector);
        } finally {
            timer.stop();
        }
    }
}
