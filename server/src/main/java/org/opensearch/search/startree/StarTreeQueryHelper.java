/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Helper class for building star-tree query
 *
 * @opensearch.internal
 * @opensearch.experimental
 */
public class StarTreeQueryHelper {

    private static StarTreeValues starTreeValues;

    /**
     * Checks if the search context can be supported by star-tree
     */
    public static boolean isStarTreeSupported(SearchContext context) {
        return context.aggregations() != null && context.mapperService().isCompositeIndexPresent() && context.parsedPostFilter() == null;
    }

    public static CompositeIndexFieldInfo getSupportedStarTree(QueryShardContext context) {
        StarTreeQueryContext starTreeQueryContext = context.getStarTreeQueryContext();
        return (starTreeQueryContext != null) ? starTreeQueryContext.getStarTree() : null;
    }

    public static StarTreeValues getStarTreeValues(LeafReaderContext context, CompositeIndexFieldInfo starTree) throws IOException {
        SegmentReader reader = Lucene.segmentReader(context.reader());
        if (!(reader.getDocValuesReader() instanceof CompositeIndexReader)) {
            return null;
        }
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
        return (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(starTree);
    }

    /**
     * Get the star-tree leaf collector
     * This collector computes the aggregation prematurely and invokes an early termination collector
     */
    public static void precomputeLeafUsingStarTree(
        SearchContext context,
        ValuesSource.Numeric valuesSource,
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        String metric,
        Consumer<Long> valueConsumer,
        Runnable finalConsumer
    ) throws IOException {
        StarTreeValues starTreeValues = getStarTreeValues(ctx, starTree);
        assert starTreeValues != null;
        String fieldName = ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName();
        String metricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(starTree.getField(), fieldName, metric);

        SortedNumericStarTreeValuesIterator valuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(
            metricName
        );
        // Obtain a FixedBitSet of matched star tree document IDs
        FixedBitSet filteredValues = getStarTreeFilteredValues(context, ctx, starTreeValues);

        int numBits = filteredValues.length();  // Get the number of the filtered values (matching docs)
        if (numBits > 0) {
            // Iterate over the filtered values
            for (int bit = filteredValues.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = (bit + 1 < numBits)
                ? filteredValues.nextSetBit(bit + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {
                // Advance to the entryId in the valuesIterator
                if (valuesIterator.advanceExact(bit) == false) {
                    continue;  // Skip if no more entries
                }

                // Iterate over the values for the current entryId
                for (int i = 0, count = valuesIterator.entryValueCount(); i < count; i++) {
                    long value = valuesIterator.nextValue();
                    valueConsumer.accept(value); // Apply the consumer operation (e.g., max, sum)
                }
            }
        }

        // Call the final consumer after processing all entries
        finalConsumer.run();
    }

    /**
     * Get the filtered values for the star-tree query
     * Cache the results in case of multiple aggregations (if cache is initialized)
     * @return FixedBitSet of matched document IDs
     */
    public static FixedBitSet getStarTreeFilteredValues(SearchContext context, LeafReaderContext ctx, StarTreeValues starTreeValues)
        throws IOException {
        FixedBitSet result = context.getQueryShardContext().getStarTreeQueryContext().maybeGetCachedNodeIdsForSegment(ctx.ord);
        if (result == null) {
            result = StarTreeTraversalUtil.getStarTreeResult(
                starTreeValues,
                context.getQueryShardContext().getStarTreeQueryContext().getBaseQueryStarTreeFilter(),
                context
            );
        }
        context.getQueryShardContext().getStarTreeQueryContext().maybeSetCachedNodeIdsForSegment(ctx.ord, result);
        return result;
    }

    public static Dimension getMatchingDimensionOrThrow(String dimensionName, List<Dimension> orderedDimensions) {
        Dimension matchingDimension = getMatchingDimensionOrNull(dimensionName, orderedDimensions);
        if (matchingDimension == null) {
            throw new IllegalStateException("No matching dimension found for [" + dimensionName + "]");
        }
        return matchingDimension;
    }

    public static Dimension getMatchingDimensionOrNull(String dimensionName, List<Dimension> orderedDimensions) {
        List<Dimension> matchingDimensions = orderedDimensions.stream().filter(x -> x.getField().equals(dimensionName)).toList();
        if (matchingDimensions.size() != 1) {
            return null;
        }
        return matchingDimensions.get(0);
    }

    public static StarTreeBucketCollector getStarTreeBucketMetricCollector(
        CompositeIndexFieldInfo starTree,
        String metric,
        ValuesSource.Numeric valuesSource,
        StarTreeBucketCollector parentCollector,
        Consumer<Long> growArrays,
        BiConsumer<Long, Long> updateBucket
    ) throws IOException {
        assert parentCollector != null;
        return new StarTreeBucketCollector(parentCollector) {
            String metricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                starTree.getField(),
                ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName(),
                metric
            );
            SortedNumericStarTreeValuesIterator metricValuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
                .getMetricValuesIterator(metricName);

            @Override
            public void collectStarTreeEntry(int starTreeEntryBit, long bucket) throws IOException {
                growArrays.accept(bucket);
                // Advance the valuesIterator to the current bit
                if (!metricValuesIterator.advanceExact(starTreeEntryBit)) {
                    return; // Skip if no entries for this document
                }
                long metricValue = metricValuesIterator.nextValue();
                updateBucket.accept(bucket, metricValue);
            }
        };
    }

    /**
     * Fetches the metric values iterator for document counts from StarTreeValues.
     */
    public static SortedNumericStarTreeValuesIterator getDocCountsIterator(
        StarTreeValues starTreeValues,
        CompositeIndexFieldInfo starTree
    ) {
        String metricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
            starTree.getField(),
            DocCountFieldMapper.NAME,
            MetricStat.DOC_COUNT.getTypeName()
        );
        return (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(metricName);
    }

    /**
     * For a StarTreeBucketCollector, get matching star-tree entries and update relevant buckets in aggregator
     */
    public static void preComputeBucketsWithStarTree(StarTreeBucketCollector starTreeBucketCollector) throws IOException {
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

    public static StarTreeFilter mergeDimensionFilterIfNotExists(
        StarTreeFilter baseStarTreeFilter,
        String dimensionToMerge,
        List<DimensionFilter> dimensionFiltersToMerge
    ) {
        Map<String, List<DimensionFilter>> dimensionFilterMap = new HashMap<>(baseStarTreeFilter.getDimensions().size());
        for (String baseDimension : baseStarTreeFilter.getDimensions()) {
            dimensionFilterMap.put(baseDimension, baseStarTreeFilter.getFiltersForDimension(baseDimension));
        }
        // Don't add groupBy when already present in base filter.
        if (!dimensionFilterMap.containsKey(dimensionToMerge)) {
            dimensionFilterMap.put(dimensionToMerge, dimensionFiltersToMerge);
        }
        return new StarTreeFilter(dimensionFilterMap);
    }

}
