/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.MetricAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    /**
     * Gets StarTreeQueryContext from the search context and source builder.
     * Returns null if the query and aggregation cannot be supported.
     */
    public static OlderStarTreeQueryContext getOlderStarTreeQueryContext(SearchContext context, SearchSourceBuilder source)
        throws IOException {
        // Current implementation assumes only single star-tree is supported
        CompositeDataCubeFieldType compositeMappedFieldType = (CompositeDataCubeFieldType) context.mapperService()
            .getCompositeFieldTypes()
            .iterator()
            .next();
        CompositeIndexFieldInfo starTree = new CompositeIndexFieldInfo(
            compositeMappedFieldType.name(),
            compositeMappedFieldType.getCompositeIndexType()
        );

        for (AggregatorFactory aggregatorFactory : context.aggregations().factories().getFactories()) {
            MetricStat metricStat = validateStarTreeMetricSupport(compositeMappedFieldType, aggregatorFactory);
            if (metricStat == null) {
                return null;
            }
        }

        // need to cache star tree values only for multiple aggregations
        boolean cacheStarTreeValues = context.aggregations().factories().getFactories().length > 1;
        int cacheSize = cacheStarTreeValues ? context.indexShard().segments(false).size() : -1;

        return StarTreeQueryHelper.tryCreateStarTreeQueryContext(starTree, compositeMappedFieldType, source.query(), cacheSize);
    }

    /**
     * Uses query builder and composite index info to form star-tree query context
     */
    private static OlderStarTreeQueryContext tryCreateStarTreeQueryContext(
        CompositeIndexFieldInfo compositeIndexFieldInfo,
        CompositeDataCubeFieldType compositeFieldType,
        QueryBuilder queryBuilder,
        int cacheStarTreeValuesSize
    ) {
        Map<String, Long> queryMap;
        if (queryBuilder == null || queryBuilder instanceof MatchAllQueryBuilder) {
            queryMap = null;
        } else if (queryBuilder instanceof TermQueryBuilder) {
            // TODO: Add support for keyword fields
            if (compositeFieldType.getDimensions().stream().anyMatch(d -> d.getDocValuesType() != DocValuesType.SORTED_NUMERIC)) {
                // return null for non-numeric fields
                return null;
            }

            List<String> supportedDimensions = compositeFieldType.getDimensions()
                .stream()
                .map(Dimension::getField)
                .collect(Collectors.toList());
            queryMap = getStarTreePredicates(queryBuilder, supportedDimensions);
            if (queryMap == null) {
                return null;
            }
        } else {
            return null;
        }
        return new OlderStarTreeQueryContext(compositeIndexFieldInfo, queryMap, cacheStarTreeValuesSize);
    }

    /**
     * Parse query body to star-tree predicates
     * @param queryBuilder to match star-tree supported query shape
     * @return predicates to match
     */
    private static Map<String, Long> getStarTreePredicates(QueryBuilder queryBuilder, List<String> supportedDimensions) {
        TermQueryBuilder tq = (TermQueryBuilder) queryBuilder;
        String field = tq.fieldName();
        if (!supportedDimensions.contains(field)) {
            return null;
        }
        long inputQueryVal = Long.parseLong(tq.value().toString());

        // Create a map with the field and the value
        Map<String, Long> predicateMap = new HashMap<>();
        predicateMap.put(field, inputQueryVal);
        return predicateMap;
    }

    private static MetricStat validateStarTreeMetricSupport(
        CompositeDataCubeFieldType compositeIndexFieldInfo,
        AggregatorFactory aggregatorFactory
    ) {
        if (aggregatorFactory instanceof MetricAggregatorFactory && aggregatorFactory.getSubFactories().getFactories().length == 0) {
            String field;
            Map<String, List<MetricStat>> supportedMetrics = compositeIndexFieldInfo.getMetrics()
                .stream()
                .collect(Collectors.toMap(Metric::getField, Metric::getMetrics));

            MetricStat metricStat = ((MetricAggregatorFactory) aggregatorFactory).getMetricStat();
            field = ((MetricAggregatorFactory) aggregatorFactory).getField();

            if (field != null && supportedMetrics.containsKey(field) && supportedMetrics.get(field).contains(metricStat)) {
                return metricStat;
            }
        }
        return null;
    }

    public static CompositeIndexFieldInfo getSupportedStarTree2(QueryShardContext context) {
        StarTreeQueryContext starTreeQueryContext = context.getStarTreeQueryContext();
        return (starTreeQueryContext != null) ? starTreeQueryContext.getStarTree() : null;
    }

    public static CompositeIndexFieldInfo getSupportedStarTree(SearchContext context) {
        OlderStarTreeQueryContext olderStarTreeQueryContext = context.getStarTreeQueryContext();
        return (olderStarTreeQueryContext != null) ? olderStarTreeQueryContext.getStarTree() : null;
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
    public static LeafBucketCollector getStarTreeLeafCollector(
        SearchContext context,
        ValuesSource.Numeric valuesSource,
        LeafReaderContext ctx,
        LeafBucketCollector sub,
        CompositeIndexFieldInfo starTree,
        String metric,
        Consumer<Long> valueConsumer,
        Runnable finalConsumer
    ) throws IOException {
        StarTreeValues starTreeValues = getStarTreeValues(ctx, starTree);
        assert starTreeValues != null;
        String fieldName = ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName();
        String metricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(starTree.getField(), fieldName, metric);

        assert starTreeValues != null;
        SortedNumericStarTreeValuesIterator valuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(
            metricName
        );
        // Obtain a FixedBitSet of matched star tree document IDs
        FixedBitSet filteredValues = getStarTreeFilteredValues(context, ctx, starTreeValues);
        assert filteredValues != null;

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

        // Return a LeafBucketCollector that terminates collection
        return new LeafBucketCollectorBase(sub, valuesSource.doubleValues(ctx)) {
            @Override
            public void collect(int doc, long bucket) {
                throw new CollectionTerminatedException();
            }
        };
    }

    /**
     * Get the filtered values for the star-tree query
     * Cache the results in case of multiple aggregations (if cache is initialized)
     * @return FixedBitSet of matched document IDs
     */
    public static FixedBitSet getStarTreeFilteredValues(SearchContext context, LeafReaderContext ctx, StarTreeValues starTreeValues)
        throws IOException {
        // TODO : Uncomment and implement caching in new STQC
        FixedBitSet result = context.getQueryShardContext().getStarTreeQueryContext().getStarTreeValues(ctx);
        if (result == null) {
            result = OlderStarTreeFilter.getStarTreeResult2(
                starTreeValues,
                context.getQueryShardContext().getStarTreeQueryContext().getBaseQueryStarTreeFilter()
            );
        }
        context.getQueryShardContext().getStarTreeQueryContext().setStarTreeValues(ctx, result);
        return result;
    }

    public static Set<Integer> traverseStarTree(StarTreeValues starTreeValues, Map<String, List<DimensionFilter>> dimensionFilterMap)
        throws IOException {

        Map<String, Integer> dimensionNameToDimIdMap = new HashMap<>();
        int ctr = 0;
        for (Dimension dimension : starTreeValues.getStarTreeField().getDimensionsOrder()) {
            dimensionNameToDimIdMap.put(dimension.getField(), ctr++);
        }

        // Sorting the dimension predicates based on their order
        String[] orderedDimPredicates = new String[dimensionFilterMap.size()];
        dimensionFilterMap.keySet().stream().sorted(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return dimensionNameToDimIdMap.get(o1).compareTo(dimensionNameToDimIdMap.get(o2));
            }
        }).collect(Collectors.toList()).toArray(orderedDimPredicates);

        Set<Integer> matchingDocIds = new HashSet<>();

        List<StarTreeNode> matchingNodes = new ArrayList<>();
        matchingNodes.add(starTreeValues.getRoot());

        List<UnMatchedDocIdSet> unmatchedDocIdSets = new ArrayList<>();

        int dimensionIndexToMatch = 0;

        // Matching all predicates that can be done in the star tree
        while (!matchingNodes.isEmpty() && dimensionIndexToMatch < dimensionFilterMap.size()) {
            String currentDimName = orderedDimPredicates[dimensionIndexToMatch];
            int currentDimId = dimensionNameToDimIdMap.get(currentDimName);
            List<StarTreeNode> effectiveParentStarTreeNodes = matchingNodes.stream().map(node -> {
                try {
                    return reachClosestParent(node, currentDimId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
            matchingNodes.clear(); // These will contain the matching nodes from next ordered dimension.
            for (StarTreeNode effectiveParentStarTreeNode : effectiveParentStarTreeNodes) {
                if (effectiveParentStarTreeNode.getDimensionId() == -1 || effectiveParentStarTreeNode.isLeaf()) {
                    unmatchedDocIdSets.add(new UnMatchedDocIdSet(currentDimId, effectiveParentStarTreeNode));
                    // TODO : Record unmatched dimensions for matching via dimension value iterator
                    continue;
                }
                for (DimensionFilter dimensionFilter : dimensionFilterMap.get(currentDimName)) {
                    // dimensionFilter.matchStarTreeNodes(effectiveParentStarTreeNode, starTreeValues, matchingNodes);
                }
            }
            dimensionIndexToMatch++;
        }

        if (!matchingNodes.isEmpty()) {
            matchingDocIds.addAll(matchingNodes.stream().map(node -> {
                try {
                    return node.getAggregatedDocId();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList()));
        }

        // TODO : Perform Dim Value Iterator matching here for things not matched in star tree
        for (UnMatchedDocIdSet unmatchedDocIdSet : unmatchedDocIdSets) {
            for (int dimId = unmatchedDocIdSet.getMinDimensionIdUnMatched(); dimId < dimensionNameToDimIdMap.size(); dimId++) {
                Dimension dimension = starTreeValues.getStarTreeField().getDimensionsOrder().get(dimId);
                StarTreeValuesIterator dimensionIterator = starTreeValues.getDimensionValuesIterator(dimension.getField());
                SequentialDocValuesIterator dimValueWrapper = new SequentialDocValuesIterator(dimensionIterator);
                for (int docIdToCheck : unmatchedDocIdSet.getUnmatchedDocIds()) {
                    if (dimensionIterator.advance(docIdToCheck) != StarTreeValuesIterator.NO_MORE_ENTRIES) {
                        long value = dimValueWrapper.value(docIdToCheck);
                        for (DimensionFilter dimensionFilter : dimensionFilterMap.get(dimension.getField())) {
                            // Change this if multivalued fields are supported in Star Tree.
                            if (dimensionFilter.matchDimValue(value, starTreeValues)) {
                                matchingDocIds.add(docIdToCheck);
                                break; // Match at least one filter.
                            }
                        }
                    }
                }
            }
        }

        return matchingDocIds;

    }

    private static StarTreeNode reachClosestParent(StarTreeNode startNode, int dimensionOrder) throws IOException {
        StarTreeNode currentNode = startNode;
        while (currentNode.getChildStarNode() != null && currentNode.getChildStarNode().getDimensionId() < dimensionOrder) {
            currentNode = currentNode.getChildStarNode();
        }
        return currentNode;
    }

    public static Dimension getMatchingDimensionOrError(String dimensionName, StarTreeValues starTreeValues) {
        List<Dimension> matchingDimensions = starTreeValues.getStarTreeField()
            .getDimensionsOrder()
            .stream()
            .filter(x -> x.getField().equals(dimensionName))
            .collect(Collectors.toList());
        if (matchingDimensions.size() != 1) {
            throw new IllegalStateException("Expected exactly one dimension but found " + matchingDimensions);
        }
        return matchingDimensions.get(0);
    }

    static class UnMatchedDocIdSet {

        private final int minDimensionIdUnMatched;

        // TODO : Just record start and end
        private final List<Integer> unmatchedDocIds;

        public UnMatchedDocIdSet(int minDimensionIdUnMatched, StarTreeNode unmatchedNode) throws IOException {
            this.minDimensionIdUnMatched = minDimensionIdUnMatched;
            unmatchedDocIds = new ArrayList<>(unmatchedNode.getEndDocId() - unmatchedNode.getStartDocId());
            for (int i = unmatchedNode.getStartDocId(); i < unmatchedNode.getEndDocId(); i++) {
                unmatchedDocIds.add(i);
            }
        }

        public int getMinDimensionIdUnMatched() {
            return minDimensionIdUnMatched;
        }

        public List<Integer> getUnmatchedDocIds() {
            return unmatchedDocIds;
        }
    }
}
