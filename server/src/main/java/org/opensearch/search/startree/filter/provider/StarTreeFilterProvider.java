/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilterMerger;
import org.opensearch.search.startree.filter.StarTreeFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts a {@link QueryBuilder} into a {@link StarTreeFilter} by generating the appropriate @{@link org.opensearch.search.startree.filter.DimensionFilter}
 * for the fields provided in the user query.
 */
@ExperimentalApi
public interface StarTreeFilterProvider {

    /**
     * Returns the {@link StarTreeFilter} generated from the {@link QueryBuilder}
     * @param context:
     * @param rawFilter:
     * @param compositeFieldType:
     * @return : {@link StarTreeFilter} if the query shape is supported, else null.
     * @throws IOException :
     */
    StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
        throws IOException;

    StarTreeFilterProvider MATCH_ALL_PROVIDER = (context, rawFilter, compositeFieldType) -> new StarTreeFilter(Collections.emptyMap());

    /**
     * Singleton instances for most {@link StarTreeFilterProvider}
     */
    class SingletonFactory {

        private static final Map<String, StarTreeFilterProvider> QUERY_BUILDERS_TO_STF_PROVIDER = Map.of(
            MatchAllQueryBuilder.NAME,
            MATCH_ALL_PROVIDER,
            TermQueryBuilder.NAME,
            new TermStarTreeFilterProvider(),
            TermsQueryBuilder.NAME,
            new TermsStarTreeFilterProvider(),
            RangeQueryBuilder.NAME,
            new RangeStarTreeFilterProvider(),
            BoolQueryBuilder.NAME,
            new BoolStarTreeFilterProvider()
        );

        public static StarTreeFilterProvider getProvider(QueryBuilder query) {
            if (query != null) {
                return QUERY_BUILDERS_TO_STF_PROVIDER.get(query.getName());
            }
            return MATCH_ALL_PROVIDER;
        }

    }

    /**
     * Converts @{@link TermQueryBuilder} into @{@link org.opensearch.search.startree.filter.ExactMatchDimFilter}
     */
    class TermStarTreeFilterProvider implements StarTreeFilterProvider {
        @Override
        public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
            throws IOException {
            TermQueryBuilder termQueryBuilder = (TermQueryBuilder) rawFilter;
            String field = termQueryBuilder.fieldName();
            MappedFieldType mappedFieldType = context.mapperService().fieldType(field);
            DimensionFilterMapper dimensionFilterMapper = mappedFieldType != null
                ? DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType)
                : null;
            Dimension matchedDimension = StarTreeQueryHelper.getMatchingDimensionOrNull(field, compositeFieldType.getDimensions());
            if (matchedDimension == null || mappedFieldType == null || dimensionFilterMapper == null) {
                return null; // Indicates Aggregators to fallback to default implementation.
            } else {
                return new StarTreeFilter(
                    Map.of(field, List.of(dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of(termQueryBuilder.value()))))
                );
            }
        }
    }

    /**
     * Converts @{@link TermsQueryBuilder} into @{@link org.opensearch.search.startree.filter.ExactMatchDimFilter}
     */
    class TermsStarTreeFilterProvider implements StarTreeFilterProvider {
        @Override
        public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
            throws IOException {
            TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) rawFilter;
            String field = termsQueryBuilder.fieldName();
            Dimension matchedDimension = StarTreeQueryHelper.getMatchingDimensionOrNull(field, compositeFieldType.getDimensions());
            MappedFieldType mappedFieldType = context.mapperService().fieldType(field);
            DimensionFilterMapper dimensionFilterMapper = mappedFieldType != null
                ? DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType)
                : null;
            if (matchedDimension == null || mappedFieldType == null || dimensionFilterMapper == null) {
                return null; // Indicates Aggregators to fallback to default implementation.
            } else {
                return new StarTreeFilter(
                    Map.of(field, List.of(dimensionFilterMapper.getExactMatchFilter(mappedFieldType, termsQueryBuilder.values())))
                );
            }
        }
    }

    /**
     * Converts @{@link RangeQueryBuilder} into @{@link org.opensearch.search.startree.filter.RangeMatchDimFilter}
     */
    class RangeStarTreeFilterProvider implements StarTreeFilterProvider {

        @Override
        public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
            throws IOException {
            RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) rawFilter;
            String field = rangeQueryBuilder.fieldName();
            Dimension matchedDimension = StarTreeQueryHelper.getMatchingDimensionOrNull(field, compositeFieldType.getDimensions());
            MappedFieldType mappedFieldType = context.mapperService().fieldType(field);
            DimensionFilterMapper dimensionFilterMapper = mappedFieldType == null
                ? null
                : DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType);
            if (matchedDimension == null || mappedFieldType == null || dimensionFilterMapper == null) {
                return null;
            } else {
                return new StarTreeFilter(
                    Map.of(
                        field,
                        List.of(
                            dimensionFilterMapper.getRangeMatchFilter(
                                mappedFieldType,
                                rangeQueryBuilder.from(),
                                rangeQueryBuilder.to(),
                                rangeQueryBuilder.includeLower(),
                                rangeQueryBuilder.includeUpper()
                            )
                        )
                    )
                );
            }
        }

    }

    class BoolStarTreeFilterProvider implements StarTreeFilterProvider {
        @Override
        public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType) throws IOException {
            BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) rawFilter;
            if (!boolQueryBuilder.hasClauses()) {
                return null;
            }

            return processMustClauses(boolQueryBuilder.must(), context, compositeFieldType);
        }

        private StarTreeFilter processMustClauses(List<QueryBuilder> mustClauses, SearchContext context,
                                                  CompositeDataCubeFieldType compositeFieldType) throws IOException {
            if (mustClauses.isEmpty()) {
                return null;
            }
            Map<String, List<DimensionFilter>> dimensionToFilters = new HashMap<>();

            for (QueryBuilder clause : mustClauses) {
                StarTreeFilter clauseFilter;

                if (clause instanceof BoolQueryBuilder) {
                    // Recursive processing for nested bool
                    clauseFilter = processMustClauses(((BoolQueryBuilder) clause).must(), context, compositeFieldType);
                } else {
                    // Process individual clause
                    StarTreeFilterProvider provider = SingletonFactory.getProvider(clause);
                    if (provider == null) {
                        return null;
                    }
                    clauseFilter = provider.getFilter(context, clause, compositeFieldType);
                }

                if (clauseFilter == null) {
                    return null;
                }

                // Merge filters for each dimension
                for (String dimension : clauseFilter.getDimensions()) {
                    List<DimensionFilter> existingFilters = dimensionToFilters.get(dimension);
                    List<DimensionFilter> newFilters = clauseFilter.getFiltersForDimension(dimension);

                    if (existingFilters == null) {
                        dimensionToFilters.put(dimension, new ArrayList<>(newFilters));
                    } else {
                        // Here's where we need the DimensionFilter merging logic
                        // For example: merging range with term, or range with range
                        DimensionFilter mergedFilter = mergeDimensionFilters(existingFilters, newFilters);
                        if (mergedFilter == null) {
                            return null; // No possible matches after merging
                        }
                        dimensionToFilters.put(dimension, Collections.singletonList(mergedFilter));
                    }
                }
            }

            return new StarTreeFilter(dimensionToFilters);
        }

        /**
         * Merges multiple DimensionFilters for the same dimension.
         * This is where we need to implement the actual merging logic.
         */
        private DimensionFilter mergeDimensionFilters(List<DimensionFilter> filters1, List<DimensionFilter> filters2) {
            DimensionFilter filter1 = filters1.getFirst();
            DimensionFilter filter2 = filters2.getFirst();

            return DimensionFilterMerger.intersect(filter1, filter2);
        }
    }
}
