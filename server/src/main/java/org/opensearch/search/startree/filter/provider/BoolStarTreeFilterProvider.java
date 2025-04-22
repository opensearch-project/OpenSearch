/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilterMerger;
import org.opensearch.search.startree.filter.StarTreeFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BoolStarTreeFilterProvider implements StarTreeFilterProvider {
    @Override
    public StarTreeFilter getFilter(SearchContext context, QueryBuilder rawFilter, CompositeDataCubeFieldType compositeFieldType)
        throws IOException {
        BoolQueryBuilder boolQueryBuilder = (BoolQueryBuilder) rawFilter;
        if (!boolQueryBuilder.hasClauses()) {
            return null;
        }

        if (boolQueryBuilder.must().isEmpty() == false) {
            return processMustClauses(boolQueryBuilder.must(), context, compositeFieldType);
        }

        if (boolQueryBuilder.should().isEmpty() == false) {
            return processShouldClauses(boolQueryBuilder.should(), context, compositeFieldType);
        }

        return null;
    }

    private StarTreeFilter processMustClauses(
        List<QueryBuilder> mustClauses,
        SearchContext context,
        CompositeDataCubeFieldType compositeFieldType
    ) throws IOException {
        if (mustClauses.isEmpty()) {
            return null;
        }
        Map<String, List<DimensionFilter>> dimensionToFilters = new HashMap<>();

        for (QueryBuilder clause : mustClauses) {
            StarTreeFilter clauseFilter;

            if (clause instanceof BoolQueryBuilder) {
                BoolQueryBuilder boolClause = (BoolQueryBuilder) clause;
                if (boolClause.must().isEmpty() == false) {
                    // Process nested MUST
                    clauseFilter = processMustClauses(boolClause.must(), context, compositeFieldType);
                } else if (boolClause.should().isEmpty() == false) {
                    // Process SHOULD inside MUST
                    clauseFilter = processShouldClauses(boolClause.should(), context, compositeFieldType);
                    // Note: clauseFilter now contains all SHOULD conditions as separate filters
                } else {
                    return null;
                }
            } else {
                // Only allow other supported QueryBuilders
                if (!(clause instanceof TermQueryBuilder)
                    && !(clause instanceof TermsQueryBuilder)
                    && !(clause instanceof RangeQueryBuilder)) {
                    return null;
                }
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
                    // No existing filters for this dimension
                    dimensionToFilters.put(dimension, new ArrayList<>(newFilters));
                } else {
                    // We have existing filters for this dimension
                    if (newFilters.size() > 1) {
                        // New filters are from SHOULD clause (multiple filters = OR condition)
                        // Need to intersect each SHOULD filter with existing filters
                        List<DimensionFilter> intersectedFilters = new ArrayList<>();
                        for (DimensionFilter shouldFilter : newFilters) {
                            for (DimensionFilter existingFilter : existingFilters) {
                                DimensionFilter intersected = DimensionFilterMerger.intersect(existingFilter, shouldFilter);
                                if (intersected != null) {
                                    intersectedFilters.add(intersected);
                                }
                            }
                        }
                        if (intersectedFilters.isEmpty()) {
                            return null; // No valid intersections
                        }
                        dimensionToFilters.put(dimension, intersectedFilters);
                    } else {
                        // Here's where we need the DimensionFilter merging logic
                        // For example: merging range with term, or range with range
                        // And a single dimension filter coming from should clause is as good as must clause
                        DimensionFilter mergedFilter = DimensionFilterMerger.intersect(
                            existingFilters.getFirst(),
                            newFilters.getFirst()
                        );
                        if (mergedFilter == null) {
                            return null; // No possible matches after merging
                        }
                        dimensionToFilters.put(dimension, Collections.singletonList(mergedFilter));
                    }
                }
            }
        }

        return new StarTreeFilter(dimensionToFilters);
    }

    private StarTreeFilter processShouldClauses(
        List<QueryBuilder> shouldClauses,
        SearchContext context,
        CompositeDataCubeFieldType compositeFieldType
    ) throws IOException {
        if (shouldClauses.isEmpty()) {
            return null;
        }

        // First, validate all SHOULD clauses are for same dimension
        String commonDimension = null;
        Map<String, List<DimensionFilter>> dimensionToFilters = new HashMap<>();
        for (QueryBuilder clause : shouldClauses) {
            StarTreeFilter clauseFilter;

            if (clause instanceof BoolQueryBuilder) {
                BoolQueryBuilder boolClause = (BoolQueryBuilder) clause;
                if (boolClause.must().isEmpty() == false) {
                    clauseFilter = processMustClauses(boolClause.must(), context, compositeFieldType);
                } else if (boolClause.should().isEmpty() == false) {
                    clauseFilter = processShouldClauses(boolClause.should(), context, compositeFieldType);
                } else {
                    return null;
                }
            } else {
                // Only allow other supported QueryBuilders
                if (!(clause instanceof TermQueryBuilder)
                    && !(clause instanceof TermsQueryBuilder)
                    && !(clause instanceof RangeQueryBuilder)) {
                    return null;
                }

                StarTreeFilterProvider provider = SingletonFactory.getProvider(clause);
                if (provider == null) {
                    return null;
                }
                clauseFilter = provider.getFilter(context, clause, compositeFieldType);
            }

            if (clauseFilter == null) {
                return null;
            }

            // Validate single dimension
            if (clauseFilter.getDimensions().size() != 1) {
                return null; // SHOULD clause must operate on single dimension
            }

            String dimension = clauseFilter.getDimensions().iterator().next();
            if (commonDimension == null) {
                commonDimension = dimension;
            } else if (commonDimension.equals(dimension) == false) {
                return null; // All SHOULD clauses must operate on same dimension
            }

            // Simply collect all filters - StarTreeTraversal will handle OR operation
            dimensionToFilters.computeIfAbsent(dimension, k -> new ArrayList<>())
                .addAll(clauseFilter.getFiltersForDimension(dimension));
        }
        return new StarTreeFilter(dimensionToFilters);
    }
}
