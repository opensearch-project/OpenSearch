/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.StarTreeFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            new RangeStarTreeFilterProvider()
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

}
