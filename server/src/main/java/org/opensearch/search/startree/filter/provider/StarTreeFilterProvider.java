/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

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
            }
            if (matchedDimension instanceof DateDimension) {
                if (!(mappedFieldType instanceof DateFieldMapper.DateFieldType)) {
                    return null;
                }
                return getDateFilter(context, rawFilter, (DateFieldMapper.DateFieldType) mappedFieldType, (DateDimension) matchedDimension);
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

        public StarTreeFilter getDateFilter(
            SearchContext context,
            QueryBuilder rawFilter,
            DateFieldMapper.DateFieldType dateFieldType,
            DateDimension dateDimension
        ) {
            RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) rawFilter;
            String field = rangeQueryBuilder.fieldName();

            Query query = context.query();
            if (query instanceof MatchNoDocsQuery) {
                return new StarTreeFilter(Collections.emptyMap());
            }

            // Convert format string to DateMathParser if provided
            DateMathParser forcedDateParser = rangeQueryBuilder.format() != null
                ? DateFormatter.forPattern(rangeQueryBuilder.format()).toDateMathParser()
                : DateFieldMapper.getDefaultDateTimeFormatter().toDateMathParser();

            ZoneId timeZone = rangeQueryBuilder.timeZone() != null ? ZoneId.of(rangeQueryBuilder.timeZone()) : null;

            // Get current time for 'now'-relative expressions
            // Star-tree can be used when the resultant date values are rounded off to intervals in star-tree
            long now = context.getQueryShardContext().nowInMillis();
            LongSupplier nowSupplier = () -> now;

            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;

            if (rangeQueryBuilder.from() != null) {
                l = DateFieldMapper.DateFieldType.parseToLong(
                    rangeQueryBuilder.from(),
                    !rangeQueryBuilder.includeLower(),
                    timeZone,
                    forcedDateParser,
                    nowSupplier,
                    dateFieldType.resolution()
                );
                if (!rangeQueryBuilder.includeLower()) {
                    if (l == Long.MAX_VALUE) {
                        return new StarTreeFilter(Collections.emptyMap());
                    }
                    ++l;
                }
            }

            if (rangeQueryBuilder.to() != null) {
                u = DateFieldMapper.DateFieldType.parseToLong(
                    rangeQueryBuilder.to(),
                    rangeQueryBuilder.includeUpper(),
                    timeZone,
                    forcedDateParser,
                    nowSupplier,
                    dateFieldType.resolution()
                );
                if (!rangeQueryBuilder.includeUpper()) {
                    if (u == Long.MIN_VALUE) {
                        return new StarTreeFilter(Collections.emptyMap());
                    }
                    --u;
                }
            }

            // Find the matching interval - preferring the highest possible interval for query optimization
            List<DateTimeUnitRounding> intervals = dateDimension.getSortedCalendarIntervals().reversed();
            DateTimeUnitRounding matchingInterval = null;

            for (DateTimeUnitRounding interval : intervals) {
                long roundedLow = l != Long.MIN_VALUE ? interval.roundFloor(l) : l;
                long roundedHigh = u != Long.MAX_VALUE ? interval.roundFloor(u) : u;

                // This is needed since OpenSearch rounds up to the last millisecond in the rounding interval.
                // so when the date parser rounds up to say 2022-05-31T23:59:59.999 we can check if by adding 1
                // the new interval which is 2022-05-31T00:00:00.000 in the example can be solved via star tree
                //
                // this is not needed for low since rounding is on the first millisecond 2022-06-01T00:00:00.000
                long roundedHighPlus1 = u != Long.MAX_VALUE ? interval.roundFloor(u + 1) : u;

                // If both bounds round to the same values, we have an exact match for this interval
                if (roundedLow == l && (roundedHigh == u || roundedHighPlus1 == u + 1)) {
                    matchingInterval = interval;
                    break; // Found the most granular matching interval
                }
            }

            if (matchingInterval == null) {
                return null; // No matching interval found, fall back to default implementation
            }

            // Construct the sub-dimension field name
            String subDimensionField = field + "_" + matchingInterval.shortName();

            return new StarTreeFilter(
                Map.of(
                    subDimensionField,
                    List.of(
                        new RangeMatchDimFilter(
                            field,
                            l,
                            u,
                            true,  // Already handled inclusion above
                            true   // Already handled inclusion above
                        )
                    )
                )
            );
        }
    }
}
