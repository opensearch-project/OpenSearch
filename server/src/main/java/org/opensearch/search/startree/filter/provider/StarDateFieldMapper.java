/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.DateFieldMapper.DateFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

class StarDateFieldMapper implements DimensionFilterMapper {

    CompositeDataCubeFieldType compositeDataCubeFieldType;
    LongSupplier nowSupplier;
    String subDimensionField;
    DateDimension dateDimension;
    List<String> sortedSubDimensions;

    public StarDateFieldMapper(SearchContext searchContext) {
        this.nowSupplier = () -> searchContext.getQueryShardContext().nowInMillis();
        this.compositeDataCubeFieldType = (CompositeDataCubeFieldType) searchContext.mapperService()
            .getCompositeFieldTypes()
            .iterator()
            .next();
        // Single date-field is used as of now
        dateDimension = (DateDimension) compositeDataCubeFieldType.getDimensions()
            .stream()
            .filter(dim -> dim instanceof DateDimension)
            .findFirst()
            .orElse(null);
        this.sortedSubDimensions = dateDimension != null ? dateDimension.getSubDimensionNames() : null;
    }

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        // TODO: Add support for exact match
        return null;
    }

    @Override
    public DimensionFilter getRangeMatchFilter(MappedFieldType mappedFieldType, StarTreeRangeQuery rangeQuery) {
        DateFieldType dateFieldType = (DateFieldType) mappedFieldType;
        String field = rangeQuery.fieldName();
        assert Objects.equals(dateDimension.getField(), rangeQuery.fieldName());

        // Convert format string to DateMathParser if provided
        DateMathParser forcedDateParser = rangeQuery.format() != null
            ? DateFormatter.forPattern(rangeQuery.format()).toDateMathParser()
            : org.opensearch.index.mapper.DateFieldMapper.getDefaultDateTimeFormatter().toDateMathParser();

        ZoneId timeZone = rangeQuery.timeZone() != null ? ZoneId.of(rangeQuery.timeZone()) : null;

        long l = Long.MIN_VALUE;
        long u = Long.MAX_VALUE;

        if (rangeQuery.from() != null) {
            l = DateFieldType.parseToLong(
                rangeQuery.from(),
                !rangeQuery.includeLower(),
                timeZone,
                forcedDateParser,
                nowSupplier,
                dateFieldType.resolution()
            );
            if (!rangeQuery.includeLower()) {
                ++l;
            }
        }

        if (rangeQuery.to() != null) {
            u = DateFieldType.parseToLong(
                rangeQuery.to(),
                rangeQuery.includeUpper(),
                timeZone,
                forcedDateParser,
                nowSupplier,
                dateFieldType.resolution()
            );
            if (!rangeQuery.includeUpper()) {
                --u;
            }
        }

        // Find the matching interval - preferring the highest possible interval for query optimization
        List<DateTimeUnitRounding> intervals = dateDimension.getSortedCalendarIntervals().reversed();
        DateTimeUnitRounding matchingInterval = null;

        for (DateTimeUnitRounding interval : intervals) {
            // OpenSearch rounds up to the last millisecond in the rounding interval.
            // So for example, closed-interval [l=2022-05-31T23:00:00.000, u=2022-05-31T23:59:59.999]
            // (equivalent to half-open interval [l=2022-05-31T23:00:00.000, u+1=2022-06-01T00:00:00.000))
            // can be resolved by star-tree 'hour' interval.
            // To verify the above case, we check whether:
            // 1/ l = 2022-05-31T23:00:00.000 is to the nearest 'hour' or not
            // 2/ u+1 = 2022-06-01T00:00:00.000 is to the nearest 'hour' or not.

            // Check if l is start of star-tree interval
            boolean roundedLowMatches = l == Long.MIN_VALUE || interval.roundFloor(l) == l;

            // Check if u+1 is the start of star-tree interval compared to u
            boolean roundedHighMatches = u == Long.MAX_VALUE || interval.roundFloor(u + 1) == u + 1;

            // If both bounds can be resolved by this star-tree interval, we have an exact match for this interval
            if (roundedLowMatches && roundedHighMatches) {
                matchingInterval = interval;
                break; // Found the most granular matching interval
            }
        }

        if (matchingInterval == null) {
            return null; // No matching interval found, fall back to default implementation
        }

        // Construct the sub-dimension field name
        subDimensionField = field + "_" + matchingInterval.shortName();

        // l & u are inclusive interval boundaries here
        return new RangeMatchDimFilter(field, l, u, true, true) {
            @Override
            public String getSubDimensionName() {
                return subDimensionField;
            }
        };
    }

    @Override
    public boolean resolveUsingSubDimension() {
        return true;
    }

    @Override
    public List<DimensionFilter> getFinalDimensionFilters(List<DimensionFilter> filters) {
        if (filters == null || filters.size() <= 1) {
            return filters;
        }

        boolean needsFinalFilter = false;
        int lowestDimensionGranularity = sortedSubDimensions.indexOf(filters.getFirst().getSubDimensionName());
        for (DimensionFilter filter : filters) {
            int granularity = sortedSubDimensions.indexOf(filter.getSubDimensionName());
            if (lowestDimensionGranularity > granularity) {
                needsFinalFilter = true;
                lowestDimensionGranularity = granularity;
            }
        }
        // return the same filters if all filters have the same granularity
        if (needsFinalFilter == false) {
            return filters;
        }

        String finalSubDimensionField = sortedSubDimensions.get(lowestDimensionGranularity);
        String dimensionName = filters.getFirst().getDimensionName();
        List<DimensionFilter> updatedFilters = new ArrayList<>();
        for (DimensionFilter filter : filters) {
            RangeMatchDimFilter rangeMatchDimFilter = (RangeMatchDimFilter) filter;
            updatedFilters.add(
                new RangeMatchDimFilter(
                    dimensionName,
                    rangeMatchDimFilter.getLow(),
                    rangeMatchDimFilter.getHigh(),
                    rangeMatchDimFilter.isIncludeLow(),
                    rangeMatchDimFilter.isIncludeHigh()
                ) {
                    @Override
                    public String getSubDimensionName() {
                        return finalSubDimensionField;
                    }
                }
            );
        }
        return updatedFilters;
    }

    @Override
    public Optional<Long> getMatchingOrdinal(
        String dimensionName,
        Object value,
        StarTreeValues starTreeValues,
        DimensionFilter.MatchType matchType
    ) {
        // Since dates are stored as longs internally, we can treat them as numeric values
        return Optional.of((Long) value);
    }

    @Override
    public int compareValues(Object v1, Object v2) {
        if (!(v1 instanceof Long) || !(v2 instanceof Long)) {
            throw new IllegalArgumentException("Expected Long values for date comparison");
        }
        return Long.compare((Long) v1, (Long) v2);
    }

    public String getSubDimensionFieldEffective(String subDimensionField1, String subDimensionField2) {
        return sortedSubDimensions.indexOf(subDimensionField1) < sortedSubDimensions.indexOf(subDimensionField2)
            ? subDimensionField1
            : subDimensionField2;
    }
}
