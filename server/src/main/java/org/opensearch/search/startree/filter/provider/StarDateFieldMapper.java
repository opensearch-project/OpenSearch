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
import org.opensearch.index.mapper.DateFieldMapper.DateFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

class StarDateFieldMapper implements DimensionFilterMapper {

    DateDimension dateDimension;
    LongSupplier nowSupplier;
    String subDimensionField;

    @Override
    public DimensionFilter getExactMatchFilter(MappedFieldType mappedFieldType, List<Object> rawValues) {
        DateFieldType dateFieldType = (DateFieldType) mappedFieldType;

        List<Object> convertedValues = new ArrayList<>(rawValues.size());
        for (Object rawValue : rawValues) {
            convertedValues.add(dateFieldType.parse(rawValue.toString()));
        }
        return new ExactMatchDimFilter(mappedFieldType.name(), convertedValues);

    }

    @Override
    public DimensionFilter getRangeMatchFilter(MappedFieldType mappedFieldType, RangeQueryBuilder rangeQueryBuilder) {

        DateFieldType dateFieldType = (DateFieldType) mappedFieldType;
        String field = rangeQueryBuilder.fieldName();

        // Convert format string to DateMathParser if provided
        DateMathParser forcedDateParser = rangeQueryBuilder.format() != null
            ? DateFormatter.forPattern(rangeQueryBuilder.format()).toDateMathParser()
            : org.opensearch.index.mapper.DateFieldMapper.getDefaultDateTimeFormatter().toDateMathParser();

        ZoneId timeZone = rangeQueryBuilder.timeZone() != null ? ZoneId.of(rangeQueryBuilder.timeZone()) : null;

        long l = Long.MIN_VALUE;
        long u = Long.MAX_VALUE;

        if (rangeQueryBuilder.from() != null) {
            l = org.opensearch.index.mapper.DateFieldMapper.DateFieldType.parseToLong(
                rangeQueryBuilder.from(),
                !rangeQueryBuilder.includeLower(),
                timeZone,
                forcedDateParser,
                nowSupplier,
                dateFieldType.resolution()
            );
            if (!rangeQueryBuilder.includeLower()) {
                if (l == Long.MAX_VALUE) {
                    return null;
                }
                ++l;
            }
        }

        if (rangeQueryBuilder.to() != null) {
            u = org.opensearch.index.mapper.DateFieldMapper.DateFieldType.parseToLong(
                rangeQueryBuilder.to(),
                rangeQueryBuilder.includeUpper(),
                timeZone,
                forcedDateParser,
                nowSupplier,
                dateFieldType.resolution()
            );
            if (!rangeQueryBuilder.includeUpper()) {
                if (u == Long.MIN_VALUE) {
                    return null;
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
        subDimensionField = field + "_" + matchingInterval.shortName();

        return new RangeMatchDimFilter(
            field,
            l,
            u,
            true,  // Already handled inclusion above
            true   // Already handled inclusion above

        );
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

    void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    void setNowSupplier(LongSupplier nowSupplier) {
        this.nowSupplier = nowSupplier;
    }

    String getSubDimensionField() {
        return subDimensionField;
    }
}
