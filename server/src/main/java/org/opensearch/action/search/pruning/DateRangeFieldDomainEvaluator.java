/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.FieldDomain;
import org.opensearch.index.mapper.DateFieldMapper;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

/**
 * Evaluates date range index bounds against generic range query constraints.
 *
 * Unsupported domains, unsupported constraints, parse failures, and invalid resolutions all return {@code true}
 * so pruning remains conservative.
 */
public final class DateRangeFieldDomainEvaluator implements FieldDomainEvaluator {
    /**
     * Returns {@code false} only when the query date range is provably disjoint from the finalized index date range.
     * Unfinalized domains are treated as potentially matching, even though the pruning service also filters them before
     * calling evaluators.
     */
    @Override
    public boolean canMatch(FieldDomain domain, QueryConstraint constraint, FieldDomainEvaluationContext context) {
        if (!(domain instanceof DateRangeFieldDomain dateRangeDomain) || !(constraint instanceof RangeQueryConstraint rangeConstraint)) {
            return true;
        }
        if (dateRangeDomain.finalized() == false) {
            return true;
        }

        Optional<NormalizedRange> indexRange = parseIndexRange(dateRangeDomain);
        if (indexRange.isEmpty()) {
            return true;
        }

        Optional<NormalizedRange> queryRange = parseQueryRange(dateRangeDomain, rangeConstraint, context);
        if (queryRange.isEmpty()) {
            return true;
        }

        NormalizedRange index = indexRange.get();
        if (index.isEmpty()) {
            return true;
        }

        NormalizedRange query = queryRange.get();
        if (query.isEmpty()) {
            return false;
        }

        return index.intersects(query);
    }

    private static Optional<NormalizedRange> parseIndexRange(DateRangeFieldDomain domain) {
        try {
            long min = Long.parseLong(domain.min());
            long max = Long.parseLong(domain.max());
            if (min > max) {
                return Optional.empty();
            }
            return Optional.of(new NormalizedRange(min, max));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private Optional<NormalizedRange> parseQueryRange(
        DateRangeFieldDomain domain,
        RangeQueryConstraint constraint,
        FieldDomainEvaluationContext context
    ) {
        DateFieldMapper.Resolution resolution = resolveResolution(domain);
        if (resolution == null) {
            return Optional.empty();
        }

        if (constraint.relation() != null) {
            return Optional.empty();
        }

        DateMathParser parser;
        try {
            String format = constraint.format() == null ? domain.format() : constraint.format();
            parser = format == null
                ? DateFieldMapper.getDefaultDateTimeFormatter().toDateMathParser()
                : DateFormatter.forPattern(format).toDateMathParser();
        } catch (RuntimeException e) {
            return Optional.empty();
        }

        ZoneId timeZone;
        try {
            timeZone = constraint.timeZone() == null ? null : ZoneId.of(constraint.timeZone());
        } catch (RuntimeException e) {
            return Optional.empty();
        }

        long lowerInclusive = Long.MIN_VALUE;
        if (constraint.hasLowerBound()) {
            // Match RangeQueryBuilder's date-bound rounding: exclusive lower bounds round up before the +1 adjustment.
            Optional<Long> parsed = parseDateValue(
                constraint.lowerValue(),
                constraint.includeLower() == false,
                timeZone,
                parser,
                context,
                resolution
            );
            if (parsed.isEmpty()) {
                return Optional.empty();
            }
            lowerInclusive = parsed.get();
            if (constraint.includeLower() == false) {
                if (lowerInclusive == Long.MAX_VALUE) {
                    return Optional.of(NormalizedRange.EMPTY);
                }
                lowerInclusive++;
            }
        }

        long upperInclusive = Long.MAX_VALUE;
        if (constraint.hasUpperBound()) {
            // Match RangeQueryBuilder's date-bound rounding: inclusive upper bounds round up to the end of the parsed value.
            Optional<Long> parsed = parseDateValue(
                constraint.upperValue(),
                constraint.includeUpper(),
                timeZone,
                parser,
                context,
                resolution
            );
            if (parsed.isEmpty()) {
                return Optional.empty();
            }
            upperInclusive = parsed.get();
            if (constraint.includeUpper() == false) {
                if (upperInclusive == Long.MIN_VALUE) {
                    return Optional.of(NormalizedRange.EMPTY);
                }
                upperInclusive--;
            }
        }

        if (lowerInclusive > upperInclusive) {
            return Optional.of(NormalizedRange.EMPTY);
        }

        return Optional.of(new NormalizedRange(lowerInclusive, upperInclusive));
    }

    private static Optional<Long> parseDateValue(
        Object value,
        boolean roundUp,
        ZoneId zone,
        DateMathParser parser,
        FieldDomainEvaluationContext context,
        DateFieldMapper.Resolution resolution
    ) {
        try {
            return Optional.of(
                DateFieldMapper.DateFieldType.parseToLong(value, roundUp, zone, parser, context.nowInMillisSupplier(), resolution)
            );
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static DateFieldMapper.Resolution resolveResolution(DateRangeFieldDomain domain) {
        if (domain.resolution() == null || domain.resolution().isEmpty()) {
            return null;
        }

        String configured = domain.resolution().toLowerCase(Locale.ROOT);
        for (DateFieldMapper.Resolution resolution : DateFieldMapper.Resolution.values()) {
            if (resolution.name().toLowerCase(Locale.ROOT).equals(configured) || resolution.type().equals(configured)) {
                return resolution;
            }
        }
        return null;
    }

    private static final class NormalizedRange {
        private static final NormalizedRange EMPTY = new NormalizedRange(1L, 0L);

        private final long min;
        private final long max;

        private NormalizedRange(long min, long max) {
            this.min = min;
            this.max = max;
        }

        private boolean isEmpty() {
            return min > max;
        }

        private boolean intersects(NormalizedRange other) {
            return max >= other.min && min <= other.max;
        }
    }
}
