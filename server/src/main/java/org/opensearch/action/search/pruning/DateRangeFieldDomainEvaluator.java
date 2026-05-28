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
import org.opensearch.index.mapper.DateFieldMapper;

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
     */
    @Override
    public boolean canMatch(FieldDomain domain, QueryConstraint constraint, FieldDomainEvaluationContext context) {
        if (!(domain instanceof DateRangeFieldDomain dateRangeDomain) || !(constraint instanceof RangeQueryConstraint rangeConstraint)) {
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

        if (queryRange.get().isEmpty()) {
            return false;
        }

        return indexRange.get().intersects(queryRange.get());
    }

    private static Optional<NormalizedRange> parseIndexRange(DateRangeFieldDomain domain) {
        try {
            long min = Long.parseLong(domain.min());
            long max = Long.parseLong(domain.max());
            if (min > max) {
                return Optional.empty();
            }
            return Optional.of(new NormalizedRange(min, max));
        } catch (RuntimeException e) {
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

        DateMathParser parser;
        try {
            parser = domain.format() == null
                ? DateFieldMapper.getDefaultDateTimeFormatter().toDateMathParser()
                : DateFormatter.forPattern(domain.format()).toDateMathParser();
        } catch (RuntimeException e) {
            return Optional.empty();
        }

        long lowerInclusive = Long.MIN_VALUE;
        if (constraint.hasLowerBound()) {
            // Match RangeQueryBuilder's date-bound rounding: exclusive lower bounds round up before the +1 adjustment.
            Optional<Long> parsed = parseDateValue(
                constraint.lowerValue(),
                constraint.includeLower() == false,
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
            Optional<Long> parsed = parseDateValue(constraint.upperValue(), constraint.includeUpper(), parser, context, resolution);
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
        DateMathParser parser,
        FieldDomainEvaluationContext context,
        DateFieldMapper.Resolution resolution
    ) {
        try {
            return Optional.of(
                DateFieldMapper.DateFieldType.parseToLong(value, roundUp, null, parser, context.nowInMillisSupplier(), resolution)
            );
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static DateFieldMapper.Resolution resolveResolution(DateRangeFieldDomain domain) {
        if (domain.resolution() == null || domain.resolution().isEmpty()) {
            return DateFieldMapper.Resolution.MILLISECONDS;
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
