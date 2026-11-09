/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.FieldDomain;
import org.opensearch.test.OpenSearchTestCase;

public class DateRangeFieldDomainEvaluatorTests extends OpenSearchTestCase {
    private final FieldDomainEvaluators evaluators = FieldDomainEvaluators.defaultEvaluators();
    private final FieldDomainEvaluationContext context = new FieldDomainEvaluationContext(() -> 0L);

    public void testCanMatchReturnsFalseForDisjointDateRanges() {
        assertFalse(evaluators.canMatch(bounds(100L, 200L, true), constraint(300L, 400L, true, true), context));
        assertFalse(evaluators.canMatch(bounds(100L, 200L, true), constraint(200L, 400L, false, true), context));
        assertFalse(evaluators.canMatch(bounds(100L, 200L, true), constraint(null, 99L, true, true), context));
    }

    public void testCanMatchReturnsTrueForIntersectingDateRanges() {
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint(50L, 100L, true, true), context));
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint(200L, 300L, true, true), context));
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint(150L, 250L, true, true), context));
    }

    public void testCanMatchHandlesOpenEndedDateRanges() {
        assertFalse(evaluators.canMatch(bounds(100L, 200L, true), constraint(201L, null, true, true), context));
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint(200L, null, true, true), context));
        assertFalse(evaluators.canMatch(bounds(100L, 200L, true), constraint(null, 99L, true, true), context));
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint(null, 100L, true, true), context));
    }

    public void testCanMatchHonorsInclusiveAndExclusiveDateRangeBoundaries() {
        RangeQueryConstraint lowerInclusive = constraint("1970-01-01T00:00:02Z", "1970-01-01T00:00:03Z", true, true);
        RangeQueryConstraint lowerExclusive = constraint("1970-01-01T00:00:02Z", "1970-01-01T00:00:03Z", false, true);
        RangeQueryConstraint upperInclusive = constraint("1970-01-01T00:00:00Z", "1970-01-01T00:00:01Z", true, true);
        RangeQueryConstraint upperExclusive = constraint("1970-01-01T00:00:00Z", "1970-01-01T00:00:01Z", true, false);

        assertTrue(evaluators.canMatch(bounds(1_000L, 2_000L, true), lowerInclusive, context));
        assertFalse(evaluators.canMatch(bounds(1_000L, 2_000L, true), lowerExclusive, context));
        assertTrue(evaluators.canMatch(bounds(1_000L, 2_000L, true), upperInclusive, context));
        assertFalse(evaluators.canMatch(bounds(1_000L, 2_000L, true), upperExclusive, context));
    }

    public void testCanMatchHandlesSinglePointDateRanges() {
        assertTrue(evaluators.canMatch(bounds(100L, 100L, true), constraint(100L, 100L, true, true), context));
        assertFalse(evaluators.canMatch(bounds(100L, 100L, true), constraint(100L, 100L, false, true), context));
        assertFalse(evaluators.canMatch(bounds(100L, 100L, true), constraint(100L, 100L, true, false), context));
    }

    public void testCanMatchReturnsFalseForEmptyQueryRange() {
        assertFalse(evaluators.canMatch(bounds(100L, 200L, true), constraint(100L, 101L, false, false), context));
    }

    public void testCanMatchParsesIsoDateRangeConstraints() {
        RangeQueryConstraint query = constraint("1970-01-01T00:00:03Z", "1970-01-01T00:00:04Z", true, true);

        assertFalse(evaluators.canMatch(bounds(1_000L, 2_000L, true), query, context));
        assertTrue(evaluators.canMatch(bounds(3_000L, 4_000L, true), query, context));
    }

    public void testCanMatchParsesDateMathUsingSearchStartTime() {
        FieldDomainEvaluationContext searchStartContext = new FieldDomainEvaluationContext(() -> 10_000L);
        RangeQueryConstraint query = constraint("now-2m", "now", true, true);

        assertTrue(evaluators.canMatch(bounds(0L, 9_000L, true), query, searchStartContext));
        assertFalse(evaluators.canMatch(bounds(11_000L, 12_000L, true), query, searchStartContext));
    }

    public void testCanMatchParsesQueryTimeZone() {
        RangeQueryConstraint query = constraint("@timestamp", "1970-01-01", null, true, true, null, "+01:00", null);

        assertTrue(evaluators.canMatch(bounds(-3_600_000L, -3_600_000L, true), query, context));
        assertFalse(evaluators.canMatch(bounds(-3_600_001L, -3_600_001L, true), query, context));
    }

    public void testCanMatchParsesPartialDateRangeConstraints() {
        RangeQueryConstraint query = constraint("1970-01-01", "1970-01-02", true, false);

        assertTrue(evaluators.canMatch(bounds(0L, 86_399_999L, true), query, context));
        assertFalse(evaluators.canMatch(bounds(86_400_000L, 172_799_999L, true), query, context));
    }

    public void testCanMatchHonorsPartialDateRoundingForInclusivity() {
        RangeQueryConstraint lowerInclusive = constraint("1970-01-01", null, true, true);
        RangeQueryConstraint lowerExclusive = constraint("1970-01-01", null, false, true);
        RangeQueryConstraint upperInclusive = constraint(null, "1970-01-01", true, true);
        RangeQueryConstraint upperExclusive = constraint(null, "1970-01-01", true, false);

        assertTrue(evaluators.canMatch(bounds(0L, 0L, true), lowerInclusive, context));
        assertFalse(evaluators.canMatch(bounds(-1L, -1L, true), lowerInclusive, context));

        assertFalse(evaluators.canMatch(bounds(86_399_999L, 86_399_999L, true), lowerExclusive, context));
        assertTrue(evaluators.canMatch(bounds(86_400_000L, 86_400_000L, true), lowerExclusive, context));

        assertTrue(evaluators.canMatch(bounds(86_399_999L, 86_399_999L, true), upperInclusive, context));
        assertFalse(evaluators.canMatch(bounds(86_400_000L, 86_400_000L, true), upperInclusive, context));

        assertTrue(evaluators.canMatch(bounds(-1L, -1L, true), upperExclusive, context));
        assertFalse(evaluators.canMatch(bounds(0L, 0L, true), upperExclusive, context));
    }

    public void testCanMatchParsesDateNanosBounds() {
        RangeQueryConstraint query = constraint("1970-01-01T00:00:03Z", "1970-01-01T00:00:04Z", true, true);

        assertFalse(
            evaluators.canMatch(
                new DateRangeFieldDomain("@timestamp", "1000000000", "2000000000", true, "test", null, "nanoseconds"),
                query,
                context
            )
        );
        assertTrue(
            evaluators.canMatch(
                new DateRangeFieldDomain("@timestamp", "3000000000", "4000000000", true, "test", null, "nanoseconds"),
                query,
                context
            )
        );
    }

    public void testCanMatchParsesDateNanosResolutionTypeCaseInsensitively() {
        RangeQueryConstraint query = constraint("1970-01-01T00:00:03Z", "1970-01-01T00:00:04Z", true, true);

        assertFalse(
            evaluators.canMatch(
                new DateRangeFieldDomain("@timestamp", "1000000000", "2000000000", true, "test", null, "DATE_NANOS"),
                query,
                context
            )
        );
    }

    public void testCanMatchHonorsDateNanosBoundaryTouch() {
        RangeQueryConstraint inclusive = constraint("1970-01-01T00:00:02Z", "1970-01-01T00:00:03Z", true, true);
        RangeQueryConstraint exclusive = constraint("1970-01-01T00:00:02Z", "1970-01-01T00:00:03Z", false, true);
        DateRangeFieldDomain domain = new DateRangeFieldDomain("@timestamp", "2000000000", "2000000000", true, "test", null, "nanoseconds");

        assertTrue(evaluators.canMatch(domain, inclusive, context));
        assertFalse(evaluators.canMatch(domain, exclusive, context));
    }

    public void testCanMatchParsesCustomMetadataFormat() {
        RangeQueryConstraint query = constraint("1970/01/01", "1970/01/02", true, false);

        assertTrue(
            evaluators.canMatch(
                new DateRangeFieldDomain("@timestamp", "0", "86399999", true, "test", "yyyy/MM/dd", "milliseconds"),
                query,
                context
            )
        );
        assertFalse(
            evaluators.canMatch(
                new DateRangeFieldDomain("@timestamp", "86400000", "172799999", true, "test", "yyyy/MM/dd", "milliseconds"),
                query,
                context
            )
        );
    }

    public void testCanMatchUsesQueryFormatOverMetadataFormat() {
        RangeQueryConstraint query = constraint("@timestamp", "1970|01|01", null, true, true, "yyyy|MM|dd", null, null);

        assertFalse(
            evaluators.canMatch(
                new DateRangeFieldDomain("@timestamp", "-1", "-1", true, "test", "yyyy/MM/dd", "milliseconds"),
                query,
                context
            )
        );
    }

    public void testCanMatchReturnsTrueWhenEvaluationIsUnsupportedOrUnsafe() {
        assertTrue(evaluators.canMatch(bounds(100L, 200L, false), constraint(300L, 400L, true, true), context));
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint("not-a-date", "1970-01-01T00:00:02Z", true, true), context));
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint("now--", "now", true, true), context));
        assertTrue(
            evaluators.canMatch(
                new DateRangeFieldDomain("@timestamp", "100", "200", true, "test", "[", "milliseconds"),
                constraint(300L, 400L, true, true),
                context
            )
        );
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), constraint("other_field", 300L, 400L, true, true), context));
        assertTrue(evaluators.canMatch(new UnsupportedFieldDomain("@timestamp"), constraint(300L, 400L, true, true), context));
        assertTrue(evaluators.canMatch(bounds(100L, 200L, true), (QueryConstraint) () -> "@timestamp", context));
        assertTrue(
            evaluators.canMatch(
                bounds(100L, 200L, true),
                constraint("@timestamp", 300L, 400L, true, true, null, null, ShapeRelation.INTERSECTS),
                context
            )
        );
    }

    private static DateRangeFieldDomain bounds(long min, long max, boolean finalized) {
        return new DateRangeFieldDomain("@timestamp", min, max, finalized, "test");
    }

    private static RangeQueryConstraint constraint(Object lower, Object upper, boolean includeLower, boolean includeUpper) {
        return constraint("@timestamp", lower, upper, includeLower, includeUpper);
    }

    private static RangeQueryConstraint constraint(String field, Object lower, Object upper, boolean includeLower, boolean includeUpper) {
        return constraint(field, lower, upper, includeLower, includeUpper, null, null, null);
    }

    private static RangeQueryConstraint constraint(
        String field,
        Object lower,
        Object upper,
        boolean includeLower,
        boolean includeUpper,
        String format,
        String timeZone,
        ShapeRelation relation
    ) {
        return new RangeQueryConstraint(field, lower, upper, includeLower, includeUpper, format, timeZone, relation);
    }

    private static final class UnsupportedFieldDomain implements FieldDomain {
        private final String field;

        private UnsupportedFieldDomain(String field) {
            this.field = field;
        }

        @Override
        public String field() {
            return field;
        }

        @Override
        public String type() {
            return "unsupported";
        }

        @Override
        public boolean finalized() {
            return true;
        }
    }
}
