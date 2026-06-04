/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.common.geo.ShapeRelation;

import java.util.Objects;

/**
 * Generic query constraint extracted from a mandatory range query.
 *
 * This class intentionally models range-query structure only. Its fields are an immutable snapshot of the normalized
 * state exposed by {@link org.opensearch.index.query.RangeQueryBuilder}: lower/upper bounds, inclusivity, and optional
 * range interpretation parameters such as format, time zone, and relation. Type-specific semantics such as date parsing,
 * numeric coercion, or geo interpretation are handled by field-domain evaluators.
 */
public final class RangeQueryConstraint implements QueryConstraint {
    private final String field;
    private final Object lowerValue;
    private final Object upperValue;
    private final boolean includeLower;
    private final boolean includeUpper;
    private final String format;
    private final String timeZone;
    private final ShapeRelation relation;

    /**
     * Creates a range query constraint with optional range-query interpretation parameters.
     *
     * @param field constrained field name
     * @param lowerValue lower bound value, or {@code null} for unbounded
     * @param upperValue upper bound value, or {@code null} for unbounded
     * @param includeLower whether the lower bound is inclusive
     * @param includeUpper whether the upper bound is inclusive
     * @param format optional query-level format
     * @param timeZone optional query-level time zone
     * @param relation optional range relation
     */
    public RangeQueryConstraint(
        String field,
        Object lowerValue,
        Object upperValue,
        boolean includeLower,
        boolean includeUpper,
        String format,
        String timeZone,
        ShapeRelation relation
    ) {
        this.field = Objects.requireNonNull(field, "field must not be null");
        if (field.isEmpty()) {
            throw new IllegalArgumentException("field must not be empty");
        }
        if (lowerValue == null && upperValue == null) {
            throw new IllegalArgumentException("range constraint must have at least one bound");
        }
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
        this.format = format;
        this.timeZone = timeZone;
        this.relation = relation;
    }

    @Override
    public String field() {
        return field;
    }

    /**
     * Lower bound value as supplied by the query builder, or {@code null} when unbounded.
     */
    public Object lowerValue() {
        return lowerValue;
    }

    /**
     * Upper bound value as supplied by the query builder, or {@code null} when unbounded.
     */
    public Object upperValue() {
        return upperValue;
    }

    /**
     * Whether the lower bound is inclusive.
     */
    public boolean includeLower() {
        return includeLower;
    }

    /**
     * Whether the upper bound is inclusive.
     */
    public boolean includeUpper() {
        return includeUpper;
    }

    /**
     * Optional query-level format as supplied by the range query builder.
     */
    public String format() {
        return format;
    }

    /**
     * Optional query-level time zone as supplied by the range query builder.
     */
    public String timeZone() {
        return timeZone;
    }

    /**
     * Optional range relation as supplied by the range query builder.
     */
    public ShapeRelation relation() {
        return relation;
    }

    /**
     * Whether this constraint has a lower bound.
     */
    public boolean hasLowerBound() {
        return lowerValue != null;
    }

    /**
     * Whether this constraint has an upper bound.
     */
    public boolean hasUpperBound() {
        return upperValue != null;
    }
}
