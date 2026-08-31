/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.List;
import java.util.Map;

/**
 * Expression-based grouping: the group key is a <em>computed</em> column rather than a raw
 * field. Used by bucket aggregations whose buckets are not field values — {@code range} (and,
 * later, {@code histogram}/{@code date_histogram}) — where each document is mapped to a single
 * bucket ordinal by an expression over the source field.
 *
 * <p>The single {@link #getFieldNames() field name} is a synthetic column that a pre-aggregate
 * projection materializes; the {@code LogicalAggregate}'s GROUP BY then resolves to it, exactly
 * as {@link FieldGrouping} resolves to a real field. This keeps {@link GroupingInfo} pure data —
 * the RexNode is built converter-side from {@link #getSourceField()} and {@link #getBounds()},
 * mirroring the way {@code PreAggregateConverter} builds the {@code missing}-substitution CASE.
 *
 * <p>{@code range} is single-membership by construction (overlapping ranges are rejected at
 * validation), so one ordinal per document is sufficient.
 */
public class ExpressionGrouping implements GroupingInfo {

    /** A single half-open bucket interval {@code [from, to)}; {@code from}/{@code to} may be ±∞. */
    public record Bound(double from, double to) {
    }

    private final String syntheticColumn;
    private final String sourceField;
    private final List<Bound> bounds;

    /**
     * Creates an expression grouping.
     *
     * @param syntheticColumn the projected group-key column name (must not collide with a mapped field)
     * @param sourceField the source field the bucket expression reads
     * @param bounds the bucket intervals in the aggregation's declaration order; ordinal {@code i}
     *        is {@code bounds.get(i)} and is the key emitted by the group expression for a document
     *        whose value falls in that interval
     */
    public ExpressionGrouping(String syntheticColumn, String sourceField, List<Bound> bounds) {
        this.syntheticColumn = syntheticColumn;
        this.sourceField = sourceField;
        this.bounds = List.copyOf(bounds);
    }

    @Override
    public List<String> getFieldNames() {
        return List.of(syntheticColumn);
    }

    /** Expression groupings never carry a {@code missing} substitution — the expression handles nulls (ELSE NULL). */
    @Override
    public Map<String, Object> getMissingByField() {
        return Map.of();
    }

    /** Returns the synthetic group-key column name. */
    public String getSyntheticColumn() {
        return syntheticColumn;
    }

    /** Returns the source field the bucket expression reads. */
    public String getSourceField() {
        return sourceField;
    }

    /** Returns the bucket intervals in declaration order; ordinal {@code i} maps to {@code get(i)}. */
    public List<Bound> getBounds() {
        return bounds;
    }
}
