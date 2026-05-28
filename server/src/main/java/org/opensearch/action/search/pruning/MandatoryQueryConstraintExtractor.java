/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Conservative query-tree walker that extracts constraints from mandatory query positions.
 *
 * Only clauses under {@code bool.filter}, {@code bool.must}, {@code constant_score}, and top-level range queries are
 * considered. Optional or negative clauses are ignored because they are not required for every matching document.
 */
public final class MandatoryQueryConstraintExtractor implements QueryConstraintExtractor {
    /**
     * Extracts mandatory constraints for the configured pruning fields.
     */
    @Override
    public List<QueryConstraint> extractMandatoryConstraints(SearchSourceBuilder source, Set<String> pruningFields) {
        if (source == null || source.query() == null || pruningFields == null || pruningFields.isEmpty()) {
            return List.of();
        }

        List<QueryConstraint> constraints = new ArrayList<>();
        visitMandatory(source.query(), pruningFields, constraints);
        return List.copyOf(constraints);
    }

    private void visitMandatory(QueryBuilder query, Set<String> pruningFields, List<QueryConstraint> constraints) {
        if (query == null) {
            return;
        }

        if (query instanceof BoolQueryBuilder) {
            BoolQueryBuilder bool = (BoolQueryBuilder) query;
            bool.filter().forEach(child -> visitMandatory(child, pruningFields, constraints));
            bool.must().forEach(child -> visitMandatory(child, pruningFields, constraints));
            return;
        }

        if (query instanceof ConstantScoreQueryBuilder) {
            visitMandatory(((ConstantScoreQueryBuilder) query).innerQuery(), pruningFields, constraints);
            return;
        }

        if (query instanceof RangeQueryBuilder) {
            RangeQueryConstraint constraint = buildRangeConstraint((RangeQueryBuilder) query, pruningFields);
            if (constraint != null) {
                constraints.add(constraint);
            }
        }
    }

    private static RangeQueryConstraint buildRangeConstraint(RangeQueryBuilder range, Set<String> pruningFields) {
        String field = range.fieldName();
        if (field == null || pruningFields.contains(field) == false) {
            return null;
        }

        if (range.from() == null && range.to() == null) {
            return null;
        }

        /*
         * format, time_zone, and relation change how a range query is interpreted.
         * The generic range constraint intentionally carries only field/bounds/inclusion.
         * Until a typed constraint models these options explicitly, skip them and keep
         * pruning conservative.
         */
        if (range.format() != null || range.timeZone() != null || range.relation() != null) {
            return null;
        }

        return new RangeQueryConstraint(field, range.from(), range.to(), range.includeLower(), range.includeUpper());
    }
}
