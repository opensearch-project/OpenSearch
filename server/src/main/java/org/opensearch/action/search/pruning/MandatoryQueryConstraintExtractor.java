/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Conservative query-tree walker that extracts constraints from mandatory query positions.
 *
 * The extractor relies on {@link QueryBuilder#visit(QueryBuilderVisitor)} to traverse the query tree. It only follows
 * children visited as {@link BooleanClause.Occur#MUST} or {@link BooleanClause.Occur#FILTER}; optional or negative
 * clauses are ignored because they are not required for every matching document.
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

        MandatoryQueryConstraintVisitor visitor = new MandatoryQueryConstraintVisitor(pruningFields);
        source.query().visit(visitor);
        return visitor.constraints();
    }

    private static final class MandatoryQueryConstraintVisitor implements QueryBuilderVisitor {
        private final Set<String> pruningFields;
        private final List<QueryConstraint> constraints = new ArrayList<>();

        private MandatoryQueryConstraintVisitor(Set<String> pruningFields) {
            this.pruningFields = pruningFields;
        }

        @Override
        public void accept(QueryBuilder queryBuilder) {
            if (queryBuilder instanceof RangeQueryBuilder) {
                RangeQueryConstraint constraint = buildRangeConstraint((RangeQueryBuilder) queryBuilder, pruningFields);
                if (constraint != null) {
                    constraints.add(constraint);
                }
            }
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            switch (occur) {
                case MUST:
                case FILTER:
                    return this;
                default:
                    return QueryBuilderVisitor.NO_OP_VISITOR;
            }
        }

        private List<QueryConstraint> constraints() {
            return List.copyOf(constraints);
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

        return new RangeQueryConstraint(
            field,
            range.from(),
            range.to(),
            range.includeLower(),
            range.includeUpper(),
            range.format(),
            range.timeZone(),
            range.relation()
        );
    }
}
