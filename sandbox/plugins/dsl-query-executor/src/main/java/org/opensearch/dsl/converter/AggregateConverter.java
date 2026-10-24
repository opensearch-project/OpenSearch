/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.LiteralColumn;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Creates a {@link LogicalAggregate} from pre-computed {@link AggregationMetadata}.
 * The metadata is produced by the tree walker and set on the context before this runs.
 *
 * <p>When the metadata carries literal-derived columns (constant call arguments or a
 * {@code missing} substitution as {@code COALESCE(field, value)}), the input is wrapped in
 * an identity project appending one column per entry; the identity prefix preserves input
 * positions, so group-by indices are unaffected.
 */
public class AggregateConverter {

    /** Creates an aggregate converter. */
    public AggregateConverter() {}

    /**
     * Builds a LogicalAggregate from the given metadata.
     *
     * @param input the input plan (scan + filter)
     * @param metadata pre-computed aggregation metadata for one granularity
     * @return the LogicalAggregate node
     */
    public RelNode convert(RelNode input, AggregationMetadata metadata) {
        RelNode aggInput = metadata.getLiteralColumns().isEmpty() ? input : appendLiteralColumns(input, metadata.getLiteralColumns());
        return LogicalAggregate.create(aggInput, metadata.getGroupByBitSet(), null, metadata.getAggregateCalls());
    }

    private static RelNode appendLiteralColumns(RelNode input, List<LiteralColumn> literals) {
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        int fieldCount = input.getRowType().getFieldCount();

        List<RexNode> projects = new ArrayList<>(fieldCount + literals.size());
        List<String> names = new ArrayList<>(fieldCount + literals.size());
        for (int i = 0; i < fieldCount; i++) {
            projects.add(rexBuilder.makeInputRef(input, i));
            names.add(input.getRowType().getFieldNames().get(i));
        }
        for (int i = 0; i < literals.size(); i++) {
            LiteralColumn column = literals.get(i);
            switch (column.kind()) {
                case DOUBLE_CONSTANT -> {
                    projects.add(rexBuilder.makeApproxLiteral(BigDecimal.valueOf(column.value())));
                    names.add("_lit" + i);
                }
                case INTEGER_CONSTANT -> {
                    projects.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf((long) column.value())));
                    names.add("_lit" + i);
                }
                case COALESCED -> {
                    // COALESCE operands must share one value type (backend CoalesceAdapter), so cast
                    // the substitute to the field's type; nullability is left to COALESCE itself.
                    RexNode fieldRef = rexBuilder.makeInputRef(input, column.coalesceFieldIndex());
                    RexNode constant = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(column.value()));
                    RexNode substitute = rexBuilder.makeCast(fieldRef.getType(), constant, false, false);
                    projects.add(rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, fieldRef, substitute));
                    names.add("_missing" + i);
                }
            }
        }
        return LogicalProject.create(input, List.of(), projects, names, Set.of());
    }
}
