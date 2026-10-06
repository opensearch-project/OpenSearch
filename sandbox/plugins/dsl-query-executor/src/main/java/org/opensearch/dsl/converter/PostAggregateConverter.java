/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Applies post-aggregation shaping: an optional HAVING filter ({@code min_doc_count}), then the
 * plan's bound and order.
 *
 * <p>Root-level sized plans get a sort with a fetch: the engine's sort becomes a bounded top-K
 * (O(size) memory, {@code size} rows transferred). The HAVING filter sits between the aggregate
 * and the sort, so filtering happens before truncation — truncate-then-filter would return
 * fewer than {@code size} buckets where classic search back-fills.
 *
 * <p>Nested levels get the per-parent equivalent ({@link #applyPerParentTopK}): the plan is
 * semi-joined to the parent level's top-N plan (only winning parents' groups survive), then a
 * window computes each row's rank within its parent partition —
 * {@code ROW_NUMBER() OVER (PARTITION BY parentFields ORDER BY bucketOrder)} — and a filter
 * keeps rank ≤ {@code size}. The same window project also attaches
 * {@code SUM(_count) OVER (PARTITION BY parentFields)} as the parent's eligible-document total,
 * computed before the rank filter so it covers the truncated remainder: every surviving row
 * carries its parent's eligible-doc total for {@code sum_other_doc_count}. The window-inside-a-project
 * plus plain-filter shape is the one unified-query PPL {@code top} emits, so the engine bridge
 * is known to execute it.
 *
 * <p>The collation always ends with the group key ascending (classic search's tie-breaker,
 * carried inside every {@code BucketOrder}), so cuts at either bound are deterministic and
 * match classic ordering. Uses {@link CollationResolver} to resolve bucket orders against the
 * actual post-aggregation schema.
 */
public class PostAggregateConverter extends AbstractDslConverter {

    /** Name of the transient window-rank column; dropped before the plan's output. */
    static final String ROW_NUMBER_NAME = "_row_number_top_";

    /**
     * Column name of a nested plan's per-parent eligible-document total: a window
     * {@code SUM(_count) OVER (PARTITION BY parentFields)} emitted here after HAVING and before
     * the per-parent truncation, so every surviving row carries its parent's
     * eligible-doc total for {@code sum_other_doc_count}. Constant within a parent's rows; public because
     * the response builder reads it back off the result columns.
     */
    public static final String PARENT_ELIGIBLE_NAME = "_parent_eligible";

    /** Creates a post-aggregate converter. */
    public PostAggregateConverter() {}

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        AggregationMetadata metadata = ctx.getAggregationMetadata();
        return metadata != null
            && (metadata.hasBucketOrders()
                || metadata.getFetch() != null
                || metadata.getPerParentFetch() != null
                || metadata.getHavingMinDocCount() != null);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        AggregationMetadata metadata = ctx.getAggregationMetadata();
        RelNode result = input;

        if (metadata.getHavingMinDocCount() != null) {
            result = applyHaving(result, ctx, metadata.getHavingMinDocCount());
        }

        if (metadata.getPerParentFetch() != null) {
            return applyPerParentTopK(result, ctx, metadata);
        }

        List<RelFieldCollation> collations = metadata.hasBucketOrders()
            ? CollationResolver.resolve(metadata, result.getRowType())
            : List.of();

        RexNode fetch = metadata.getFetch() == null
            ? null
            : ctx.getRexBuilder()
                .makeLiteral(metadata.getFetch(), ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);

        if (collations.isEmpty() && fetch == null) {
            return result;
        }
        return LogicalSort.create(result, RelCollations.of(collations), null, fetch);
    }

    /** HAVING: {@code _count >= min_doc_count}, between the aggregate and the plan's bound. */
    private static RelNode applyHaving(RelNode input, ConversionContext ctx, long minDocCount) throws ConversionException {
        RelDataTypeField countField = input.getRowType().getField(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME, false, false);
        if (countField == null) {
            throw new ConversionException(
                "min_doc_count requires the implicit " + AggregationMetadataBuilder.IMPLICIT_COUNT_NAME + " column in the aggregate output"
            );
        }
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(countField.getType(), countField.getIndex()),
            rexBuilder.makeLiteral(minDocCount, ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT), false)
        );
        return LogicalFilter.create(input, condition);
    }

    /**
     * Per-parent top-K for a nested level: semi-join to the parent plan's top-N, window-rank
     * within the parent partition, keep rank ≤ K, attach the per-parent eligible total.
     */
    private RelNode applyPerParentTopK(RelNode input, ConversionContext ctx, AggregationMetadata metadata) throws ConversionException {
        RelNode parentPlan = ctx.getParentPlan();
        if (parentPlan == null) {
            throw new ConversionException(
                "Nested aggregation plan ["
                    + String.join(",", metadata.getAggNamePath())
                    + "] requires the parent level's plan for its per-parent bound"
            );
        }
        RexBuilder rexBuilder = ctx.getRexBuilder();
        RelDataType rowType = input.getRowType();
        // All group fields except the last are the parent's keys (eligibility guarantees
        // single-field groupings).
        List<String> parentFields = metadata.getGroupByFieldNames().subList(0, metadata.getGroupByFieldNames().size() - 1);

        // 1. Semi-join: only groups belonging to the parent plan's top-N survive. The semi
        // output keeps the left schema, so downstream indices are unchanged.
        List<RexNode> equalities = new ArrayList<>(parentFields.size());
        int leftFieldCount = rowType.getFieldCount();
        for (String parentField : parentFields) {
            RelDataTypeField left = rowType.getField(parentField, false, false);
            RelDataTypeField right = parentPlan.getRowType().getField(parentField, false, false);
            if (left == null || right == null) {
                throw new ConversionException("Parent group field '" + parentField + "' not found for nested plan join");
            }
            equalities.add(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    rexBuilder.makeInputRef(left.getType(), left.getIndex()),
                    rexBuilder.makeInputRef(right.getType(), leftFieldCount + right.getIndex())
                )
            );
        }
        RexNode joinCondition = equalities.size() == 1 ? equalities.get(0) : rexBuilder.makeCall(SqlStdOperatorTable.AND, equalities);
        RelNode joined = LogicalJoin.create(input, parentPlan, List.of(), joinCondition, Set.of(), JoinRelType.SEMI);

        // 2. Window project: every column, plus the per-parent eligible total, plus the rank.
        List<RexNode> partitionKeys = new ArrayList<>(parentFields.size());
        for (String parentField : parentFields) {
            RelDataTypeField field = rowType.getField(parentField, false, false);
            partitionKeys.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
        }

        ImmutableList.Builder<RexFieldCollation> orderKeys = ImmutableList.builder();
        for (RelFieldCollation collation : CollationResolver.resolve(metadata, rowType)) {
            Set<SqlKind> flags = new HashSet<>();
            if (collation.getDirection() == RelFieldCollation.Direction.DESCENDING) {
                flags.add(SqlKind.DESCENDING);
            }
            if (collation.nullDirection == RelFieldCollation.NullDirection.FIRST) {
                flags.add(SqlKind.NULLS_FIRST);
            } else if (collation.nullDirection == RelFieldCollation.NullDirection.LAST) {
                flags.add(SqlKind.NULLS_LAST);
            }
            RelDataTypeField field = rowType.getFieldList().get(collation.getFieldIndex());
            orderKeys.add(new RexFieldCollation(rexBuilder.makeInputRef(field.getType(), field.getIndex()), flags));
        }

        RelDataType bigint = ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode rowNumber = rexBuilder.makeOver(
            bigint,
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            partitionKeys,
            orderKeys.build(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, // rows frame
            true, // allow partial
            false,
            false,
            false
        );

        RelDataTypeField countField = rowType.getField(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME, false, false);
        if (countField == null) {
            throw new ConversionException(
                "Nested plan requires the implicit " + AggregationMetadataBuilder.IMPLICIT_COUNT_NAME + " column for its parent totals"
            );
        }
        RexNode parentEligible = rexBuilder.makeOver(
            ctx.getCluster().getTypeFactory().createTypeWithNullability(bigint, true),
            SqlStdOperatorTable.SUM,
            List.of(rexBuilder.makeInputRef(countField.getType(), countField.getIndex())),
            partitionKeys,
            ImmutableList.of(), // whole partition — no ordering, unbounded frame
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            false, // range frame
            true,
            false,
            false,
            false
        );

        List<RexNode> withWindow = new ArrayList<>(leftFieldCount + 2);
        List<String> withWindowNames = new ArrayList<>(leftFieldCount + 2);
        for (RelDataTypeField field : rowType.getFieldList()) {
            withWindow.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
            withWindowNames.add(field.getName());
        }
        withWindow.add(parentEligible);
        withWindowNames.add(PARENT_ELIGIBLE_NAME);
        withWindow.add(rowNumber);
        withWindowNames.add(ROW_NUMBER_NAME);
        RelNode windowed = LogicalProject.create(joined, List.of(), withWindow, withWindowNames);

        // 3. Keep each parent's top K.
        RexNode rankLimit = rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(bigint, leftFieldCount + 1),
            rexBuilder.makeLiteral(metadata.getPerParentFetch(), bigint, false)
        );
        RelNode ranked = LogicalFilter.create(windowed, rankLimit);

        // 4. Drop the transient rank column; the parent-eligible total stays for the response.
        RelDataType rankedType = ranked.getRowType();
        List<RexNode> visible = new ArrayList<>(leftFieldCount + 1);
        List<String> visibleNames = new ArrayList<>(leftFieldCount + 1);
        for (int i = 0; i <= leftFieldCount; i++) {
            RelDataTypeField field = rankedType.getFieldList().get(i);
            visible.add(rexBuilder.makeInputRef(field.getType(), i));
            visibleNames.add(field.getName());
        }
        return LogicalProject.create(ranked, List.of(), visible, visibleNames);
    }
}
