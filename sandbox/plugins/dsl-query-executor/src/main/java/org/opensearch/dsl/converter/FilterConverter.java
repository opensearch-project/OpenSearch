/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Converts the DSL {@code query} clause and {@code search_after} to a {@link LogicalFilter}.
 */
public class FilterConverter extends AbstractDslConverter {

    private final QueryRegistry queryRegistry;

    /**
     * Creates a filter converter.
     *
     * @param queryRegistry registry of query translators
     */
    public FilterConverter(QueryRegistry queryRegistry) {
        this.queryRegistry = Objects.requireNonNull(queryRegistry, "queryRegistry must not be null");
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return hasQuery(ctx) || hasSearchAfter(ctx);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        List<RexNode> conditions = new ArrayList<>();

        if (hasQuery(ctx)) {
            conditions.add(queryRegistry.convert(ctx.getSearchSource().query(), ctx));
        }

        if (hasSearchAfter(ctx)) {
            conditions.add(buildSearchAfterCondition(input, ctx));
        }

        RexNode condition = conditions.size() == 1 ? conditions.get(0)
            : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, conditions);
        return LogicalFilter.create(input, condition);
    }

    private RexNode buildSearchAfterCondition(RelNode input, ConversionContext ctx) throws ConversionException {
        SearchSourceBuilder ss = ctx.getSearchSource();
        Object[] values = ss.searchAfter();

        if (ss.sorts() == null || ss.sorts().isEmpty()) {
            throw new ConversionException("search_after requires sort");
        }
        if (values.length != ss.sorts().size()) {
            throw new ConversionException("search_after values must match sort fields");
        }

        List<RexNode> orConditions = new ArrayList<>();
        for (int sortIdx = 0; sortIdx < ss.sorts().size(); sortIdx++) {
            if (!(ss.sorts().get(sortIdx) instanceof FieldSortBuilder fieldSort)) {
                throw new ConversionException("search_after only supports field sorts");
            }

            RelDataTypeField field = input.getRowType().getField(fieldSort.getFieldName(), false, false);
            if (field == null) {
                throw new ConversionException("Field not found: " + fieldSort.getFieldName());
            }

            RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
            RexNode literal = ctx.getRexBuilder().makeLiteral(values[sortIdx], field.getType(), true);
            RexNode comparison = fieldSort.order() == SortOrder.ASC
                ? ctx.getRexBuilder().makeCall(SqlStdOperatorTable.GREATER_THAN, fieldRef, literal)
                : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal);

            List<RexNode> equals = new ArrayList<>();
            for (int j = 0; j < sortIdx; j++) {
                FieldSortBuilder prev = (FieldSortBuilder) ss.sorts().get(j);
                RelDataTypeField pf = input.getRowType().getField(prev.getFieldName(), false, false);
                equals.add(ctx.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
                    ctx.getRexBuilder().makeInputRef(pf.getType(), pf.getIndex()),
                    ctx.getRexBuilder().makeLiteral(values[j], pf.getType(), true)));
            }

            if (equals.isEmpty()) {
                orConditions.add(comparison);
            } else {
                RexNode allEquals = equals.size() == 1 ? equals.get(0)
                    : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, equals);
                orConditions.add(ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, allEquals, comparison));
            }
        }

        return orConditions.size() == 1 ? orConditions.get(0)
            : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.OR, orConditions);
    }

    private static boolean hasQuery(ConversionContext ctx) {
        return ctx.getSearchSource().query() != null
            && !(ctx.getSearchSource().query() instanceof MatchAllQueryBuilder);
    }

    private static boolean hasSearchAfter(ConversionContext ctx) {
        return ctx.getSearchSource().searchAfter() != null && ctx.getSearchSource().searchAfter().length > 0;
    }
}

