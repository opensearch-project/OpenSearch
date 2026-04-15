/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Converts DSL {@code sort}, {@code from}, and {@code size} into a {@link LogicalSort}
 * with collation (ordering) and offset/fetch (pagination).
 */
public class SortConverter extends AbstractDslConverter {

    /** Creates a sort converter. */
    public SortConverter() {}

    // Core defaults to _score DESC when no sort is specified. The analytics engine
    // has no relevance scoring, so unsorted queries return rows in unspecified order.
    // TODO: handle ScoreSortBuilder (_score sort)
    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return hasSort(ctx) || hasNonDefaultPagination(ctx);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        RelNode processedInput = processSortModes(input, ctx);
        RelCollation collation = buildCollation(processedInput, ctx);
        RexNode offset = buildOffset(ctx);
        RexNode fetch = buildFetch(ctx);

        return LogicalSort.create(processedInput, collation, offset, fetch);
    }

    private RelNode processSortModes(RelNode input, ConversionContext ctx) throws ConversionException {
        if (!hasSort(ctx)) {
            return input;
        }

        RelNode current = input;
        List<String> addedFields = new ArrayList<>();

        for (SortBuilder<?> sortBuilder : ctx.getSearchSource().sorts()) {
            if (sortBuilder instanceof FieldSortBuilder fieldSort && fieldSort.sortMode() != null) {
                String fieldName = fieldSort.getFieldName();
                String modeFieldName = fieldName + "_mode";
                current = addModeField(current, fieldName, modeFieldName, fieldSort.sortMode(), ctx);
                addedFields.add(modeFieldName);
            }
        }

        return current;
    }

    private RelNode addModeField(RelNode input, String arrayField, String modeField, SortMode mode, ConversionContext ctx) 
            throws ConversionException {
        RexBuilder rexBuilder = ctx.getRexBuilder();
        CorrelationId correlationId = ctx.getCluster().createCorrel();
        
        RelNode left = input;
        
        RelDataTypeField field = input.getRowType().getField(arrayField, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + arrayField + "' not found");
        }
        
        RelDataType fieldType = field.getType();
        RelDataType elementType = fieldType.getComponentType();
        
        // If field is not an array type, wrap it as an array for UNNEST
        if (elementType == null) {
            elementType = fieldType;
            fieldType = ctx.getCluster().getTypeFactory().createArrayType(elementType, -1);
        }
        
        RexNode correlVar = rexBuilder.makeCorrel(input.getRowType(), correlationId);
        RexNode arrayRef = rexBuilder.makeFieldAccess(correlVar, field.getIndex());
        
        // Cast to array type if needed
        if (field.getType().getComponentType() == null) {
            arrayRef = rexBuilder.makeCast(fieldType, arrayRef);
        }
        
        LogicalTableFunctionScan unnest = LogicalTableFunctionScan.create(
            ctx.getCluster(),
            Collections.emptyList(),
            rexBuilder.makeCall(SqlStdOperatorTable.UNNEST, arrayRef),
            null,
            ctx.getCluster().getTypeFactory().builder()
                .add("value", elementType)
                .build(),
            Collections.emptySet()
        );
        
        org.apache.calcite.sql.SqlAggFunction aggFunc = getAggFunction(mode);
        org.apache.calcite.rel.core.AggregateCall aggCall = 
            org.apache.calcite.rel.core.AggregateCall.create(
                aggFunc,
                false,
                Collections.singletonList(0),
                -1,
                0,
                unnest,
                null,
                modeField
            );
        
        LogicalAggregate aggregate = LogicalAggregate.create(
            unnest,
            false,
            ImmutableBitSet.of(),
            null,
            Collections.singletonList(aggCall)
        );
        
        LogicalCorrelate correlate = LogicalCorrelate.create(
            left,
            aggregate,
            correlationId,
            ImmutableBitSet.of(field.getIndex()),
            JoinRelType.INNER
        );
        
        List<RexNode> projects = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        
        for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
            projects.add(rexBuilder.makeInputRef(correlate, i));
            fieldNames.add(left.getRowType().getFieldList().get(i).getName());
        }
        
        projects.add(rexBuilder.makeInputRef(correlate, left.getRowType().getFieldCount()));
        fieldNames.add(modeField);
        
        return LogicalProject.create(correlate, Collections.emptyList(), projects, fieldNames);
    }

    private org.apache.calcite.sql.SqlAggFunction getAggFunction(SortMode mode) throws ConversionException {
        return switch (mode) {
            case MIN -> SqlStdOperatorTable.MIN;
            case MAX -> SqlStdOperatorTable.MAX;
            case SUM -> SqlStdOperatorTable.SUM;
            case AVG -> SqlStdOperatorTable.AVG;
            case MEDIAN -> throw new ConversionException("MEDIAN sort mode is not yet supported");
            default -> throw new ConversionException("Unsupported sort mode: " + mode);
        };
    }

    private RelCollation buildCollation(RelNode input, ConversionContext ctx) throws ConversionException {
        if (!hasSort(ctx)) {
            return RelCollations.EMPTY;
        }

        RelDataType rowType = input.getRowType();
        List<RelFieldCollation> fieldCollations = new ArrayList<>();

        for (SortBuilder<?> sortBuilder : ctx.getSearchSource().sorts()) {
            if (sortBuilder instanceof FieldSortBuilder fieldSort) {
                String fieldName = fieldSort.sortMode() != null 
                    ? fieldSort.getFieldName() + "_mode" 
                    : fieldSort.getFieldName();
                    
                RelDataTypeField field = rowType.getField(fieldName, false, false);
                if (field == null) {
                    throw new ConversionException("Sort field '" + fieldName + "' not found in schema");
                }

                RelFieldCollation.Direction direction = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.Direction.ASCENDING
                    : RelFieldCollation.Direction.DESCENDING;

                RelFieldCollation.NullDirection nullDirection = (fieldSort.order() == SortOrder.ASC)
                    ? RelFieldCollation.NullDirection.LAST
                    : RelFieldCollation.NullDirection.FIRST;

                fieldCollations.add(new RelFieldCollation(field.getIndex(), direction, nullDirection));
            } else {
                throw new ConversionException("Sort type not supported: " + sortBuilder.getClass().getSimpleName());
            }
        }

        return RelCollations.of(fieldCollations);
    }

    private RexNode buildOffset(ConversionContext ctx) {
        SearchSourceBuilder ss = ctx.getSearchSource();
        int from = ss.from() != -1 ? ss.from() : SearchService.DEFAULT_FROM;
        if (from <= 0) {
            return null;
        }
        return ctx.getRexBuilder().makeLiteral(from,
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);
    }

    private RexNode buildFetch(ConversionContext ctx) {
        if (!hasNonDefaultPagination(ctx)) {
            return null;
        }
        SearchSourceBuilder ss = ctx.getSearchSource();
        int size = ss.size() != -1 ? ss.size() : SearchService.DEFAULT_SIZE;
        return ctx.getRexBuilder().makeLiteral(size,
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER), false);
    }

    private static boolean hasSort(ConversionContext ctx) {
        return ctx.getSearchSource().sorts() != null && !ctx.getSearchSource().sorts().isEmpty();
    }

    private static boolean hasNonDefaultPagination(ConversionContext ctx) {
        SearchSourceBuilder ss = ctx.getSearchSource();
        int from = ss.from() != -1 ? ss.from() : SearchService.DEFAULT_FROM;
        int size = ss.size() != -1 ? ss.size() : SearchService.DEFAULT_SIZE;
        return !(from == SearchService.DEFAULT_FROM && size == SearchService.DEFAULT_SIZE);
    }
}
