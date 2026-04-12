/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline.parent;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.aggregation.pipeline.GapPolicyHandler;
import org.opensearch.dsl.aggregation.pipeline.PipelineTranslator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Translates {@code bucket_sort} pipeline aggregation into a {@link LogicalSort}
 * applied on top of the parent bucket aggregation's plan.
 *
 * <p>Supports sorting by metric columns, {@code _count}, and {@code _key},
 * with optional {@code from} (offset) and {@code size} (limit) for truncation.
 */
public class BucketSortTranslator implements PipelineTranslator<BucketSortPipelineAggregationBuilder> {

    /** Creates a bucket_sort translator. */
    public BucketSortTranslator() {}

    @Override
    public Class<BucketSortPipelineAggregationBuilder> getBuilderClass() {
        return BucketSortPipelineAggregationBuilder.class;
    }

    @Override
    public RelNode translate(BucketSortPipelineAggregationBuilder builder, RelNode input,
                             ConversionContext ctx) throws ConversionException {
        RexBuilder rexBuilder = ctx.getRexBuilder();
        List<FieldSortBuilder> sorts = builder.sorts();

        // Apply gap policy to each sort field column.
        // SKIP filters out rows with null sort values; INSERT_ZEROS replaces nulls with 0.
        RelNode gapHandled = input;
        for (FieldSortBuilder sort : sorts) {
            int colIndex = resolveColumn(sort.getFieldName(), gapHandled);
            gapHandled = GapPolicyHandler.apply(builder.gapPolicy(), gapHandled, colIndex, ctx);
        }

        // Build collation from sort fields
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        for (FieldSortBuilder sort : sorts) {
            int colIndex = resolveColumn(sort.getFieldName(), gapHandled);
            RelFieldCollation.Direction direction = sort.order() == SortOrder.DESC
                ? RelFieldCollation.Direction.DESCENDING
                : RelFieldCollation.Direction.ASCENDING;
            fieldCollations.add(new RelFieldCollation(colIndex, direction));
        }

        RelCollation collation = fieldCollations.isEmpty()
            ? RelCollations.EMPTY
            : RelCollations.of(fieldCollations);

        // Build offset and fetch (from/size)
        RexNode offset = builder.from() > 0
            ? rexBuilder.makeExactLiteral(BigDecimal.valueOf(builder.from()))
            : null;
        RexNode fetch = builder.size() != null
            ? rexBuilder.makeExactLiteral(BigDecimal.valueOf(builder.size()))
            : null;

        return LogicalSort.create(gapHandled, collation, offset, fetch);
    }

    @Override
    public Type type() {
        return Type.PARENT;
    }

    @Override
    public InternalAggregation toInternalAggregation(BucketSortPipelineAggregationBuilder builder, Object[] row) {
        // bucket_sort doesn't produce its own aggregation result —
        // it modifies the parent bucket's ordering and truncation.
        return null;
    }

    private static int resolveColumn(String fieldName, RelNode relNode) throws ConversionException {
        List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        throw new ConversionException(
            "bucket_sort field '" + fieldName + "' could not be resolved. "
                + "Available columns: " + relNode.getRowType().getFieldNames()
        );
    }
}
