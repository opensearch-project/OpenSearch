/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.converter.CollationResolver;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketHelpers;
import org.opensearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Composes the plan for sibling pipeline aggregations: the sibling's aggregate output,
 * shaped to mirror its visible buckets, wrapped in a second-level global aggregate.
 *
 * <pre>
 * LogicalAggregate(group=[{}], one call per pipeline)
 *   [LogicalProject]           gap_policy insert_zeros — COALESCE(metric, 0)
 *     LogicalSort(fetch=size)  sibling's bucket order + truncation
 *       [LogicalFilter]        _count &gt;= min_doc_count (when &gt; 1)
 *         &lt;sibling LogicalAggregate&gt;
 * </pre>
 *
 * <p>The shaping exists because vanilla runs sibling pipelines at final reduce, after the
 * sibling is min_doc_count-filtered, sorted, and truncated to {@code size} — the pipeline
 * must aggregate over the buckets the response shows, not over every group in the data.
 * All pipelines targeting one sibling share this plan; each contributes one aggregate
 * call, and results map back to pipelines by column name.
 */
public final class PipelinePlanComposer {

    private PipelinePlanComposer() {}

    /**
     * One pipeline aggregation to compose: the builder and its resolved metric column.
     *
     * @param pipeline the pipeline aggregation builder
     * @param metricColumn the sibling-plan column its buckets_path resolved to
     */
    public record PipelineTarget(PipelineAggregationBuilder pipeline, String metricColumn) {
    }

    /**
     * Composes the shared plan for all pipeline aggregations targeting one sibling.
     *
     * @param targets the pipelines and their resolved metric columns
     * @param sibling the sibling terms aggregation
     * @param siblingMetadata the walker metadata for the sibling's granularity
     * @param siblingAggregate the sibling's LogicalAggregate node (pre-sort)
     * @param rexBuilder the rex builder
     * @param registry the pipeline translator registry
     * @return the composed pipeline plan
     * @throws ConversionException if a metric column or the count column cannot be resolved
     */
    public static RelNode compose(
        List<PipelineTarget> targets,
        TermsAggregationBuilder sibling,
        AggregationMetadata siblingMetadata,
        RelNode siblingAggregate,
        RexBuilder rexBuilder,
        PipelineRegistry registry
    ) throws ConversionException {
        RelDataType rowType = siblingAggregate.getRowType();
        RelNode shaped = applyMinDocCount(siblingAggregate, sibling, rowType, rexBuilder);
        shaped = applyOrderAndSize(shaped, sibling, siblingMetadata, rowType, rexBuilder);
        shaped = prepareMetricColumns(shaped, targets, rowType, rexBuilder);

        List<AggregateCall> calls = new ArrayList<>(targets.size());
        for (PipelineTarget target : targets) {
            int column = columnIndex(rowType, target.metricColumn(), target.pipeline().getName());
            PipelineTranslator<PipelineAggregationBuilder> translator = registry.get(target.pipeline().getClass());
            calls.add(translator.createAggregateCall(target.pipeline(), column, rexBuilder.getTypeFactory()));
        }
        return LogicalAggregate.create(shaped, ImmutableBitSet.of(), null, calls);
    }

    /** Excludes groups below the sibling's min_doc_count; a no-op at the default of 1. */
    private static RelNode applyMinDocCount(RelNode input, TermsAggregationBuilder sibling, RelDataType rowType, RexBuilder rexBuilder)
        throws ConversionException {
        long minDocCount = sibling.minDocCount();
        if (minDocCount <= 1) {
            return input;
        }
        int countColumn = columnIndex(rowType, AggregationMetadataBuilder.IMPLICIT_COUNT_NAME, sibling.getName());
        RelDataTypeField countField = rowType.getFieldList().get(countColumn);
        RexNode countRef = rexBuilder.makeInputRef(countField.getType(), countColumn);
        RexNode threshold = rexBuilder.makeExactLiteral(BigDecimal.valueOf(minDocCount), countField.getType());
        return LogicalFilter.create(input, rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, countRef, threshold));
    }

    /**
     * Mirrors the sibling's visible buckets: its bucket order with fetch = size.
     * Resolves the sibling's own {@code order()} rather than the metadata's accumulated
     * orders — same-field siblings share one metadata, and the response side truncates
     * each aggregation by its own comparator, which this sort must match.
     */
    private static RelNode applyOrderAndSize(
        RelNode input,
        TermsAggregationBuilder sibling,
        AggregationMetadata siblingMetadata,
        RelDataType rowType,
        RexBuilder rexBuilder
    ) throws ConversionException {
        List<RelFieldCollation> collations = CollationResolver.resolve(siblingMetadata, List.of(sibling.order()), rowType);
        RexNode fetch = rexBuilder.makeExactLiteral(BigDecimal.valueOf(sibling.size()));
        return LogicalSort.create(input, RelCollations.of(collations), null, fetch);
    }

    /**
     * Prepares referenced metric columns: gap policy plus the DOUBLE widening cast.
     * _count is never a gap (a group always has rows), so it takes only the cast.
     */
    private static RelNode prepareMetricColumns(RelNode input, List<PipelineTarget> targets, RelDataType rowType, RexBuilder rexBuilder)
        throws ConversionException {
        Map<Integer, BucketHelpers.GapPolicy> policiesByColumn = new HashMap<>();
        for (PipelineTarget target : targets) {
            int column = columnIndex(rowType, target.metricColumn(), target.pipeline().getName());
            BucketHelpers.GapPolicy policy = gapPolicy(target.pipeline());
            if (AggregationMetadataBuilder.IMPLICIT_COUNT_NAME.equals(target.metricColumn())) {
                policy = BucketHelpers.GapPolicy.SKIP;
            }
            policiesByColumn.put(column, policy);
        }
        return MetricColumnPreparer.prepare(input, rexBuilder, policiesByColumn);
    }

    private static BucketHelpers.GapPolicy gapPolicy(PipelineAggregationBuilder pipeline) {
        if (pipeline instanceof BucketMetricsPipelineAggregationBuilder<?> bucketMetrics) {
            return bucketMetrics.gapPolicy();
        }
        return BucketHelpers.GapPolicy.SKIP;
    }

    private static int columnIndex(RelDataType rowType, String columnName, String forName) throws ConversionException {
        RelDataTypeField field = rowType.getField(columnName, false, false);
        if (field == null) {
            throw new ConversionException(
                "Pipeline aggregation [" + forName + "] column [" + columnName + "] not found. Available: " + rowType.getFieldNames()
            );
        }
        return field.getIndex();
    }
}
