/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline.sibling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.aggregation.pipeline.BucketsPathResolver;
import org.opensearch.dsl.aggregation.pipeline.GapPolicyHandler;
import org.opensearch.dsl.aggregation.pipeline.PipelineTranslator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.InternalPercentilesBucket;
import org.opensearch.search.aggregations.pipeline.PercentilesBucketPipelineAggregationBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.fill;

/**
 * Translates {@code percentiles_bucket} pipeline aggregation.
 * Produces percentile computations using {@code PERCENTILE_CONT} or
 * falls back to an approximation using sorted aggregation.
 *
 * <p>Default percentiles when none specified: 1, 5, 25, 50, 75, 95, 99.
 */
public class PercentilesBucketTranslator
    implements PipelineTranslator<PercentilesBucketPipelineAggregationBuilder> {

    /** Default percentile values matching OpenSearch defaults. */
    private static final double[] DEFAULT_PERCENTS = { 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0 };

    /** Creates a percentiles_bucket translator. */
    public PercentilesBucketTranslator() {}

    @Override
    public Class<PercentilesBucketPipelineAggregationBuilder> getBuilderClass() {
        return PercentilesBucketPipelineAggregationBuilder.class;
    }

    @Override
    public Type type() {
        return Type.SIBLING;
    }

    @Override
    public RelNode translate(PercentilesBucketPipelineAggregationBuilder builder, RelNode input,
                             ConversionContext ctx) throws ConversionException {
        int colIndex = BucketsPathResolver.resolve(builder.getBucketsPaths()[0], input);
        RelNode gapHandled = GapPolicyHandler.apply(builder.gapPolicy(), input, colIndex, ctx);

        double[] percents = builder.getPercents();
        if (percents == null || percents.length == 0) {
            percents = DEFAULT_PERCENTS;
        }

        RexBuilder rexBuilder = ctx.getRexBuilder();
        RelDataType doubleType = ctx.getCluster().getTypeFactory().createTypeWithNullability(
            ctx.getCluster().getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true);
        String name = builder.getName();

        // PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY metric_col)
        // In Calcite's AggregateCall model:
        //   - The fraction literal is projected as a new column, referenced in argList
        //   - The metric column is specified via the collation (ORDER BY)
        // We project one fraction literal per percentile, then create aggregate calls.
        List<RexNode> projects = new ArrayList<>();
        List<String> projectNames = new ArrayList<>();
        int fieldCount = gapHandled.getRowType().getFieldCount();

        // Pass through all existing columns
        for (int i = 0; i < fieldCount; i++) {
            projects.add(rexBuilder.makeInputRef(gapHandled.getRowType().getFieldList().get(i).getType(), i));
            projectNames.add(gapHandled.getRowType().getFieldNames().get(i));
        }

        // Add one literal column per percentile fraction
        int[] fractionColIndices = new int[percents.length];
        for (int i = 0; i < percents.length; i++) {
            double fraction = percents[i] / 100.0;
            projects.add(rexBuilder.makeExactLiteral(BigDecimal.valueOf(fraction), doubleType));
            projectNames.add("_pct_" + formatPercent(percents[i]));
            fractionColIndices[i] = fieldCount + i;
        }

        RelNode projected = LogicalProject.create(gapHandled, List.of(), projects, projectNames, ImmutableSet.of());

        // Create PERCENTILE_CONT calls: arg = fraction column, collation = ORDER BY metric column
        RelCollation orderByMetric = RelCollations.of(new RelFieldCollation(colIndex));
        List<AggregateCall> calls = new ArrayList<>();
        for (int i = 0; i < percents.length; i++) {
            String pctName = name + "_" + formatPercent(percents[i]);
            calls.add(AggregateCall.create(
                SqlStdOperatorTable.PERCENTILE_CONT, false, false, false,
                ImmutableList.of(), List.of(fractionColIndices[i]), -1, null, orderByMetric,
                doubleType, pctName
            ));
        }

        return LogicalAggregate.create(projected, ImmutableList.of(), ImmutableBitSet.of(), null, calls);
    }

    @Override
    public InternalAggregation toInternalAggregation(PercentilesBucketPipelineAggregationBuilder builder, Object[] row) {
        double[] percents = builder.getPercents();
        if (percents == null || percents.length == 0) {
            percents = DEFAULT_PERCENTS;
        }
        double[] percentiles = new double[percents.length];
        if (row != null && row.length >= percents.length) {
            for (int i = 0; i < percents.length; i++) {
                percentiles[i] = row[i] != null ? ((Number) row[i]).doubleValue() : Double.NaN;
            }
        } else {
            fill(percentiles, Double.NaN);
        }
        return new InternalPercentilesBucket(builder.getName(), percents, percentiles, builder.getKeyed(), PipelineTranslator.resolveFormat(builder), null);
    }

    private static String formatPercent(double pct) {
        return String.valueOf(pct).replace('.', '_');
    }
}
