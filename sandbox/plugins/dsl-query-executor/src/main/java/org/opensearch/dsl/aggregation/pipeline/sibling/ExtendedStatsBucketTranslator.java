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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
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
import org.opensearch.search.aggregations.pipeline.ExtendedStatsBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalExtendedStatsBucket;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates {@code extended_stats_bucket} pipeline aggregation.
 * <p>
 * Produces a {@link LogicalProject} that appends a {@code col * col} column,
 * followed by a {@link LogicalAggregate} with COUNT, MIN, MAX, SUM, and
 * SUM(col*col). The {@link InternalExtendedStatsBucket} computes variance,
 * stddev, and sigma bounds internally from count, sum, min, max, and sumOfSqrs.
 */
public class ExtendedStatsBucketTranslator
    implements PipelineTranslator<ExtendedStatsBucketPipelineAggregationBuilder> {

    /** Creates an extended_stats_bucket translator. */
    public ExtendedStatsBucketTranslator() {}

    @Override
    public Class<ExtendedStatsBucketPipelineAggregationBuilder> getBuilderClass() {
        return ExtendedStatsBucketPipelineAggregationBuilder.class;
    }

    @Override
    public Type type() {
        return Type.SIBLING;
    }

    @Override
    public RelNode translate(ExtendedStatsBucketPipelineAggregationBuilder builder, RelNode input,
                             ConversionContext ctx) throws ConversionException {
        int colIndex = BucketsPathResolver.resolve(builder.getBucketsPaths()[0], input);
        RelNode gapHandled = GapPolicyHandler.apply(builder.gapPolicy(), input, colIndex, ctx);

        RexBuilder rexBuilder = ctx.getRexBuilder();
        RelDataTypeFactory typeFactory = ctx.getCluster().getTypeFactory();
        RelDataType colType = gapHandled.getRowType().getFieldList().get(colIndex).getType();
        RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        String name = builder.getName();

        // Pre-project: pass through all columns and append col * col
        List<RelDataTypeField> inputFields = gapHandled.getRowType().getFieldList();
        List<RexNode> preProjects = new ArrayList<>();
        List<String> preNames = new ArrayList<>();
        for (int i = 0; i < inputFields.size(); i++) {
            preProjects.add(rexBuilder.makeInputRef(inputFields.get(i).getType(), i));
            preNames.add(inputFields.get(i).getName());
        }
        RexNode colRef = rexBuilder.makeInputRef(colType, colIndex);
        RexNode colSquared = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, colRef, colRef);
        preProjects.add(colSquared);
        preNames.add(gapHandled.getRowType().getFieldNames().get(colIndex) + "_squared");
        int colSquaredIndex = preProjects.size() - 1;

        RelNode projected = LogicalProject.create(gapHandled, List.of(), preProjects, preNames, ImmutableSet.of());

        // Aggregate: COUNT, MIN, MAX, SUM, SUM(col*col)
        List<AggregateCall> calls = new ArrayList<>();
        calls.add(AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, false, false,
            ImmutableList.of(), List.of(colIndex), -1, null, RelCollations.EMPTY,
            bigintType, name + "_count"
        ));
        calls.add(AggregateCall.create(
            SqlStdOperatorTable.MIN, false, false, false,
            ImmutableList.of(), List.of(colIndex), -1, null, RelCollations.EMPTY,
            colType, name + "_min"
        ));
        calls.add(AggregateCall.create(
            SqlStdOperatorTable.MAX, false, false, false,
            ImmutableList.of(), List.of(colIndex), -1, null, RelCollations.EMPTY,
            colType, name + "_max"
        ));
        calls.add(AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false,
            ImmutableList.of(), List.of(colIndex), -1, null, RelCollations.EMPTY,
            colType, name + "_sum"
        ));
        calls.add(AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false,
            ImmutableList.of(), List.of(colSquaredIndex), -1, null, RelCollations.EMPTY,
            colType, name + "_sum_of_sqrs"
        ));

        return LogicalAggregate.create(projected, ImmutableList.of(), ImmutableBitSet.of(), null, calls);
    }

    @Override
    public InternalAggregation toInternalAggregation(ExtendedStatsBucketPipelineAggregationBuilder builder, Object[] row) {
        // Row layout: count(0), min(1), max(2), sum(3), sumOfSqrs(4)
        if (row == null || row.length < 5) {
            return new InternalExtendedStatsBucket(builder.getName(), 0, 0.0, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, 0.0, builder.sigma(), PipelineTranslator.resolveFormat(builder), null);
        }
        long count = row[0] != null ? ((Number) row[0]).longValue() : 0;
        double min = row[1] != null ? ((Number) row[1]).doubleValue() : Double.POSITIVE_INFINITY;
        double max = row[2] != null ? ((Number) row[2]).doubleValue() : Double.NEGATIVE_INFINITY;
        double sum = row[3] != null ? ((Number) row[3]).doubleValue() : 0.0;
        double sumOfSqrs = row[4] != null ? ((Number) row[4]).doubleValue() : 0.0;
        return new InternalExtendedStatsBucket(
            builder.getName(), count, sum, min, max, sumOfSqrs, builder.sigma(), PipelineTranslator.resolveFormat(builder), null
        );
    }
}
