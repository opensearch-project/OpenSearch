/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline.sibling;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.aggregation.pipeline.BucketsPathResolver;
import org.opensearch.dsl.aggregation.pipeline.GapPolicyHandler;
import org.opensearch.dsl.aggregation.pipeline.PipelineTranslator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;

import java.util.List;

/**
 * Translates {@code max_bucket} pipeline aggregation to a second-level
 * {@link LogicalAggregate} with {@code MAX} over the resolved metric column.
 */
public class MaxBucketTranslator implements PipelineTranslator<MaxBucketPipelineAggregationBuilder> {

    /** Creates a max_bucket translator. */
    public MaxBucketTranslator() {}

    @Override
    public Class<MaxBucketPipelineAggregationBuilder> getBuilderClass() {
        return MaxBucketPipelineAggregationBuilder.class;
    }

    @Override
    public Type type() {
        return Type.SIBLING;
    }

    @Override
    public RelNode translate(MaxBucketPipelineAggregationBuilder builder, RelNode input,
                             ConversionContext ctx) throws ConversionException {
        int colIndex = BucketsPathResolver.resolve(builder.getBucketsPaths()[0], input);
        RelNode gapHandled = GapPolicyHandler.apply(builder.gapPolicy(), input, colIndex, ctx);

        AggregateCall maxCall = AggregateCall.create(
            SqlStdOperatorTable.MAX, false, false, false,
            ImmutableList.of(), List.of(colIndex), -1, null, RelCollations.EMPTY,
            gapHandled.getRowType().getFieldList().get(colIndex).getType(),
            builder.getName()
        );
        return LogicalAggregate.create(gapHandled, ImmutableList.of(), ImmutableBitSet.of(), null, List.of(maxCall));
    }

    @Override
    public InternalAggregation toInternalAggregation(MaxBucketPipelineAggregationBuilder builder, Object[] row) {
        if (row == null || row.length < 1) {
            return new InternalSimpleValue(builder.getName(), Double.NEGATIVE_INFINITY, PipelineTranslator.resolveFormat(builder), null);
        }
        double value = row[0] != null ? ((Number) row[0]).doubleValue() : Double.NEGATIVE_INFINITY;
        return new InternalSimpleValue(builder.getName(), value, PipelineTranslator.resolveFormat(builder), null);
    }
}
