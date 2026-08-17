/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;

import java.util.Collections;

/**
 * Translates {@code avg_bucket} to an {@code AVG} aggregate call over the resolved metric
 * column. An empty sibling yields SQL NULL, mapped to {@code NaN} which
 * {@link InternalSimpleValue} renders as {@code "value": null} — vanilla's count==0 behavior.
 */
public class AvgBucketTranslator implements PipelineTranslator<AvgBucketPipelineAggregationBuilder> {

    /** Creates an avg_bucket translator. */
    public AvgBucketTranslator() {}

    @Override
    public Class<AvgBucketPipelineAggregationBuilder> getBuilderClass() {
        return AvgBucketPipelineAggregationBuilder.class;
    }

    @Override
    public AggregateCall createAggregateCall(AvgBucketPipelineAggregationBuilder builder, int inputColumn, RelDataTypeFactory typeFactory) {
        // Declared DOUBLE so the engine's AVG decomposition divides in floating point
        // even when the metric column is integral.
        RelDataType doubleType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
        return AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            Collections.singletonList(inputColumn),
            -1,
            RelCollations.EMPTY,
            doubleType,
            builder.getName()
        );
    }

    @Override
    public InternalAggregation toInternalAggregation(AvgBucketPipelineAggregationBuilder builder, Object cellValue) {
        double value = cellValue instanceof Number number ? number.doubleValue() : Double.NaN;
        return new InternalSimpleValue(builder.getName(), value, PipelineTranslator.resolveFormat(builder), null);
    }
}
