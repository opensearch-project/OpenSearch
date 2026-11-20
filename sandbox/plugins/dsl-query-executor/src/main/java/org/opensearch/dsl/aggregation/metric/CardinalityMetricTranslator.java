/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed
 * under the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;

import java.util.Map;

/** Translates field-based cardinality aggregations to distributed approximate distinct counts. */
public class CardinalityMetricTranslator extends AbstractMetricTranslator<CardinalityAggregationBuilder> {

    /** Creates a cardinality metric translator. */
    public CardinalityMetricTranslator() {}

    @Override
    public Class<CardinalityAggregationBuilder> getAggregationType() {
        return CardinalityAggregationBuilder.class;
    }

    @Override
    public void validate(CardinalityAggregationBuilder agg) throws ConversionException {
        super.validate(agg);
        if (agg.precisionThreshold() != null) {
            throw new ConversionException(
                "[precision_threshold] on cardinality aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
        if (agg.executionHint() != null) {
            throw new ConversionException(
                "[execution_hint] on cardinality aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
    }

    @Override
    protected String getFieldName(CardinalityAggregationBuilder agg) {
        return agg.field();
    }

    @Override
    protected RelDataType getReturnType(CardinalityAggregationBuilder agg, RelDataTypeField field, RelDataTypeFactory typeFactory) {
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
    }

    @Override
    public InternalAggregation toInternalAggregation(String name, Object value, Map<String, Object> metadata) {
        if (value == null) {
            return new InternalAnalyticsCardinality(name, 0L, metadata);
        }
        if (value instanceof Number number) {
            return new InternalAnalyticsCardinality(name, number.longValue(), metadata);
        }
        throw new IllegalStateException("Expected numeric cardinality result but got " + value.getClass().getSimpleName());
    }
}
