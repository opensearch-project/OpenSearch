/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.opensearch.dsl.aggregation.LiteralColumnAllocator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base class for simple metric translators (single value: AVG, SUM, MIN, MAX, COUNT).
 * Provides default implementations for single-value metrics, including the shared
 * {@code missing} handling (aggregate over {@code COALESCE(field, missing)}) and
 * {@code format} validation.
 */
public abstract class AbstractMetricTranslator<T extends ValuesSourceAggregationBuilder<T>> implements MetricTranslator<T> {

    /** Creates a metric translator. */
    protected AbstractMetricTranslator() {}

    /**
     * Rejects request parameters the analytics path cannot honor, before any plan state
     * accumulates — see {@link MetricTranslator#validateSupportedParams}.
     */
    @Override
    public void validate(T agg) throws ConversionException {
        MetricTranslator.validateSupportedParams(agg);
    }

    /** Returns the SQL aggregate function (e.g., AVG, SUM, MIN, MAX). */
    protected abstract SqlAggFunction getAggFunction();

    /**
     * Returns the field name from the aggregation builder.
     *
     * @param agg the aggregation builder
     * @return the field name
     */
    protected abstract String getFieldName(T agg);

    /** Whether the field must be numeric; count-like metrics override to accept any type. */
    protected boolean requiresNumericField() {
        return true;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(T agg, RelDataType rowType, LiteralColumnAllocator literals) throws ConversionException {
        MetricTranslator.validateFormat(agg.format(), agg.getName());
        String fieldName = getFieldName(agg);
        RelDataTypeField field;
        if (requiresNumericField()) {
            field = MetricTranslator.resolveNumericField(rowType, fieldName, agg.getType());
        } else {
            field = rowType.getField(fieldName, false, false);
            if (field == null) {
                throw new ConversionException("Aggregation field '" + fieldName + "' not found in schema");
            }
        }

        // Calcite enforces the return type to be same as input type; eg: AVG int→double coercion happens in response layer.
        AggregateCall call = AggregateCall.create(
            getAggFunction(),
            false,
            false,
            false,
            Collections.singletonList(inputColumn(agg, field, literals)),
            -1,
            RelCollations.EMPTY,
            field.getType(),
            agg.getName()
        );
        return Collections.singletonList(call);
    }

    /**
     * The aggregate's input column: the field, or {@code COALESCE(field, missing)} when the
     * request sets {@code missing}. The coalesced column keeps the field's value type, so
     * declared call types are unaffected. {@code literals} may be null only when missing is unset.
     */
    static int inputColumn(ValuesSourceAggregationBuilder<?> agg, RelDataTypeField field, LiteralColumnAllocator literals)
        throws ConversionException {
        if (agg.missing() == null) {
            return field.getIndex();
        }
        return literals.coalescedColumnFor(field.getIndex(), MetricTranslator.missingValue(agg.missing(), agg.getName()));
    }

    @Override
    public List<String> getAggregateFieldNames(T agg) {
        return Collections.singletonList(agg.getName());
    }

    /** Extracts this metric's single output cell ({@code null} when the map is null or the cell is SQL NULL). */
    protected Object singleValue(T agg, Map<String, Object> values) {
        return values == null ? null : values.get(agg.getName());
    }

    /**
     * Coerces an engine result cell to double. Calcite keeps the input column type (AVG over
     * an INTEGER column returns an integral value), so the int→double widening happens here.
     *
     * @param value the raw cell value (must be a {@link Number})
     */
    protected static double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw new IllegalStateException(
            "Expected numeric aggregation result but got " + (value == null ? "null" : value.getClass().getSimpleName())
        );
    }
}
