/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.LiteralColumnAllocator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Translates a metric aggregation to Calcite AggregateCall(s),
 * and converts raw result values back to OpenSearch InternalAggregation for response building.
 */
public interface MetricTranslator<T extends AggregationBuilder> extends AggregationTranslator<T> {

    /**
     * Resolves {@code fieldName} and requires a numeric column type, rejecting others with the
     * classic path's {@code illegal_argument_exception} shape. Date and boolean columns are
     * rejected too (unlike classic search) until the analytics path implements them.
     *
     * @param rowType the index row type
     * @param fieldName the aggregated field
     * @param aggregationType the request aggregation type name for the error message (e.g. "avg")
     * @return the resolved numeric field
     * @throws ConversionException if the field does not exist
     */
    static RelDataTypeField resolveNumericField(RelDataType rowType, String fieldName, String aggregationType) throws ConversionException {
        RelDataTypeField field = resolveField(rowType, fieldName);
        SqlTypeName type = field.getType().getSqlTypeName();
        if (SqlTypeName.NUMERIC_TYPES.contains(type) == false) {
            throw new IllegalArgumentException(
                "Field [" + fieldName + "] of type [" + type + "] is not supported for aggregation [" + aggregationType + "]"
            );
        }
        return field;
    }

    /**
     * Resolves {@code fieldName} against the index row type, accepting any column type.
     *
     * @param rowType the index row type
     * @param fieldName the aggregated field
     * @return the resolved field
     * @throws ConversionException if the field does not exist
     */
    static RelDataTypeField resolveField(RelDataType rowType, String fieldName) throws ConversionException {
        RelDataTypeField field = rowType.getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Aggregation field '" + fieldName + "' not found in schema");
        }
        return field;
    }

    /**
     * Resolves the metric's {@link DocValueFormat} from the index mapping, honoring the
     * request's {@code format} pattern when present — the same precedence classic search
     * applies in {@code ValuesSourceConfig.resolveFormat}. Fails loudly when the mapping
     * or the field cannot be resolved, matching the terms rendering contract.
     *
     * @param mapperServiceSupplier supplies the target index's MapperService
     * @param field the aggregated field
     * @param pattern the request's format pattern, or null/empty for the mapping's format
     * @param aggName the aggregation name for error messages
     * @return the resolved format
     */
    static DocValueFormat resolveFormat(Supplier<MapperService> mapperServiceSupplier, String field, String pattern, String aggName) {
        MapperService mapperService = mapperServiceSupplier == null ? null : mapperServiceSupplier.get();
        if (mapperService == null) {
            throw new IllegalStateException(
                "index mapping unavailable for metric aggregation ["
                    + aggName
                    + "] — cannot resolve the value format for field ["
                    + field
                    + "]"
            );
        }
        MappedFieldType fieldType = mapperService.fieldType(field);
        if (fieldType == null) {
            throw new IllegalStateException(
                "field [" + field + "] of metric aggregation [" + aggName + "] is not present in the index mapping"
            );
        }
        return fieldType.docValueFormat(pattern == null || pattern.isEmpty() ? null : pattern, null);
    }

    /**
     * Validates the {@code format} pattern so an invalid pattern fails at conversion,
     * before any engine work.
     *
     * @param pattern the request's format pattern
     * @param aggName the aggregation name for the error message
     * @throws ConversionException if the pattern is not a valid decimal format
     */
    static void validateFormat(String pattern, String aggName) throws ConversionException {
        if (pattern == null || pattern.isEmpty()) {
            return;
        }
        try {
            new DocValueFormat.Decimal(pattern);
        } catch (IllegalArgumentException e) {
            throw new ConversionException("aggregation [" + aggName + "] has an invalid format [" + pattern + "]: " + e.getMessage());
        }
    }

    /**
     * Rejects request parameters the analytics path cannot honor. Scripted values are
     * computed per document at collection time, which has no SQL equivalent.
     *
     * @param agg the metric aggregation builder
     * @throws ConversionException if the aggregation uses a script
     */
    static void validateSupportedParams(ValuesSourceAggregationBuilder<?> agg) throws ConversionException {
        if (agg.script() != null) {
            throw new ConversionException("aggregation [" + agg.getName() + "] with a script is not supported");
        }
    }

    /**
     * Coerces the request's {@code missing} value to a double for the COALESCE substitute
     * column; non-numeric substitutes are rejected.
     *
     * @param missing the request's missing value (non-null)
     * @param aggName the aggregation name for the error message
     * @return the substitute as a double
     * @throws ConversionException if the value is not a number or numeric string
     */
    static double missingValue(Object missing, String aggName) throws ConversionException {
        if (missing instanceof Number number) {
            return number.doubleValue();
        }
        if (missing instanceof String s) {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException e) {
                // fall through to the shared error below
            }
        }
        throw new ConversionException("aggregation [" + aggName + "] has a non-numeric missing value [" + missing + "]");
    }

    /**
     * Converts the metric aggregation to Calcite AggregateCall(s). Constants allocated
     * through {@code literals} — aggregate-call literal arguments or the {@code missing}
     * substitute's COALESCE column — become input columns of the aggregate. Callers run
     * {@link #validate} first (the tree walker does, once per aggregation); this method
     * assumes an already-validated request.
     *
     * @param agg the metric aggregation builder
     * @param rowType the index row type for field lookup
     * @param literals allocator for literal-derived input columns
     * @return list of Calcite AggregateCalls
     * @throws ConversionException if conversion fails
     */
    List<AggregateCall> toAggregateCalls(T agg, RelDataType rowType, LiteralColumnAllocator literals) throws ConversionException;

    /**
     * Literal-free convenience variant for callers with no {@link LiteralColumnAllocator}
     * (tests, mostly). Requests whose translation would need a literal column —
     * {@code missing} is the one such parameter today — are rejected here rather than
     * silently mistranslated.
     *
     * @param agg the metric aggregation builder
     * @param rowType the index row type for field lookup
     * @return list of Calcite AggregateCalls
     * @throws ConversionException if conversion fails
     */
    default List<AggregateCall> toAggregateCalls(T agg, RelDataType rowType) throws ConversionException {
        if (agg instanceof ValuesSourceAggregationBuilder<?> vs && vs.missing() != null) {
            throw new ConversionException("aggregation [" + agg.getName() + "] with a missing value requires literal column support");
        }
        return toAggregateCalls(agg, rowType, null);
    }

    /**
     * Returns the output field names for this aggregation.
     *
     * @param agg the metric aggregation builder
     * @return list of aggregate field names
     */
    List<String> getAggregateFieldNames(T agg);

    // TODO: Revisit signature — accept a stream/iterator of <String,Object> for bulk conversion
    // to avoid per-row virtual dispatch overhead, and use Arrow-native types once Analytics Core
    // exposes them.
    /**
     * Converts raw result values from execution into an OpenSearch InternalAggregation.
     * The builder is passed (not just the name) so translators can honor request
     * parameters that shape the response, e.g. extended_stats sigma.
     *
     * @param agg the original aggregation builder from the request
     * @param values values keyed by {@link #getAggregateFieldNames} entries; {@code null}
     *               when execution produced no row for this metric's granularity
     * @return the corresponding InternalAggregation
     */
    InternalAggregation toInternalAggregation(T agg, Map<String, Object> values);
}
