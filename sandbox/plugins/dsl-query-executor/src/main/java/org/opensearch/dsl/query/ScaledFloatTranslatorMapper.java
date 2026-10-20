/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.schema.ScaledFloatType;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;

import java.util.Optional;

/**
 * Translator mapper for {@code scaled_float} fields. Implements scaling via
 * {@code Math.round(value * factor)} inline.
 *
 * <p>This mapper is a stateless singleton shared across every {@code scaled_float} field in
 * the schema. The scaling factor is read from {@link ScaledFloatType#getScalingFactor()} on
 * each call, never cached on this instance. Legacy {@code ScaledFloatFieldMapper.java:176}
 * holds {@code scalingFactor} as an instance field because there the object IS the field;
 * our mapper serves every scaled_float field, so a cached factor would corrupt a second
 * field with a different factor.
 */
final class ScaledFloatTranslatorMapper extends BaseTranslatorMapper {

    /** Singleton instance. */
    static final ScaledFloatTranslatorMapper INSTANCE = new ScaledFloatTranslatorMapper();

    private ScaledFloatTranslatorMapper() {}

    /**
     * Translates a single range bound for a scaled_float field.
     * Scales via {@code Math.round(value * factor)} then applies integer inclusivity adjustment
     * per legacy {@code ScaledFloatFieldMapper.ScaledFloatFieldType.rangeQuery} and
     * {@code NumberFieldMapper.longRangeQuery} semantics.
     */
    @Override
    protected RexNode translateBound(Object value, boolean isLower, boolean inclusive, RelDataTypeField field, ConversionContext ctx)
        throws ConversionException {
        if (value == null) {
            return null;
        }

        ScaledFloatType sft = (ScaledFloatType) field.getType();
        long scaledBound = scaleBound(value, sft, field.getName());

        // Inclusivity adjustment per NumberFieldMapper.longRangeQuery: exclusive bounds
        // increment (lower) or decrement (upper) to make them inclusive.
        if (!inclusive) {
            if (isLower) {
                if (scaledBound == Long.MAX_VALUE) {
                    return ctx.getRexBuilder().makeLiteral(false);
                }
                scaledBound++;
            } else {
                if (scaledBound == Long.MIN_VALUE) {
                    return ctx.getRexBuilder().makeLiteral(false);
                }
                scaledBound--;
            }
        }

        RexNode literal = ctx.getRexBuilder().makeLiteral(scaledBound, field.getType(), true);
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        SqlOperator op = isLower ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        return ctx.getRexBuilder().makeCall(op, fieldRef, literal);
    }

    /**
     * Converts one value to a scaled long literal for term/terms queries.
     * Mirrors {@code ScaledFloatFieldMapper.ScaledFloatFieldType.termQuery}: scales via
     * {@code Math.round(value * factor)} then builds an exact equality literal.
     */
    @Override
    public Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException {
        ScaledFloatType sft = (ScaledFloatType) field.getType();
        long scaledValue = scaleToLong(value, sft.getScalingFactor(), field.getName());
        RexNode literal = ctx.getRexBuilder().makeLiteral(scaledValue, field.getType(), true);
        return Optional.of(literal);
    }

    /**
     * Scales a bound value for a scaled_float field per
     * {@code ScaledFloatFieldMapper.ScaledFloatFieldType.rangeQuery} semantics:
     * {@code Math.round(doubleValue * scalingFactor)}.
     *
     * @throws ConversionException if the value is non-numeric, non-finite, or the scaled result overflows Long
     */
    private static long scaleBound(Object bound, ScaledFloatType sft, String fieldName) throws ConversionException {
        double doubleValue = parseFiniteDouble(bound, fieldName);
        double scaled = doubleValue * sft.getScalingFactor();
        if (Double.isNaN(scaled) || Double.isInfinite(scaled) || scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
            throw new ConversionException(
                "Scaled value overflows Long range for field '" + fieldName + "': " + bound + " * " + sft.getScalingFactor()
            );
        }
        return Math.round(scaled);
    }

    /**
     * Scales a term/terms value to a long for scaled_float equality queries.
     * Reached from {@link TermQueryTranslator} and {@link TermsQueryTranslator} via toTermLiteral.
     *
     * @throws ConversionException if the value is non-numeric, non-finite, or overflows Long after scaling
     */
    private static long scaleToLong(Object value, double factor, String fieldName) throws ConversionException {
        double doubleValue = parseFiniteDouble(value, fieldName);
        double scaled = doubleValue * factor;
        if (Double.isNaN(scaled) || Double.isInfinite(scaled) || scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
            throw new ConversionException("Scaled value overflows Long range for field '" + fieldName + "': " + value + " * " + factor);
        }
        return Math.round(scaled);
    }

    /**
     * Parses an object to a finite double, rejecting NaN and Infinity.
     *
     * @throws ConversionException if the value is non-numeric or non-finite
     */
    private static double parseFiniteDouble(Object value, String fieldName) throws ConversionException {
        double doubleValue;
        if (value instanceof Number) {
            doubleValue = ((Number) value).doubleValue();
        } else {
            try {
                doubleValue = Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                throw new ConversionException("Non-numeric term value for scaled_float field '" + fieldName + "': " + value);
            }
        }
        if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
            throw new ConversionException("Non-finite value for scaled_float field '" + fieldName + "': " + value);
        }
        return doubleValue;
    }
}
