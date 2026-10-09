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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.lucene.util.BytesRef;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.range.RangeBoundMath;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

/**
 * Catch-all translator mapper carrying today's entire non-UDT bound-translation path:
 * the integer decimal truncate-and-adjust branch, the whole-integer branch, and the
 * permissive generic tail that builds comparisons for any remaining type (including
 * VARCHAR/CHAR keyword ranges).
 *
 * <p>This mapper is NOT gated on {@code RangeBoundMath.isNumericType} because VARCHAR/CHAR
 * keyword ranges are served only by the generic tail ({@code processValue} returns the
 * string unchanged), so a numeric gate would break keyword range queries.
 *
 * <p>Stateless singleton; per-field state is read from the {@code RelDataType} on each call.
 */
final class DefaultTranslatorMapper extends BaseTranslatorMapper {

    /** Singleton instance. */
    static final DefaultTranslatorMapper INSTANCE = new DefaultTranslatorMapper();

    private DefaultTranslatorMapper() {}

    /**
     * Translates a single range bound into a comparison RexNode.
     * Rejects non-finite values (NaN, Infinity) on integer and float fields per legacy
     * {@code NumberFieldMapper.NumberType.INTEGER.rangeQuery} and
     * {@code NumberFieldMapper.NumberType.FLOAT.rangeQuery} semantics, and applies
     * overflow guards for integer-typed fields matching legacy
     * {@code NumberFieldMapper.NumberType.INTEGER.rangeQuery} IllegalArgumentException
     * on out-of-range values. DOUBLE fields accept non-finite values unchanged per legacy.
     *
     * <p>NOTE: Legacy byte/short/integer NumberFieldMapper.hasDecimalPart silently produces a
     * bound of 0 for NaN (a pre-existing legacy bug). We deliberately do NOT replicate that
     * bug; legacy DOES throw for long via Numbers.toLongExact. For FLOAT/HALF_FLOAT, legacy
     * NumberFieldMapper throws "supports only finite values".
     */
    @Override
    protected RexNode translateBound(Object value, boolean isLower, boolean inclusive, RelDataTypeField field, ConversionContext ctx)
        throws ConversionException {
        if (value == null) {
            return null;
        }

        SqlTypeName fieldTypeName = field.getType().getSqlTypeName();

        // WHY: NaN/Infinity on integer fields silently become 0 or Long.MIN/MAX_VALUE via
        // Double.longValue() (IEEE-754 "round toward zero" is undefined for non-finite).
        // On REAL/FLOAT fields, legacy NumberFieldMapper throws "supports only finite values".
        // DOUBLE deliberately accepts non-finite per legacy behaviour.
        if (RangeBoundMath.isNonFinite(value)) {
            if (RangeBoundMath.isIntegerType(fieldTypeName)) {
                throw new ConversionException(
                    "Non-finite value (" + value + ") is not supported for integer field '" + field.getName() + "'"
                );
            }
            if (fieldTypeName == SqlTypeName.REAL || fieldTypeName == SqlTypeName.FLOAT) {
                throw new ConversionException(
                    "Non-finite value ("
                        + value
                        + ") is not supported for float field '"
                        + field.getName()
                        + "' (legacy NumberFieldMapper supports only finite values)"
                );
            }
            // DOUBLE: fall through — legacy accepts non-finite doubles
        }

        Object adjusted = value;
        boolean adjustedInclusive = inclusive;
        final boolean isInteger = RangeBoundMath.isIntegerType(fieldTypeName);
        final boolean hasDecimal = isInteger && NumberFieldMapper.NumberType.hasDecimalPart(value);

        // Decimal bounds on integer fields per NumberFieldMapper INTEGER.rangeQuery:
        // truncate to int and adjust based on sign and bound direction.
        if (isInteger && hasDecimal) {
            long truncated = RangeBoundMath.toLongValue(value);
            if (isLower) {
                // Positive decimal lower bound -> increment
                if (NumberFieldMapper.NumberType.signum(value) > 0) {
                    if (truncated >= RangeBoundMath.getMaxValueForType(fieldTypeName)) {
                        return ctx.getRexBuilder().makeLiteral(false);
                    }
                    adjusted = RangeBoundMath.narrowToFieldType(truncated + 1, fieldTypeName);
                } else {
                    adjusted = RangeBoundMath.narrowToFieldType(truncated, fieldTypeName);
                }
            } else {
                // Negative decimal upper bound -> decrement
                if (NumberFieldMapper.NumberType.signum(value) < 0) {
                    if (truncated <= RangeBoundMath.getMinValueForType(fieldTypeName)) {
                        return ctx.getRexBuilder().makeLiteral(false);
                    }
                    adjusted = RangeBoundMath.narrowToFieldType(truncated - 1, fieldTypeName);
                } else {
                    adjusted = RangeBoundMath.narrowToFieldType(truncated, fieldTypeName);
                }
            }
            adjustedInclusive = true; // decimal adjustment makes bound inclusive
        } else if (isInteger && !hasDecimal && value instanceof Number) {
            // Whole numeric value on integer field: range-checked narrow to field-appropriate type.
            // WHY: unchecked (int) cast silently truncates via JLS 5.1.3 narrowing, e.g.
            // 2147483648L becomes -2147483648 and matches everything.
            RangeBoundMath.CheckedNarrow narrowed = RangeBoundMath.narrowChecked(
                ((Number) value).longValue(),
                fieldTypeName,
                isLower,
                field.getName()
            );
            switch (narrowed.result()) {
                case MATCH_NONE:
                    return ctx.getRexBuilder().makeLiteral(false);
                case NO_CONSTRAINT:
                    return null;
                case OK:
                    adjusted = narrowed.value();
                    break;
            }
        }

        RexNode literal = createLiteral(adjusted, field, ctx, fieldTypeName);
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        SqlOperator op;
        if (isLower) {
            op = adjustedInclusive ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.GREATER_THAN;
        } else {
            op = adjustedInclusive ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN;
        }

        return ctx.getRexBuilder().makeCall(op, fieldRef, literal);
    }

    /**
     * Generic term-literal behaviour: creates a typed literal using the field's type.
     */
    @Override
    public Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException {
        SqlTypeName fieldTypeName = field.getType().getSqlTypeName();
        // WHY: A String/BytesRef skips the Number-gated guards below and crashes in Calcite; normalise to BigDecimal so they
        // apply unchanged.
        if ((fieldTypeName == SqlTypeName.TINYINT
            || fieldTypeName == SqlTypeName.SMALLINT
            || fieldTypeName == SqlTypeName.INTEGER
            || fieldTypeName == SqlTypeName.BIGINT) && (value instanceof String || value instanceof BytesRef)) {
            String raw = (value instanceof BytesRef ? ((BytesRef) value).utf8ToString() : (String) value).trim();
            // Try the exact BigDecimal parse first so values above 2^53 keep full precision, then fall back to the lenient
            // Double.parseDouble path (whitespace, d/f suffixes, Infinity/NaN) only when BigDecimal rejects the input.
            try {
                value = new BigDecimal(raw);
            } catch (NumberFormatException exact) {
                try {
                    double parsed = Double.parseDouble(raw);
                    value = Double.isFinite(parsed) ? new BigDecimal(parsed) : parsed;
                } catch (NumberFormatException lenient) {
                    String capped = raw.length() > 64 ? raw.substring(0, 64) + "..." : raw;
                    throw new ConversionException(
                        "Value [" + capped + "] is not a valid number for " + fieldTypeName + " field '" + field.getName() + "'"
                    );
                }
            }
        }
        // Parity with NumberFieldMapper exact-integer termQuery: a fractional value can never match → drop it (match-none).
        if ((fieldTypeName == SqlTypeName.TINYINT
            || fieldTypeName == SqlTypeName.SMALLINT
            || fieldTypeName == SqlTypeName.INTEGER
            || fieldTypeName == SqlTypeName.BIGINT) && NumberFieldMapper.NumberType.hasDecimalPart(value)) {
            return Optional.empty();
        }
        // Parity with NumberFieldMapper range check: a whole value outside the integer domain rejects or matches none.
        if (RangeBoundMath.isIntegerType(fieldTypeName) && value instanceof Number) {
            long longValue;
            if (value instanceof BigInteger || value instanceof BigDecimal || value instanceof Double || value instanceof Float) {
                // Narrow via BigInteger so a whole Double/Float beyond long's range overflows the bitLength guard rather than saturating.
                BigInteger asInt;
                if (value instanceof BigDecimal) {
                    asInt = ((BigDecimal) value).toBigInteger();
                } else if (value instanceof BigInteger) {
                    asInt = (BigInteger) value;
                } else {
                    asInt = new BigDecimal(value.toString()).toBigInteger();
                }
                if (asInt.bitLength() > Long.SIZE - 1) {
                    if (fieldTypeName == SqlTypeName.BIGINT) {
                        throw new ConversionException("Value [" + value + "] is out of range for a long");
                    }
                    throw new ConversionException(
                        "Value " + value + " is out of range for " + fieldTypeName + " field '" + field.getName() + "'"
                    );
                }
                longValue = asInt.longValue();
            } else {
                longValue = ((Number) value).longValue();
            }
            RangeBoundMath.CheckedNarrow narrowed = RangeBoundMath.narrowChecked(longValue, fieldTypeName, true, field.getName());
            if (narrowed.result() != RangeBoundMath.NarrowResult.OK) {
                return Optional.empty();
            }
        }
        // Calcite rejects Double/Float literals for exact-integer target types; coerce the surviving whole value to BigDecimal.
        if ((fieldTypeName == SqlTypeName.TINYINT
            || fieldTypeName == SqlTypeName.SMALLINT
            || fieldTypeName == SqlTypeName.INTEGER
            || fieldTypeName == SqlTypeName.BIGINT) && (value instanceof Double || value instanceof Float)) {
            value = new BigDecimal(value.toString());
        }
        RexNode literal = ctx.getRexBuilder().makeLiteral(value, field.getType(), true);
        return Optional.of(literal);
    }

    /**
     * Creates a literal RexNode with appropriate type based on the field type and value.
     *
     * @param value the value to create a literal for
     * @param field the field definition from the schema
     * @param ctx the conversion context
     * @param fieldTypeName the SqlTypeName of the field
     * @return RexNode literal with appropriate type and precision
     */
    private RexNode createLiteral(Object value, RelDataTypeField field, ConversionContext ctx, SqlTypeName fieldTypeName) {
        return ctx.getRexBuilder().makeLiteral(value, field.getType(), true);
    }
}
