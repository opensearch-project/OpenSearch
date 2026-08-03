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
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;

import java.util.Optional;

/**
 * Translator mapper for {@code unsigned_long} fields. Delegates bound translation to
 * {@link RangeBoundMath#translateUnsignedLongBound} and term parsing to
 * {@link RangeBoundMath#parseUnsignedLongTerm}.
 *
 * <p>This mapper is a stateless singleton shared across every {@code unsigned_long} field in
 * the schema. No per-field state is held; all parameters are derived from the
 * {@code RelDataType} on each call.
 */
final class UnsignedLongTranslatorMapper extends BaseTranslatorMapper {

    /** Singleton instance. */
    static final UnsignedLongTranslatorMapper INSTANCE = new UnsignedLongTranslatorMapper();

    private UnsignedLongTranslatorMapper() {}

    /**
     * Translates a single range bound for an unsigned_long field.
     * Delegates to {@link RangeBoundMath#translateUnsignedLongBound} which applies legacy
     * {@code NumberFieldMapper.unsignedLongRangeQuery} semantics: negative clamping,
     * decimal truncation, and overflow guards.
     */
    @Override
    protected RexNode translateBound(Object value, boolean isLower, boolean inclusive, RelDataTypeField field, ConversionContext ctx)
        throws ConversionException {
        return RangeBoundMath.translateUnsignedLongBound(value, isLower, inclusive, field, ctx);
    }

    /**
     * Converts one value to a typed literal for unsigned_long term/terms queries.
     * Mirrors {@code NumberFieldMapper.NumberType.UNSIGNED_LONG.termQuery}: fractional values
     * can never match a whole-number document value, so return {@code Optional.empty()} for them
     * (match-none semantics). Negative values also return empty.
     */
    @Override
    public Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException {
        Long unsignedValue = RangeBoundMath.parseUnsignedLongTerm(value, field.getName());
        if (unsignedValue == null) {
            return Optional.empty();
        }
        long longVal = unsignedValue;
        RexNode literal = ctx.getRexBuilder().makeLiteral(longVal, field.getType(), true);
        return Optional.of(literal);
    }
}
