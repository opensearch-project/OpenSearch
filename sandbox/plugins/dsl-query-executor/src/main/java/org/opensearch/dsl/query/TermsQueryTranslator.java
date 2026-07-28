/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.schema.ScaledFloatType;
import org.opensearch.analytics.schema.UnsignedLongType;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.List;

/**
 * Converts a {@link TermsQueryBuilder} to a Calcite IN RexNode.
 *
 * <p>For {@code scaled_float} fields, each value is scaled via {@code Math.round(value * factor)}
 * before the IN comparison — mirroring
 * {@code ScaledFloatFieldMapper.ScaledFloatFieldType.termsQuery}.
 */
public class TermsQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return TermsQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {

        TermsQueryBuilder termsQuery = (TermsQueryBuilder) query;

        if (termsQuery.termsLookup() != null) {
            throw new ConversionException("Terms query does not support terms lookup");
        }
        if (termsQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Terms query does not support non-default boost");
        }
        if (termsQuery.queryName() != null) {
            throw new ConversionException("Terms query does not support _name");
        }
        if (termsQuery.valueType() != TermsQueryBuilder.ValueType.DEFAULT) {
            throw new ConversionException("Terms query does not support non-default value_type");
        }

        String fieldName = termsQuery.fieldName();
        List<?> values = termsQuery.values();

        if (values == null || values.isEmpty()) {
            throw new ConversionException("Terms query must have values");
        }

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }

        RelDataType fieldType = field.getType();
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(fieldType, field.getIndex());

        if (fieldType instanceof ScaledFloatType sft) {
            // ScaledFloatFieldMapper.ScaledFloatFieldType.termsQuery: Math.round(value * factor)
            // for each value, then delegates to NumberFieldMapper.NumberType.LONG.termsQuery.
            List<RexNode> literals = new java.util.ArrayList<>();
            for (Object value : values) {
                long scaledValue = RangeBoundMath.scaleToLong(value, sft.getScalingFactor(), fieldName);
                literals.add(ctx.getRexBuilder().makeLiteral(scaledValue, fieldType, true));
            }
            return ctx.getRexBuilder().makeIn(fieldRef, literals);
        }

        if (fieldType instanceof UnsignedLongType) {
            // NumberFieldMapper.NumberType.UNSIGNED_LONG.termsQuery: skip negative/decimal values.
            List<RexNode> literals = new java.util.ArrayList<>();
            for (Object value : values) {
                Long parsed = RangeBoundMath.parseUnsignedLongTerm(value, fieldName);
                if (parsed != null) {
                    long longVal = parsed;
                    literals.add(ctx.getRexBuilder().makeLiteral(longVal, fieldType, true));
                }
            }
            if (literals.isEmpty()) {
                return ctx.getRexBuilder().makeLiteral(false);
            }
            return ctx.getRexBuilder().makeIn(fieldRef, literals);
        }

        List<RexNode> literals = new java.util.ArrayList<>();
        for (Object value : values) {
            literals.add(ctx.getRexBuilder().makeLiteral(value, fieldType, true));
        }

        return ctx.getRexBuilder().makeIn(fieldRef, literals);
    }

}
