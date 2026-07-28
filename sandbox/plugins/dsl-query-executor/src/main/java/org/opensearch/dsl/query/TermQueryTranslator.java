/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.schema.ScaledFloatType;
import org.opensearch.analytics.schema.UnsignedLongType;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

/**
 * Converts a {@link TermQueryBuilder} to a Calcite EQUALS RexNode.
 * {@code {"term": {"status": "active"}}} becomes {@code status = 'active'}.
 *
 * <p>For {@code scaled_float} fields, the value is scaled via {@code Math.round(value * factor)}
 * before equality comparison — mirroring
 * {@code ScaledFloatFieldMapper.ScaledFloatFieldType.termQuery}.
 */
public class TermQueryTranslator implements QueryTranslator {

    /** Creates a new term query translator. */
    public TermQueryTranslator() {}

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return TermQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        TermQueryBuilder termQuery = (TermQueryBuilder) query;
        String fieldName = termQuery.fieldName();
        Object value = termQuery.value();

        RexNode fieldRef = ctx.makeFieldRef(fieldName);
        RelDataType fieldType = ctx.getField(fieldName).getType();

        if (fieldType instanceof ScaledFloatType sft) {
            // ScaledFloatFieldMapper.ScaledFloatFieldType.termQuery: Math.round(value * factor)
            // then delegates to NumberFieldMapper.NumberType.LONG.termQuery for exact equality.
            long scaledValue = RangeBoundMath.scaleToLong(value, sft.getScalingFactor(), fieldName);
            RexNode literal = ctx.getRexBuilder().makeLiteral(scaledValue, fieldType, true);
            return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
        }

        if (fieldType instanceof UnsignedLongType) {
            // NumberFieldMapper.NumberType.UNSIGNED_LONG.termQuery: negative/decimal → match-none;
            // above Long.MAX → ConversionException.
            Long unsignedValue = RangeBoundMath.parseUnsignedLongTerm(value, fieldName);
            if (unsignedValue == null) {
                return ctx.getRexBuilder().makeLiteral(false);
            }
            long longVal = unsignedValue;
            RexNode literal = ctx.getRexBuilder().makeLiteral(longVal, fieldType, true);
            return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
        }

        RexNode literal = ctx.getRexBuilder().makeLiteral(value, fieldType, true);
        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
    }

}
