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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.Optional;

/**
 * Converts a {@link TermQueryBuilder} to a Calcite EQUALS RexNode.
 * {@code {"term": {"status": "active"}}} becomes {@code status = 'active'}.
 *
 * <p>For {@code scaled_float} fields, the value is scaled via {@code Math.round(value * factor)}
 * before equality comparison — mirroring
 * {@code ScaledFloatFieldMapper.ScaledFloatFieldType.termQuery}.
 */
public class TermQueryTranslator implements QueryTranslator {

    private static final TranslatorMapperRegistry REGISTRY = TranslatorMapperRegistry.INSTANCE;

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

        if (termQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Term query parameter 'boost' is not supported");
        }
        // matched_queries is not surfaced by this path
        if (termQuery.queryName() != null) {
            throw new ConversionException("Term query parameter '_name' is not supported");
        }

        RelDataTypeField field = ctx.getField(fieldName);
        RelDataType fieldType = field.getType();
        RexNode fieldRef = ctx.makeFieldRef(fieldName);

        Optional<RexNode> literal = REGISTRY.resolve(fieldType).toTermLiteral(value, field, ctx);
        if (literal.isEmpty()) {
            // Value can never match (e.g. fractional unsigned_long) → match-none.
            return ctx.getRexBuilder().makeLiteral(false);
        }

        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal.get());
    }

}
