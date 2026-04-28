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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

/**
 * Converts a {@link TermQueryBuilder} to a Calcite EQUALS RexNode.
 * {@code {"term": {"status": "active"}}} becomes {@code status = 'active'}.
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

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        RexNode literal = ctx.getRexBuilder().makeLiteral(value, field.getType(), true);

        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, fieldRef, literal);
    }
}
