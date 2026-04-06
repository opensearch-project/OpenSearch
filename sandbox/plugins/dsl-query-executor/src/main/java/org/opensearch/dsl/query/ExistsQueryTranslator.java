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
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Converts an {@link ExistsQueryBuilder} to a Calcite IS NOT NULL RexNode.
 */
public class ExistsQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return ExistsQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        ExistsQueryBuilder existsQuery = (ExistsQueryBuilder) query;
        String fieldName = existsQuery.fieldName();
        float boost = existsQuery.boost();

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }
        if (boost != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("boost is unsupported for Exists query type");
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldRef);
    }
}
