/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

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
    public ValidationResult validate(QueryBuilder query) {
        ExistsQueryBuilder existsQuery = (ExistsQueryBuilder) query;

        if (existsQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return ValidationResult.rejected("exists.boost", "boost is unsupported for Exists query type");
        }

        return ValidationResult.accepted();
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        ExistsQueryBuilder existsQuery = (ExistsQueryBuilder) query;
        ValidationResult validationResult = validate(existsQuery);
        if (!validationResult.isAccepted()) {
            throw new ConversionException(validationResult.message());
        }

        String fieldName = existsQuery.fieldName();
        RexNode fieldRef = ctx.makeFieldRef(fieldName);
        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldRef);
    }
}
