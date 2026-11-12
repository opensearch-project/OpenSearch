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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/** Translates a {@link PrefixQueryBuilder} to a PREFIX_QUERY delegated-relevance RexCall. */
public class PrefixQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return PrefixQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        PrefixQueryBuilder prefixQuery = (PrefixQueryBuilder) query;

        rejectScoringParams(prefixQuery, "Prefix");

        // MappedFieldType.prefixQuery:291-297 — only keyword and text fields support prefix queries
        RelDataTypeField field = ctx.getField(prefixQuery.fieldName());
        if (field.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            throw new ConversionException(
                "Can only use prefix queries on keyword and text fields - not on ["
                    + prefixQuery.fieldName()
                    + "] which is of type ["
                    + field.getType().getSqlTypeName()
                    + "]"
            );
        }

        return DelegatedRelevanceCallHelper.buildDelegatedRelevanceCall(
            "PREFIX_QUERY",
            field,
            prefixQuery.value(),
            prefixQuery.caseInsensitive(),
            prefixQuery.rewrite(),
            ctx
        );
    }
}
