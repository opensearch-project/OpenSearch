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
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

/** Translates a {@link WildcardQueryBuilder} to a WILDCARD_QUERY_DSL delegated-relevance RexCall. */
public class WildcardQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return WildcardQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        WildcardQueryBuilder wildcardQuery = (WildcardQueryBuilder) query;

        if (wildcardQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Wildcard query parameter 'boost' is not supported");
        }
        // matched_queries is not surfaced by this path
        if (wildcardQuery.queryName() != null) {
            throw new ConversionException("Wildcard query parameter '_name' is not supported");
        }

        // MappedFieldType.wildcardQuery:309-317 — only keyword and text fields support wildcard queries
        RelDataTypeField field = ctx.getField(wildcardQuery.fieldName());
        if (field.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            throw new ConversionException(
                "Can only use wildcard queries on keyword and text fields - not on ["
                    + wildcardQuery.fieldName()
                    + "] which is of type ["
                    + field.getType().getSqlTypeName()
                    + "]"
            );
        }

        return DelegatedRelevanceCallHelper.buildDelegatedRelevanceCall(
            "WILDCARD_QUERY_DSL",
            field,
            wildcardQuery.value(),
            wildcardQuery.caseInsensitive(),
            wildcardQuery.rewrite(),
            ctx
        );
    }
}
