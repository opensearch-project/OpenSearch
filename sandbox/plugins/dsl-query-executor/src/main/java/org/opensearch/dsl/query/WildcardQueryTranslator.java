/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

/**
 * Converts a {@link WildcardQueryBuilder} to a WILDCARD_QUERY_DSL RexCall that delegates to Lucene
 * via the analytics backend serializer. The Lucene wildcard pattern is passed verbatim — no
 * SQL LIKE conversion or escape manipulation occurs. The {@code rewrite} parameter is passed
 * through when present; validation occurs on the data node via {@code QueryParsers.parseRewriteMethod}.
 */
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
        // _name is intentionally not read — matched_queries is not surfaced by this path.
        // Reject-vs-ignore convention is unsettled family-wide (TermsQueryTranslator:44 rejects it).

        return DelegatedRelevanceCallHelper.buildDelegatedRelevanceCall(
            "WILDCARD_QUERY_DSL",
            "wildcard",
            wildcardQuery.fieldName(),
            wildcardQuery.value(),
            wildcardQuery.caseInsensitive(),
            wildcardQuery.rewrite(),
            ctx
        );
    }
}
