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
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Converts a {@link PrefixQueryBuilder} to a PREFIX_QUERY RexCall that delegates to Lucene
 * via the analytics backend serializer. The prefix value is passed verbatim — no SQL LIKE
 * escaping or pattern construction occurs. The {@code rewrite} parameter is passed through
 * when present; validation occurs on the data node via {@code QueryParsers.parseRewriteMethod}.
 */
public class PrefixQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return PrefixQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        PrefixQueryBuilder prefixQuery = (PrefixQueryBuilder) query;

        if (prefixQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Prefix query parameter 'boost' is not supported");
        }
        // _name is intentionally not read — matched_queries is not surfaced by this path.
        // Reject-vs-ignore convention is unsettled family-wide (TermsQueryTranslator:44 rejects it).

        return DelegatedRelevanceCallHelper.buildDelegatedRelevanceCall(
            "PREFIX_QUERY",
            "prefix",
            prefixQuery.fieldName(),
            prefixQuery.value(),
            prefixQuery.caseInsensitive(),
            prefixQuery.rewrite(),
            ctx
        );
    }
}
