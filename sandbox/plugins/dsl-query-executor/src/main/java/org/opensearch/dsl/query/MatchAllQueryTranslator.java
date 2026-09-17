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
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Converts a {@link MatchAllQueryBuilder} to a boolean TRUE literal.
 * A plain top-level match_all produces no filter (the table scan returns all rows), but it
 * still reaches this translator when nested in a bool query, or when it carries an option
 * this path cannot honour.
 */
public class MatchAllQueryTranslator implements QueryTranslator {

    /** Creates a new match-all query translator. */
    public MatchAllQueryTranslator() {}

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return MatchAllQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        MatchAllQueryBuilder matchAllQuery = (MatchAllQueryBuilder) query;

        if (matchAllQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Match_all query parameter 'boost' is not supported");
        }
        // matched_queries is not surfaced by this path
        if (matchAllQuery.queryName() != null) {
            throw new ConversionException("Match_all query parameter '_name' is not supported");
        }

        return ctx.getRexBuilder().makeLiteral(true);
    }
}
