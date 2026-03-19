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
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Converts a {@link MatchAllQueryBuilder} to a boolean TRUE literal.
 * A top-level match_all produces no filter (table scan returns all rows),
 * but match_all can appear nested inside bool queries and needs a translator.
 */
public class MatchAllQueryTranslator implements QueryTranslator {

    /** Creates a new match-all query translator. */
    public MatchAllQueryTranslator() {}

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return MatchAllQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) {
        return ctx.getRexBuilder().makeLiteral(true);
    }
}
