/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.Objects;

/**
 * Converts the DSL {@code query} clause to a {@link LogicalFilter}.
 * Skips a parameter-free match_all since a table scan already returns all rows;
 * a match_all carrying {@code boost} or {@code _name} still produces a filter so those params are rejected.
 */
public class FilterConverter extends AbstractDslConverter {

    private final QueryRegistry queryRegistry;

    /**
     * Creates a filter converter.
     *
     * @param queryRegistry registry of query translators
     */
    public FilterConverter(QueryRegistry queryRegistry) {
        this.queryRegistry = Objects.requireNonNull(queryRegistry, "queryRegistry must not be null");
    }

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        QueryBuilder query = ctx.getSearchSource().query();
        if (query == null) {
            return false;
        }
        if (query instanceof MatchAllQueryBuilder matchAll) {
            // A table scan already returns all rows, so a bare match_all needs no filter; but a match_all
            // carrying boost or _name must reach its translator to be rejected rather than silently dropped.
            return matchAll.boost() != AbstractQueryBuilder.DEFAULT_BOOST || matchAll.queryName() != null;
        }
        return true;
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        RexNode condition = queryRegistry.convert(ctx.getSearchSource().query(), ctx);
        return LogicalFilter.create(input, condition);
    }
}
