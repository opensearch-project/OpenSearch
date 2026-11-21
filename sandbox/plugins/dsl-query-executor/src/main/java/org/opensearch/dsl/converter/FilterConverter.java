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
 * Skips a plain match_all since a table scan already returns all rows; one carrying an
 * unsupported option still goes through so it can be rejected.
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
            // A plain match_all needs no filter — the table scan already returns all rows. One
            // carrying boost/_name must still reach its translator so the unsupported option is
            // rejected rather than silently dropped, matching how it behaves nested in a bool.
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
