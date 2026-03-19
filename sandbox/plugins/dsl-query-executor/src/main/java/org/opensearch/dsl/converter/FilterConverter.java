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
import org.opensearch.index.query.MatchAllQueryBuilder;

import java.util.Objects;

/**
 * Converts the DSL {@code query} clause to a {@link LogicalFilter}.
 * Skips match_all since a table scan already returns all rows.
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
        return ctx.getSearchSource().query() != null
            && !(ctx.getSearchSource().query() instanceof MatchAllQueryBuilder);
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        RexNode condition = queryRegistry.convert(ctx.getSearchSource().query(), ctx);
        return LogicalFilter.create(input, condition);
    }
}
