/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.opensearch.OpenSearchException;
import org.opensearch.analytics.query.QueryBuilderTranslatorProvider;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

/** Exposes the DSL query translator registry to the parent analytics-engine plugin. */
public final class DslQueryBuilderTranslatorProvider implements QueryBuilderTranslatorProvider {

    private final QueryRegistry registry = QueryRegistryFactory.create();

    public DslQueryBuilderTranslatorProvider() {}

    @Override
    public RexNode translate(QueryBuilder query, RelOptCluster cluster, RelOptTable table) throws OpenSearchException {
        try {
            RexNode translated = registry.convert(query, new ConversionContext(new SearchSourceBuilder(), cluster, table));
            ensureResolved(translated);
            return translated;
        } catch (ConversionException e) {
            throw new OpenSearchException("Failed to translate read-access policy", e);
        }
    }

    private static void ensureResolved(RexNode node) {
        // QueryRegistry deliberately preserves unsupported QueryBuilder types as
        // UnresolvedQueryCall nodes so another execution backend may handle them. A
        // read-access policy cannot use that deferred-resolution behavior: this provider's
        // output is serialized into the secured backend plan, where leaving any part of the
        // policy unresolved could cause it to be omitted or fail only after execution starts.
        // Reject every unresolved node, including nested operands, so document-level security
        // fails closed during plan construction.
        if (node instanceof UnresolvedQueryCall unresolved) {
            throw new OpenSearchException(
                "No analytics translator is registered for read-access policy query ["
                    + unresolved.getQueryBuilder().getWriteableName()
                    + "]"
            );
        }
        if (node instanceof RexCall call) {
            call.getOperands().forEach(DslQueryBuilderTranslatorProvider::ensureResolved);
        }
    }
}
