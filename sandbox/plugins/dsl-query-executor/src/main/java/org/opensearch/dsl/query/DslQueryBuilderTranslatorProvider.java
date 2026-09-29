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
            return registry.convert(query, new ConversionContext(new SearchSourceBuilder(), cluster, table));
        } catch (ConversionException e) {
            throw new OpenSearchException("Failed to translate read-access policy", e);
        }
    }
}
