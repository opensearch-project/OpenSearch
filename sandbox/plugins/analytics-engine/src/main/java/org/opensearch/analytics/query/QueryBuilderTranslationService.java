/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.query;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.opensearch.OpenSearchException;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/** Provides the single QueryBuilder translator installed for analytics planning. */
public final class QueryBuilderTranslationService {

    private final QueryBuilderTranslatorProvider provider;

    public QueryBuilderTranslationService(List<QueryBuilderTranslatorProvider> providers) {
        if (providers.size() > 1) {
            throw new IllegalStateException("Only one QueryBuilderTranslatorProvider may be installed, found [" + providers.size() + "]");
        }
        this.provider = providers.isEmpty() ? null : providers.getFirst();
    }

    /** Translates a policy query. Match-none is handled without requiring a provider. */
    public RexNode translate(QueryBuilder query, RelOptCluster cluster, RelOptTable table) throws OpenSearchException {
        if (query instanceof MatchNoneQueryBuilder) {
            return cluster.getRexBuilder().makeLiteral(false);
        }
        if (provider == null) {
            throw new OpenSearchException("No QueryBuilderTranslatorProvider is installed");
        }
        return provider.translate(query, cluster, table);
    }
}
