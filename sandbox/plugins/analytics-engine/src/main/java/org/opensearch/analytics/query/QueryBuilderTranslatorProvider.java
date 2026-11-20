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
import org.opensearch.index.query.QueryBuilder;

/**
 * Extension point for translating an OpenSearch {@link QueryBuilder} into a Calcite predicate.
 * Implementations are supplied by plugins extending {@code analytics-engine}.
 */
public interface QueryBuilderTranslatorProvider {

    /** Translates {@code query} against the schema represented by {@code table}. */
    RexNode translate(QueryBuilder query, RelOptCluster cluster, RelOptTable table) throws OpenSearchException;
}
