/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.SchemaProvider;

/**
 * Bundle of services that {@code AnalyticsPlugin} pushes to each {@link AnalyticsFrontEndExtension}
 * consumer once Guice has constructed them.
 *
 * <p>Bundled rather than passed through individual setters so future analytics-engine services can
 * be added without changing the {@link AnalyticsFrontEndExtension} signature — frontends that do
 * not consume the new service simply ignore the new accessor.
 *
 * @param queryPlanExecutor coordinator-level query plan executor
 * @param schemaProvider    builds a Calcite {@code SchemaPlus} from the current cluster state
 *
 * @opensearch.internal
 */
public record AnalyticsServices(QueryPlanExecutor<RelNode, Iterable<Object[]>> queryPlanExecutor, SchemaProvider schemaProvider) {
}
