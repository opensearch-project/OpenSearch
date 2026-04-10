/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * SPI extension point for back-end query engines for query planning and execution capabilities
 * as needed by the {@link org.opensearch.analytics.exec.QueryPlanExecutor}
 */
public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider {

}
