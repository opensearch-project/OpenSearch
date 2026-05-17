/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Plugin-side stats providers for the DataFusion native execution engine.
 *
 * <p>Contains {@link org.opensearch.be.datafusion.stats.DataFusionBackendStatsProvider}
 * which implements the SPI {@code BackendStatsProvider} interface. The core stats types
 * ({@code DataFusionStats}, {@code NativeExecutorsStats}) live in the
 * {@code org.opensearch.plugin.stats} package.
 */
package org.opensearch.be.datafusion.stats;
