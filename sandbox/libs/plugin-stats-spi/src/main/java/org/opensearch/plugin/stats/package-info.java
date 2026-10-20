/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * SPI for plugin-level data-format statistics.
 *
 * <p>Defines the {@link org.opensearch.plugin.stats.DataFormatStatsProvider} contract,
 * the {@link org.opensearch.plugin.stats.DataFormatShardStats} typed shard-stats marker,
 * and the JVM-wide {@link org.opensearch.plugin.stats.DataFormatStatsProviderRegistry}
 * registry. Per-format plugins (Lucene, Parquet, etc.) implement these interfaces to
 * surface their counters via the per-format REST endpoints in the
 * {@code transport} sub-package.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
package org.opensearch.plugin.stats;

import org.opensearch.common.annotation.ExperimentalApi;
