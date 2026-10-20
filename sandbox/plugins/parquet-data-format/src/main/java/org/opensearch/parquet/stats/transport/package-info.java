/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Transport and REST actions for the parquet per-format statistics endpoints.
 *
 * <p>Wires {@link org.opensearch.parquet.stats.ParquetStatsProvider} into the
 * {@code /_plugins/parquet/{index}/_stats} and {@code /_plugins/parquet/_nodes/_stats}
 * REST + transport surfaces by extending the abstract bases in
 * {@link org.opensearch.plugin.stats.transport}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
package org.opensearch.parquet.stats.transport;

import org.opensearch.common.annotation.ExperimentalApi;
