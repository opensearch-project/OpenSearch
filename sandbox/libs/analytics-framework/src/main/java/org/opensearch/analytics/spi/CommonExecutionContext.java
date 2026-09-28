/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Marker interface for execution contexts provided by Core to instruction handlers.
 * Concrete implementations carry the information relevant to their execution path:
 * <ul>
 *   <li>{@code ShardScanExecutionContext} — shard fragment execution (reader, task, tableName)</li>
 *   <li>{@code ExchangeSinkContext} — coordinator reduce execution (planBytes, allocator, schema)</li>
 * </ul>
 *
 * @opensearch.internal
 */
public interface CommonExecutionContext {}
