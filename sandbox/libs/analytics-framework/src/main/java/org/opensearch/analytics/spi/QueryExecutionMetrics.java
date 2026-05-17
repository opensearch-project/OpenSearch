/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Snapshot of per-query execution metrics reported by an analytics backend.
 *
 * <p>The context id (the map key in {@link AnalyticsSearchBackendPlugin#getTopQueriesByMemory()})
 * is not repeated here — this record holds only the memory accounting fields.
 *
 * @param currentBytes bytes currently reserved by the query's memory pool
 *
 * @opensearch.internal
 */
public record QueryExecutionMetrics(long currentBytes) {}
