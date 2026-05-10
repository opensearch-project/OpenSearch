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
 * <p>The context id (the map key in {@link AnalyticsSearchBackendPlugin#getActiveQueryMetrics()})
 * is not repeated here — this record holds only the remaining parameters: memory accounting,
 * wall time, and whether the query has completed but not yet been drained.
 *
 * @param currentBytes bytes currently reserved by the query's memory pool
 * @param peakBytes    high-water mark of bytes reserved during the query's lifetime
 * @param wallNanos    live or frozen wall-clock duration in nanoseconds
 * @param completed    {@code true} when the query has finished executing but its entry has not
 *                     yet been drained from the backend's internal registry
 *
 * @opensearch.internal
 */
public record QueryExecutionMetrics(long currentBytes, long peakBytes, long wallNanos, boolean completed) {}
