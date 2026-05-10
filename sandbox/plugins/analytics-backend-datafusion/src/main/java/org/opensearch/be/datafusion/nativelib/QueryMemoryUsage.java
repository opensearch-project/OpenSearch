/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

/**
 * Per-query memory tracking snapshot, decoded from the native {@code QUERY_REGISTRY}.
 *
 * <p>A single record represents one query's tracker at the moment the native snapshot
 * was taken. Completed queries stay in the registry until
 * {@code query_tracker::drain_completed_query} removes them, so {@link #completed()}
 * distinguishes live from finished queries.
 *
 * @param contextId    the {@code context_id} passed to {@code QueryTrackingContext::new}
 * @param currentBytes bytes currently reserved by this query (saturates at {@code Long.MAX_VALUE})
 * @param peakBytes    peak bytes reserved over the query's lifetime
 * @param wallNanos    live or frozen wall-clock duration in nanoseconds
 * @param completed    {@code true} once the {@code QueryTrackingContext} has been dropped
 */
public record QueryMemoryUsage(long contextId, long currentBytes, long peakBytes, long wallNanos, boolean completed) {}
