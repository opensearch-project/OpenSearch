/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

/**
 * Aggregate execution mode for distributed query execution.
 *
 * @opensearch.internal
 */
public enum AggregateMode {
    /** Partial aggregation at data nodes (emits intermediate state). */
    PARTIAL,
    /** Final aggregation at coordinator (merges partial states). */
    FINAL,
    /** Single-shard — no split needed, full aggregation in one pass. */
    SINGLE
}
