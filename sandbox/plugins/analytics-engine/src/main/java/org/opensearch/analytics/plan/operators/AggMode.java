/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.operators;

/**
 * Aggregation mode for {@link OpenSearchAggregate}.
 *
 * <ul>
 *   <li>{@link #UNRESOLVED} – initial state after wrapping; the AggSplitRule targets this mode.</li>
 *   <li>{@link #PARTIAL} – runs close to the data; produces intermediate results.</li>
 *   <li>{@link #FINAL} – merges partial results into the final output.</li>
 * </ul>
 */
public enum AggMode {
    UNRESOLVED,
    PARTIAL,
    FINAL
}
