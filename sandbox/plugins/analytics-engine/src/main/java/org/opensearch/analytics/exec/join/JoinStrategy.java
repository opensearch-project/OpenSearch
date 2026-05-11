/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

/**
 * Strategy for distributed join execution.
 *
 * <p>The coordinator selects one of these after the CBO plan is produced; strategy selection
 * is not a planning decision (see design note in {@code 65-mpp-joins-design.md §4}).
 *
 * @opensearch.internal
 */
public enum JoinStrategy {

    /** Both sides reduced to coordinator (SINGLETON), join runs there. Safe default. */
    COORDINATOR_CENTRIC,

    /** Small (build) side broadcast to all probe-side data nodes; join runs in parallel on each. */
    BROADCAST,

    /** Both sides hash-partitioned by join key and shuffled to workers. Reserved for M2. */
    HASH_SHUFFLE
}
