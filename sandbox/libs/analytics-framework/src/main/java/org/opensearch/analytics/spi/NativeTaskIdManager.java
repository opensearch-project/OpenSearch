/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages JVM-unique native task ids, analogous to how TaskManager mints per-node task ids.
 *
 * <p>The native engine keeps one process-wide registry keyed by context id, but
 * {@code Task} ids are only unique per node. Nodes sharing a JVM (internal test
 * clusters) therefore collide when task ids are used directly: one node's
 * cancellation reaches another node's live query. Callers registering a native
 * context MUST key it with {@link #next()} and route cancellation through the
 * same minted id; {@code 0} remains the "tracking disabled" sentinel.
 *
 * @opensearch.internal
 */
public final class NativeTaskIdManager {

    private static final AtomicLong COUNTER = new AtomicLong();

    private NativeTaskIdManager() {}

    /** Returns the next JVM-unique, non-zero context id. */
    public static long next() {
        return COUNTER.incrementAndGet();
    }
}
