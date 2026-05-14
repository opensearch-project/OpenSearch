/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide counters of how many queries were dispatched under each {@link JoinStrategy}.
 *
 * <p>Updated once per query in {@code DefaultPlanExecutor.executeInternal} after the advisor
 * picks a strategy and after the kill-switch has been applied. The counters reflect the
 * <em>routed</em> strategy — i.e. what the dispatcher actually ran, including kill-switch
 * downgrades from BROADCAST to COORDINATOR_CENTRIC and HASH_SHUFFLE fallthroughs to
 * coord-centric until M2 lands.
 *
 * <p>Exposed via {@code GET /_analytics/_strategies}. End-to-end tests use the counter delta
 * around a query to assert that BROADCAST actually fired (vs. silently degrading to M0).
 *
 * @opensearch.internal
 */
public final class JoinStrategyMetrics {

    private final EnumMap<JoinStrategy, AtomicLong> counters = new EnumMap<>(JoinStrategy.class);

    public JoinStrategyMetrics() {
        for (JoinStrategy s : JoinStrategy.values()) {
            counters.put(s, new AtomicLong(0));
        }
    }

    /** Increments the counter for the strategy actually dispatched. */
    public void recordDispatch(JoinStrategy strategy) {
        AtomicLong counter = counters.get(strategy);
        if (counter != null) {
            counter.incrementAndGet();
        }
    }

    /** Returns an immutable snapshot of all counters. */
    public Map<JoinStrategy, Long> snapshot() {
        EnumMap<JoinStrategy, Long> out = new EnumMap<>(JoinStrategy.class);
        for (Map.Entry<JoinStrategy, AtomicLong> entry : counters.entrySet()) {
            out.put(entry.getKey(), entry.getValue().get());
        }
        return out;
    }
}
