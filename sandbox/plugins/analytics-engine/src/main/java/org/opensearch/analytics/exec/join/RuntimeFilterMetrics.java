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
 * Process-wide counters for join runtime filters, exposed via {@code GET /_analytics/_strategies}.
 *
 * <p>Every stage of a runtime filter is allowed to decline — a join whose type forbids filtering the
 * probe side, a build side over the row gate, an ambiguous predicate placement, a pre-pass that failed,
 * a capture that could not be unioned. Each of those is silent by design, because a query with no filter
 * is the query as it runs today. That makes the feature <em>invisible</em> without counters: a
 * before/after measurement showing no change is indistinguishable from a filter that never fired, and
 * "the filter fired" is exactly what an A/B has to establish before its numbers mean anything.
 *
 * <p>The counters are therefore a funnel, and the interesting signal is where it narrows:
 * {@code SHUFFLE_PLANNED → SHUFFLE_PLANTED → PRE_PASS_RUN → PRE_PASS_WITH_PAYLOAD → PAYLOAD_ATTACHED}.
 *
 * <p>Cumulative since node start, recorded on the coordinator that handled the query. Row-level
 * elimination counts live on the data node and are not aggregated here.
 *
 * @opensearch.internal
 */
public final class RuntimeFilterMetrics {

    /** What the coordinator did with a runtime filter, at each point one can be abandoned. */
    public enum Counter {
        /** Shuffle joins for which a filter was planned as legal and worth its cost. */
        SHUFFLE_PLANNED,
        /** Of those, ones whose probe predicate could actually be placed in the plan. */
        SHUFFLE_PLANTED,
        /** Pre-pass mini-DAGs dispatched to compute a build-side summary. */
        PRE_PASS_RUN,
        /** Pre-passes that came back with a usable, unionable bitset. */
        PRE_PASS_WITH_PAYLOAD,
        /**
         * Filter payloads delivered to a stage as an instruction.
         *
         * <p>Counted per payload, not per stage, so it is comparable with {@link #PRE_PASS_WITH_PAYLOAD}
         * directly above it — several filters can legitimately target one leaf, and counting stages made the
         * funnel appear to lose payloads that had in fact been delivered.
         */
        PAYLOAD_ATTACHED,
        /**
         * Total size of the merged filter payloads, summed once per filter.
         *
         * <p>Not the bytes on the wire. Each payload is serialised independently to every probe-side task,
         * so a filter attached to a stage with N shard tasks costs about N times what is counted here.
         * Read this as "how large are the filters this node built", and multiply by the probe stage's task
         * count to reason about network cost.
         */
        PAYLOAD_BYTES,
        /** Wall-clock nanoseconds spent in pre-passes, the deduction from any measured win. */
        PRE_PASS_NANOS,
        /** Can-match filters derived from a captured broadcast build (the free family). */
        BROADCAST_CAN_MATCH_ATTACHED
    }

    private final EnumMap<Counter, AtomicLong> counters = new EnumMap<>(Counter.class);

    public RuntimeFilterMetrics() {
        for (Counter counter : Counter.values()) {
            counters.put(counter, new AtomicLong(0));
        }
    }

    public void record(Counter counter, long amount) {
        AtomicLong value = counters.get(counter);
        if (value != null) {
            value.addAndGet(amount);
        }
    }

    public void increment(Counter counter) {
        record(counter, 1L);
    }

    /** Immutable snapshot of every counter. */
    public Map<Counter, Long> snapshot() {
        EnumMap<Counter, Long> out = new EnumMap<>(Counter.class);
        for (Map.Entry<Counter, AtomicLong> entry : counters.entrySet()) {
            out.put(entry.getKey(), entry.getValue().get());
        }
        return out;
    }
}
