/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.search.SearchService;

import java.util.OptionalDouble;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads the three cluster-settings inputs the sub-plan fan-out decision needs, and nothing else.
 */
public final class DslGateInputs {

    private static final Logger logger = LogManager.getLogger(DslGateInputs.class);

    /**
     * Key of the sibling DataFusion plugin's concurrency-gate multiplier. Held as a string on
     * purpose — see the class javadoc; {@code DatafusionSettings} is not referenceable from here.
     */
    private static final String FRAGMENT_EXECUTOR_MULTIPLIER_KEY = "datafusion.concurrency.fragment_executor_multiplier";

    /** Key of the parent analytics-engine plugin's per-node in-flight shard-request cap. */
    private static final String MAX_CONCURRENT_SHARD_REQUESTS_KEY = "analytics.query.max_concurrent_shard_requests_per_node";

    private final ClusterSettings clusterSettings;

    // One-shot latches so the "input unavailable" cases are reported once per node, not per query.
    // One latch per distinct condition, deliberately: a latch shared between the "key not registered",
    // "registered with a non-numeric value" and "registered but unreadable" branches would let whichever
    private final AtomicBoolean multiplierUnregisteredLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierNonNumericLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierNonFiniteLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierNonPositiveLogged = new AtomicBoolean();
    private final AtomicBoolean multiplierUnreadableLogged = new AtomicBoolean();
    private final AtomicBoolean shardRequestCapUnregisteredLogged = new AtomicBoolean();
    private final AtomicBoolean shardRequestCapNonNumericLogged = new AtomicBoolean();
    private final AtomicBoolean shardRequestCapUnreadableLogged = new AtomicBoolean();

    /**
     * Creates a reader over the live cluster-settings registry.
     *
     * @param clusterSettings the node's single {@link ClusterSettings} instance, holding every
     *                        {@code NodeScope} setting of every installed plugin
     */
    public DslGateInputs(ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    /**
     * Live value of {@code datafusion.concurrency.fragment_executor_multiplier}, or empty when no
     * backend on this node declares it.
     *
     * @return the multiplier, always &gt; 0 when present, or {@link OptionalDouble#empty()} if the key is
     *         unregistered, registered with a non-numeric type, registered with a non-finite or
     *         non-positive value, or registered with a value the owning {@code Setting} refuses to hand
     *         over
     */
    public OptionalDouble fragmentExecutorMultiplier() {
        Setting<?> descriptor = clusterSettings.get(FRAGMENT_EXECUTOR_MULTIPLIER_KEY);
        if (descriptor == null) {
            if (firstDebugReport(multiplierUnregisteredLogged)) {
                logger.debug(
                    "[{}] is not registered on this node (no gated backend installed); the concurrency-gate "
                        + "term is dropped from the sub-plan fan-out width",
                    FRAGMENT_EXECUTOR_MULTIPLIER_KEY
                );
            }
            return OptionalDouble.empty();
        }
        Object value;
        try {
            value = clusterSettings.get(descriptor);
        } catch (RuntimeException e) {
            // Registered but unreadable. The descriptor came out of this same registry, so a scope
            // mismatch cannot happen here — what can is the owning Setting refusing to parse the value it
            // has been given (AbstractScopedSettings.get(Setting) delegates straight to
            // setting.get(lastSettingsApplied, settings), which propagates the Setting's own
            // IllegalArgumentException). Same fail-secure "drop the term" signal as absent: this whole read
            // path exists to survive another plugin changing its setting under us, so it must not turn that
            // into a failed query.
            if (firstDebugReport(multiplierUnreadableLogged)) {
                logger.debug(
                    "[{}] is registered but its live value could not be read [{}]; the concurrency-gate term "
                        + "is dropped from the sub-plan fan-out width",
                    FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                    e
                );
            }
            return OptionalDouble.empty();
        }
        if (value instanceof Number number) {
            double multiplier = number.doubleValue();
            if (Double.isFinite(multiplier) == false) {
                // Numeric-typed but unusable, and reachable through the owning descriptor rather than only
                // through a hypothetical re-declaration: Setting's double parser range-checks with
                // value < min / value > max, and both comparisons are false for NaN, so an operator PUT of
                // "NaN" passes DataFusion's own 0.1-to-10.0 bounds. Handing that on would poison every term
                // derived from it and print as NaN in the fan-out's observability line, so it degrades to the
                // same absent signal as an unreadable value. Note what that costs and does not cost: the
                // gate term is dropped, so the fan-out falls back to being bounded by the sub-plan count,
                // the operator's own cap (max 2, enforced in the Setting) and the search-pool term — it is
                // NOT substituted with 1.0, the one value this contract forbids synthesising.
                if (firstDebugReport(multiplierNonFiniteLogged)) {
                    logger.debug(
                        "[{}] is registered with a non-finite value [{}]; the concurrency-gate term is dropped "
                            + "from the sub-plan fan-out width",
                        FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                        multiplier
                    );
                }
                return OptionalDouble.empty();
            }
            if (multiplier <= 0.0) {
                // Domain guard on a single value, the symmetric counterpart of the Math.max(1L, raw) in
                // maxConcurrentShardRequestsPerNode(): both defend the same hypothesis — a bounds relaxation
                // or a type change (doubleSetting -> intSetting with a 0 default) on the declaring side,
                // which is compile-invisible here precisely because the read is by string. A multiplier of 0
                // or below makes the consumer's gate term vCPU * multiplier / target_partitions zero or
                // negative, i.e. a permanently killed fan-out or a negative width — the direction this
                // class's contract says it must never fail into.
                if (firstDebugReport(multiplierNonPositiveLogged)) {
                    logger.debug(
                        "[{}] is registered with a non-positive value [{}]; the concurrency-gate term is dropped "
                            + "from the sub-plan fan-out width",
                        FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                        multiplier
                    );
                }
                return OptionalDouble.empty();
            }
            return OptionalDouble.of(multiplier);
        }
        // Registered but not numeric: fail secure with the same "drop the term" signal rather than
        // throwing a cast exception onto the query path.
        if (firstDebugReport(multiplierNonNumericLogged)) {
            logger.debug(
                "[{}] is registered with a non-numeric value [{}]; the concurrency-gate term is dropped "
                    + "from the sub-plan fan-out width",
                FRAGMENT_EXECUTOR_MULTIPLIER_KEY,
                value
            );
        }
        return OptionalDouble.empty();
    }

    /**
     * Live re-derivation of the backend's {@code target_partitions}, clamped to at least 1.
     *
     * @return the derived target partition count, always &gt;= 1
     */
    public int targetPartitions() {
        String mode = clusterSettings.get(SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE);
        int maxSliceCount = clusterSettings.get(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING);
        return Math.max(1, deriveTargetPartitionsMirror(mode, maxSliceCount));
    }

    /**
     * Live value of {@code analytics.query.max_concurrent_shard_requests_per_node}, clamped to at
     * least 1, or {@link Integer#MAX_VALUE} when the key cannot be resolved.
     *
     * @return the per-node in-flight shard-request cap, always &gt;= 1
     */
    public int maxConcurrentShardRequestsPerNode() {
        Setting<?> descriptor = clusterSettings.get(MAX_CONCURRENT_SHARD_REQUESTS_KEY);
        if (descriptor == null) {
            if (firstDebugReport(shardRequestCapUnregisteredLogged)) {
                logger.debug(
                    "[{}] is not registered on this node; treating the per-node shard-request cap as unbounded",
                    MAX_CONCURRENT_SHARD_REQUESTS_KEY
                );
            }
            return Integer.MAX_VALUE;
        }
        Object value;
        try {
            value = clusterSettings.get(descriptor);
        } catch (RuntimeException e) {
            // Registered but unreadable — see the same branch in fragmentExecutorMultiplier(). Falls back
            // to the documented MAX_VALUE rather than throwing onto the query path.
            if (firstDebugReport(shardRequestCapUnreadableLogged)) {
                logger.debug(
                    "[{}] is registered but its live value could not be read [{}]; treating the per-node "
                        + "shard-request cap as unbounded",
                    MAX_CONCURRENT_SHARD_REQUESTS_KEY,
                    e
                );
            }
            return Integer.MAX_VALUE;
        }
        if (value instanceof Number number) {
            // Domain guard on a single value, not composition: keeps the consumer's F >= 1 even if
            // the declaring plugin relaxes the setting's own minimum of 1. Saturating rather than
            // Number.intValue(): the read is by string, so a type change on the declaring side
            long raw = number.longValue();
            return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, raw));
        }
        if (firstDebugReport(shardRequestCapNonNumericLogged)) {
            logger.debug(
                "[{}] is registered with a non-numeric value [{}]; treating the per-node shard-request cap as unbounded",
                MAX_CONCURRENT_SHARD_REQUESTS_KEY,
                value
            );
        }
        return Integer.MAX_VALUE;
    }

    /**
     * Byte-faithful mirror of the DataFusion backend's {@code deriveTargetPartitions}: mode
     * {@code "none"} forces 1, a {@code maxSliceCount} of 0 means the backend owns the concurrency
     * level and uses half the available processors, otherwise the slice count is capped at the
     * available processors.
     *
     * @param mode          value of {@code search.concurrent_segment_search.mode}
     * @param maxSliceCount value of {@code search.concurrent.max_slice_count}
     * @return the derived target partition count, which may be 0
     */
    static int deriveTargetPartitionsMirror(String mode, int maxSliceCount) {
        if (SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE.equals(mode)) {
            return 1;
        }

        if (maxSliceCount == 0) {
            return Runtime.getRuntime().availableProcessors() / 2;
        }

        return Math.min(maxSliceCount, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Trips a latch, returning {@code true} only the first time the message can actually be emitted.
     * These accessors run on every fan-out decision, so an unlatched log line would be per-query noise;
     * the log calls stay at their call sites (rather than behind a varargs helper) so the logger-usage
     * checker can see their arity.
     */
    private static boolean firstDebugReport(AtomicBoolean latch) {
        return logger.isDebugEnabled() && latch.compareAndSet(false, true);
    }
}
