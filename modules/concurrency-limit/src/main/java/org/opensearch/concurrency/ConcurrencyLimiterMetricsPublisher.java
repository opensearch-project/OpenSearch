/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionConcurrencyLimiterStats.ActionLimiterSnapshot;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Push-telemetry bridge for the concurrency limiter.
 * <p>
 * Registers one set of observable gauges per active alias with the OpenSearch
 * {@link MetricsRegistry}. Each gauge's value provider reads the alias's live
 * {@link ActionLimiterSnapshot} — the exact same values the pull path
 * ({@code /_nodes/stats} {@code concurrency_limiters}) reports — so both telemetry mechanisms
 * publish an identical metric set with identical values.
 * <p>
 * Gauges are (re)registered on {@link #onActive} and torn down on {@link #onInactive}; re-firing
 * {@code onActive} for an already-registered alias refreshes its tags (e.g. after a mode change).
 * When telemetry is disabled the node supplies a no-op {@code MetricsRegistry}, so this bridge is
 * inert with no special-casing here.
 */
public final class ConcurrencyLimiterMetricsPublisher implements ActionConcurrencyLimiterRegistry.MetricsListener, Closeable {

    private static final Logger LOG = LogManager.getLogger(ConcurrencyLimiterMetricsPublisher.class);

    static final String CURRENT_LIMIT = "concurrency_limit.current_limit";
    static final String IN_FLIGHT = "concurrency_limit.in_flight";
    static final String TOTAL_REJECTED = "concurrency_limit.total_rejected";
    static final String LAST_RTT = "concurrency_limit.last_rtt";
    static final String RTT_NOLOAD = "concurrency_limit.rtt_noload";

    private final MetricsRegistry metricsRegistry;
    private final Map<String, List<Closeable>> gaugesByAlias = new ConcurrentHashMap<>();

    /**
     * Creates a new publisher backed by the given metrics registry.
     *
     * @param metricsRegistry the metrics registry to register gauges with
     */
    public ConcurrencyLimiterMetricsPublisher(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
    }

    @Override
    public synchronized void onActive(String alias, Supplier<ActionLimiterSnapshot> snapshotSupplier) {
        // Refresh: drop any prior registration so tags reflect the current config.
        closeAlias(alias);

        ActionLimiterSnapshot s0 = snapshotSupplier.get();
        Tags tags = Tags.create()
            .addTag("alias", alias)
            .addTag("action_name", s0.getActionName())
            .addTag("mode", s0.getMode())
            .addTag("algorithm", s0.getAlgorithm());

        List<Closeable> gauges = new ArrayList<>(5);
        gauges.add(
            metricsRegistry.createGauge(
                CURRENT_LIMIT,
                "Current adaptive concurrency limit",
                "1",
                () -> (double) snapshotSupplier.get().getCurrentLimit(),
                tags
            )
        );
        gauges.add(
            metricsRegistry.createGauge(
                IN_FLIGHT,
                "Requests currently holding a token",
                "1",
                () -> (double) snapshotSupplier.get().getInFlight(),
                tags
            )
        );
        gauges.add(
            metricsRegistry.createGauge(
                TOTAL_REJECTED,
                "Cumulative rejected requests (includes monitor_only would-be rejections)",
                "1",
                () -> (double) snapshotSupplier.get().getTotalRejected(),
                tags
            )
        );
        gauges.add(
            metricsRegistry.createGauge(
                LAST_RTT,
                "Last observed round-trip time (-1 if unavailable)",
                "ms",
                () -> (double) snapshotSupplier.get().getLastRttMillis(),
                tags
            )
        );
        gauges.add(
            metricsRegistry.createGauge(
                RTT_NOLOAD,
                "Round-trip time under no load (-1 if unavailable)",
                "ms",
                () -> (double) snapshotSupplier.get().getRttNoLoadMillis(),
                tags
            )
        );

        gaugesByAlias.put(alias, gauges);
    }

    @Override
    public synchronized void onInactive(String alias) {
        closeAlias(alias);
    }

    private void closeAlias(String alias) {
        List<Closeable> gauges = gaugesByAlias.remove(alias);
        if (gauges == null) return;
        for (Closeable gauge : gauges) {
            try {
                gauge.close();
            } catch (IOException e) {
                LOG.warn("Failed to close concurrency-limit gauge for alias [{}]: {}", alias, e.getMessage());
            }
        }
    }

    // Visible for testing.
    int registeredAliasCount() {
        return gaugesByAlias.size();
    }

    @Override
    public synchronized void close() {
        for (String alias : new ArrayList<>(gaugesByAlias.keySet())) {
            closeAlias(alias);
        }
    }
}
