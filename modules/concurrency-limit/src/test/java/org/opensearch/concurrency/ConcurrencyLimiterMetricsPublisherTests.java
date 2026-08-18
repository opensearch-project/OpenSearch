/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionConcurrencyLimiterStats.ActionLimiterSnapshot;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.TaggedMeasurement;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ConcurrencyLimiterMetricsPublisherTests extends OpenSearchTestCase {

    /** Recording MetricsRegistry: captures createGauge registrations and exposes close tracking. */
    private static final class RecordingMetricsRegistry implements MetricsRegistry {
        static final class GaugeReg {
            final String name;
            final String unit;
            final Supplier<Double> supplier;
            final Tags tags;
            boolean closed = false;

            GaugeReg(String name, String unit, Supplier<Double> supplier, Tags tags) {
                this.name = name;
                this.unit = unit;
                this.supplier = supplier;
                this.tags = tags;
            }
        }

        final List<GaugeReg> gauges = new ArrayList<>();

        @Override
        public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
            GaugeReg reg = new GaugeReg(name, unit, valueProvider, tags);
            gauges.add(reg);
            return () -> reg.closed = true;
        }

        @Override
        public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public Counter createCounter(String name, String description, String unit) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public Counter createUpDownCounter(String name, String description, String unit) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public Histogram createHistogram(String name, String description, String unit) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public void close() {}

        GaugeReg byName(String name) {
            return gauges.stream().filter(g -> g.name.equals(name) && !g.closed).findFirst().orElse(null);
        }

        long openCount() {
            return gauges.stream().filter(g -> !g.closed).count();
        }
    }

    private static Supplier<ActionLimiterSnapshot> snap(
        String alias,
        String action,
        String mode,
        String algo,
        int limit,
        int inFlight,
        long rejected,
        long lastRtt,
        long noLoad
    ) {
        return () -> new ActionLimiterSnapshot(alias, action, mode, algo, limit, inFlight, rejected, lastRtt, noLoad);
    }

    public void testRegistersFiveGaugesPerActiveAlias() {
        RecordingMetricsRegistry mr = new RecordingMetricsRegistry();
        ConcurrencyLimiterMetricsPublisher pub = new ConcurrencyLimiterMetricsPublisher(mr);

        pub.onActive("coordinator_search", snap("coordinator_search", "indices:data/read/search", "enforced", "vegas", 150, 0, 0, -1, -1));

        assertEquals(1, pub.registeredAliasCount());
        assertEquals(5, mr.openCount());
        assertNotNull(mr.byName(ConcurrencyLimiterMetricsPublisher.CURRENT_LIMIT));
        assertNotNull(mr.byName(ConcurrencyLimiterMetricsPublisher.IN_FLIGHT));
        assertNotNull(mr.byName(ConcurrencyLimiterMetricsPublisher.TOTAL_REJECTED));
        assertNotNull(mr.byName(ConcurrencyLimiterMetricsPublisher.LAST_RTT));
        assertNotNull(mr.byName(ConcurrencyLimiterMetricsPublisher.RTT_NOLOAD));
    }

    public void testGaugeTagsFromSnapshot() {
        RecordingMetricsRegistry mr = new RecordingMetricsRegistry();
        ConcurrencyLimiterMetricsPublisher pub = new ConcurrencyLimiterMetricsPublisher(mr);

        pub.onActive("a", snap("a", "indices:data/read/search", "monitor_only", "aimd", 10, 0, 0, -1, -1));

        Tags tags = mr.byName(ConcurrencyLimiterMetricsPublisher.CURRENT_LIMIT).tags;
        assertEquals("a", tags.getTagsMap().get("alias"));
        assertEquals("indices:data/read/search", tags.getTagsMap().get("action_name"));
        assertEquals("monitor_only", tags.getTagsMap().get("mode"));
        assertEquals("aimd", tags.getTagsMap().get("algorithm"));
    }

    public void testGaugeValuesTrackLiveSnapshot() {
        RecordingMetricsRegistry mr = new RecordingMetricsRegistry();
        ConcurrencyLimiterMetricsPublisher pub = new ConcurrencyLimiterMetricsPublisher(mr);

        // Mutable backing values to prove the gauge samples live state at collection time.
        AtomicInteger inFlight = new AtomicInteger(3);
        Supplier<ActionLimiterSnapshot> live = () -> new ActionLimiterSnapshot(
            "a",
            "act",
            "enforced",
            "vegas",
            150,
            inFlight.get(),
            7,
            42,
            5
        );
        pub.onActive("a", live);

        Supplier<Double> inFlightGauge = mr.byName(ConcurrencyLimiterMetricsPublisher.IN_FLIGHT).supplier;
        assertEquals(3.0, inFlightGauge.get(), 0.0);
        inFlight.set(9);
        assertEquals("gauge must re-sample live snapshot", 9.0, inFlightGauge.get(), 0.0);

        assertEquals(150.0, mr.byName(ConcurrencyLimiterMetricsPublisher.CURRENT_LIMIT).supplier.get(), 0.0);
        assertEquals(7.0, mr.byName(ConcurrencyLimiterMetricsPublisher.TOTAL_REJECTED).supplier.get(), 0.0);
        assertEquals(42.0, mr.byName(ConcurrencyLimiterMetricsPublisher.LAST_RTT).supplier.get(), 0.0);
        assertEquals(5.0, mr.byName(ConcurrencyLimiterMetricsPublisher.RTT_NOLOAD).supplier.get(), 0.0);
    }

    public void testGaugesClosedOnInactive() {
        RecordingMetricsRegistry mr = new RecordingMetricsRegistry();
        ConcurrencyLimiterMetricsPublisher pub = new ConcurrencyLimiterMetricsPublisher(mr);

        pub.onActive("a", snap("a", "act", "enforced", "vegas", 10, 0, 0, -1, -1));
        assertEquals(5, mr.openCount());

        pub.onInactive("a");
        assertEquals(0, pub.registeredAliasCount());
        assertEquals("all gauges for the alias must be closed", 0, mr.openCount());
        assertEquals(5, mr.gauges.size()); // 5 ever created, all now closed
    }

    public void testReconfigureRefreshesTagsWithoutLeak() {
        RecordingMetricsRegistry mr = new RecordingMetricsRegistry();
        ConcurrencyLimiterMetricsPublisher pub = new ConcurrencyLimiterMetricsPublisher(mr);

        pub.onActive("a", snap("a", "act", "enforced", "vegas", 10, 0, 0, -1, -1));
        // Re-fire (e.g. mode change) — old gauges must be closed, new ones registered.
        pub.onActive("a", snap("a", "act", "monitor_only", "vegas", 10, 0, 0, -1, -1));

        assertEquals(1, pub.registeredAliasCount());
        assertEquals("only the latest generation of 5 gauges should be open", 5, mr.openCount());
        assertEquals("first generation must have been closed", 10, mr.gauges.size());
        assertEquals("monitor_only", mr.byName(ConcurrencyLimiterMetricsPublisher.CURRENT_LIMIT).tags.getTagsMap().get("mode"));
    }

    public void testMultipleAliasesIndependent() {
        RecordingMetricsRegistry mr = new RecordingMetricsRegistry();
        ConcurrencyLimiterMetricsPublisher pub = new ConcurrencyLimiterMetricsPublisher(mr);

        pub.onActive("filterer", snap("filterer", "act1", "enforced", "vegas", 10, 0, 0, -1, -1));
        pub.onActive("agger", snap("agger", "act2", "enforced", "vegas", 20, 0, 0, -1, -1));
        assertEquals(2, pub.registeredAliasCount());
        assertEquals(10, mr.openCount());

        pub.onInactive("filterer");
        assertEquals(1, pub.registeredAliasCount());
        assertEquals(5, mr.openCount());
    }

    public void testNoopRegistryIsInert() {
        // With the node's no-op registry (telemetry disabled), nothing should throw.
        ConcurrencyLimiterMetricsPublisher pub = new ConcurrencyLimiterMetricsPublisher(NoopMetricsRegistry.INSTANCE);
        pub.onActive("a", snap("a", "act", "enforced", "vegas", 10, 0, 0, -1, -1));
        assertEquals(1, pub.registeredAliasCount());
        pub.onInactive("a");
        assertEquals(0, pub.registeredAliasCount());
        pub.close();
    }
}
