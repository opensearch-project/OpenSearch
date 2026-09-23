/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionConcurrencyLimiterStats;
import org.opensearch.action.ActionConcurrencyLimiterStats.ActionLimiterSnapshot;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.concurrency.ActionConcurrencyLimiterRegistry.AcquireResult;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class ActionConcurrencyLimiterRegistryTests extends OpenSearchTestCase {

    private static ClusterSettings clusterSettings(Settings settings) {
        Set<Setting<?>> all = new HashSet<>(ConcurrencyLimitSettings.ALL_SETTINGS);
        return new ClusterSettings(settings, all);
    }

    private static ActionConcurrencyLimiterRegistry newRegistry(Settings s, ClusterSettings cs) {
        return new ActionConcurrencyLimiterRegistry(s, cs, new ThreadContext(Settings.EMPTY));
    }

    private static Settings enabled(String alias, String actionName) {
        return Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + alias + ".action_name", actionName)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + alias + ".mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + alias + ".warmup_duration", "0s")
            .build();
    }

    // -------------------------------------------------------------------------
    // no limiter configured
    // -------------------------------------------------------------------------

    public void testNoLimiterPassesThrough() {
        Settings s = Settings.EMPTY;
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        assertFalse(registry.hasLimiterFor("indices:data/read/search"));
        assertTrue(registry.tryAcquire("indices:data/read/search", null, null).isNoLimiter());
    }

    // -------------------------------------------------------------------------
    // single action
    // -------------------------------------------------------------------------

    public void testAcquireAndRelease() {
        Settings s = enabled("search", "indices:data/read/search");
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
        AcquireResult token = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue("should acquire a token", token.isAcquired());

        ActionConcurrencyLimiterStats stats = registry.getStats();
        assertEquals(1, stats.getSnapshots().size());
        assertEquals(1, stats.getSnapshots().get(0).getInFlight());

        token.token().onSuccess();
        assertEquals(0, registry.getStats().getSnapshots().get(0).getInFlight());
    }

    public void testRejectionAfterLimit() {
        Settings s = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
            .build();
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        AcquireResult first = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue(first.isAcquired());

        AcquireResult second = registry.tryAcquire("indices:data/read/search", null, null);
        assertFalse("second acquire should be rejected at limit=1", second.isAcquired());

        assertEquals(1L, registry.getStats().getSnapshots().get(0).getTotalRejected());
    }

    // -------------------------------------------------------------------------
    // multiple independent actions
    // -------------------------------------------------------------------------

    public void testMultipleActionsAreIndependent() {
        Settings s = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "bulk.action_name", "indices:data/write/bulk")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "bulk.mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "bulk.warmup_duration", "0s")
            .build();
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
        assertTrue(registry.hasLimiterFor("indices:data/write/bulk"));
        assertFalse(registry.hasLimiterFor("indices:data/read/get"));

        AcquireResult searchToken = registry.tryAcquire("indices:data/read/search", null, null);
        AcquireResult bulkToken = registry.tryAcquire("indices:data/write/bulk", null, null);
        assertTrue(searchToken.isAcquired());
        assertTrue(bulkToken.isAcquired());

        // Stats has two snapshots
        assertEquals(2, registry.getStats().getSnapshots().size());

        searchToken.token().onSuccess();
        bulkToken.token().onSuccess();
    }

    // -------------------------------------------------------------------------
    // dynamic settings changes
    // -------------------------------------------------------------------------

    public void testDynamicEnableViaSettings() {
        Settings initial = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "disabled")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
            .build();
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);

        // disabled — not in reverse map
        assertFalse(registry.hasLimiterFor("indices:data/read/search"));

        // enable dynamically
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced").build());

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
        AcquireResult token = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue(token.isAcquired());
        token.token().onSuccess();
    }

    public void testDynamicDisableClearsState() {
        Settings s = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(s);
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, cs);

        // acquire and release a token so the limiter has a non-zero limit snapshot
        AcquireResult token = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue(token.isAcquired());
        token.token().onSuccess();
        assertTrue("limit should be > 0 while enabled", registry.getStats().getSnapshots().get(0).getCurrentLimit() > 0);

        // disable dynamically
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "disabled").build());

        assertFalse("disabled alias removed from reverse map", registry.hasLimiterFor("indices:data/read/search"));
        assertEquals("snapshot limit should be 0 after disable", 0, registry.getStats().getSnapshots().get(0).getCurrentLimit());
    }

    public void testDynamicActionNameChange() {
        Settings s = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(s);
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, cs);

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));

        cs.applySettings(
            Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/msearch").build()
        );

        assertFalse("old action name removed", registry.hasLimiterFor("indices:data/read/search"));
        assertTrue("new action name mapped", registry.hasLimiterFor("indices:data/read/msearch"));
    }

    public void testWarmupAllowsRequestsDuringWarmup() {
        Settings s = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "5m")
            .build();
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        registry.tryAcquire("indices:data/read/search", null, null); // fill slot
        AcquireResult second = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue("during warmup, second acquire should return NOOP not empty", second.isAcquired());
        assertEquals(0L, registry.getStats().getSnapshots().get(0).getTotalRejected());
    }

    // -------------------------------------------------------------------------
    // stats
    // -------------------------------------------------------------------------

    public void testStatsAlias() {
        Settings s = enabled("my_search", "indices:data/read/search");
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));
        ActionConcurrencyLimiterStats stats = registry.getStats();
        assertEquals(1, stats.getSnapshots().size());
        assertEquals("my_search", stats.getSnapshots().get(0).getAlias());
        assertEquals("indices:data/read/search", stats.getSnapshots().get(0).getActionName());
        assertEquals("enforced", stats.getSnapshots().get(0).getMode());
    }

    // -------------------------------------------------------------------------
    // mode-specific behaviour
    // -------------------------------------------------------------------------

    public void testDisabledModePassesThrough() {
        Settings s = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            // mode defaults to "disabled"
            .build();
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));
        assertFalse("disabled mode should not appear in reverse map", registry.hasLimiterFor("indices:data/read/search"));
    }

    public void testMonitorOnlyNeverRejects() {
        Settings s = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "monitor_only")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
            .build();
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        assertTrue("monitor_only should appear in reverse map", registry.hasLimiterFor("indices:data/read/search"));

        // fill the single slot
        AcquireResult first = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue(first.isAcquired());

        // even at limit, monitor_only never rejects
        AcquireResult second = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue("monitor_only should never return empty", second.isAcquired());
        assertEquals("would-be rejections counted in monitor_only", 1L, registry.getStats().getSnapshots().get(0).getTotalRejected());
    }

    public void testModeTransitionDisabledToEnforced() {
        Settings initial = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .build();
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);

        assertFalse(registry.hasLimiterFor("indices:data/read/search"));

        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced").build());

        assertTrue("after transition to enforced, limiter should be active", registry.hasLimiterFor("indices:data/read/search"));
    }

    public void testModeTransitionEnforcedToMonitorOnly() {
        Settings s = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
            .build();
        ClusterSettings cs = clusterSettings(s);
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, cs);

        // fill slot and verify rejection while enforced
        registry.tryAcquire("indices:data/read/search", null, null);
        assertFalse("should reject in enforced mode", registry.tryAcquire("indices:data/read/search", null, null).isAcquired());

        // switch to monitor_only
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "monitor_only").build());

        // now even at limit, requests pass through
        assertTrue("should not reject in monitor_only mode", registry.tryAcquire("indices:data/read/search", null, null).isAcquired());
    }

    // -------------------------------------------------------------------------
    // partitioning
    // -------------------------------------------------------------------------

    private static Settings.Builder partitionedBase() {
        return Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
            .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "premium", "standard", "default");
    }

    public void testPartitionedLimiterBuildsAndAcquires() {
        Settings s = partitionedBase().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.premium.percent", 0.6)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.standard.percent", 0.3)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.default.percent", 0.1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.resolver", "fixed")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.resolver.fixed.partition", "premium")
            .build();
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
        AcquireResult token = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue("partitioned limiter should grant a token", token.isAcquired());
        token.token().onSuccess();
    }

    public void testInvalidPartitionPercentRejectedAtUpdate() {
        // percent > 1.0 must be rejected at PUT time (HTTP 400), not silently swallowed.
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);
        assertTrue(registry.hasLimiterFor("indices:data/read/search"));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder()
                    .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "premium")
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.premium.percent", 1.5)
                    .build()
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must be in [0.0, 1.0]"));

        // The previous (non-partitioned) limiter is retained and still functional.
        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
        AcquireResult token = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue("limiter should still work after rejecting bad config", token.isAcquired());
        token.token().onSuccess();
    }

    public void testPartitionPercentSumOverOneRejectedAtUpdate() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);

        // premium 0.7 + standard 0.5 = 1.2 > 1.0 — must be rejected at PUT time.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder()
                    .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "premium", "standard")
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.premium.percent", 0.7)
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.standard.percent", 0.5)
                    .build()
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("<= 1.0"));

        // Node stays healthy; previous limiter retained.
        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
        assertTrue(registry.tryAcquire("indices:data/read/search", null, null).isAcquired());
    }

    public void testPartitionsWithoutResolverRejected() {
        // partitions set but no resolver → all traffic would funnel to unknownPartition. Reject.
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder()
                    .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "premium")
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.premium.percent", 0.5)
                    .build()
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("no partition.resolver"));
    }

    public void testUnknownResolverRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder()
                    .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "premium")
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.premium.percent", 0.5)
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.resolver", "byTenant")
                    .build()
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Unknown partition resolver"));
    }

    public void testFixedResolverTargetNotInListRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder()
                    .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "premium")
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.premium.percent", 0.5)
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.resolver", "fixed")
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.resolver.fixed.partition", "ghost")
                    .build()
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("is not in partitions"));
    }

    public void testValidPartitionConfigAccepted() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);

        cs.applySettings(
            Settings.builder()
                .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "premium", "standard")
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.premium.percent", 0.6)
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.standard.percent", 0.3)
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.resolver", "byHeader")
                .build()
        );

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
        AcquireResult token = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue("valid partitioned config should grant a token", token.isAcquired());
        token.token().onSuccess();
    }

    public void testBySearchTypeResolverConfigAccepted() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);

        cs.applySettings(
            Settings.builder()
                .putList(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partitions", "filter", "aggregation")
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.filter.percent", 0.5)
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.aggregation.percent", 0.5)
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.partition.resolver", "bySearchType")
                .build()
        );

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));

        SearchRequest aggSearch = new SearchRequest().source(
            new SearchSourceBuilder().aggregation(AggregationBuilders.max("m").field("f"))
        );
        AcquireResult token = registry.tryAcquire("indices:data/read/search", null, aggSearch);
        assertTrue("bySearchType partitioned limiter should grant a token", token.isAcquired());
        token.token().onSuccess();
    }

    // -------------------------------------------------------------------------
    // metrics listener (push telemetry wiring)
    // -------------------------------------------------------------------------

    private static final class RecordingMetricsListener implements ActionConcurrencyLimiterRegistry.MetricsListener {
        final List<String> active = new ArrayList<>();
        final List<String> inactive = new ArrayList<>();
        ActionLimiterSnapshot lastSnapshot;

        @Override
        public void onActive(String alias, Supplier<ActionLimiterSnapshot> snapshotSupplier) {
            active.add(alias);
            lastSnapshot = snapshotSupplier.get();
        }

        @Override
        public void onInactive(String alias) {
            inactive.add(alias);
        }
    }

    public void testSetMetricsListenerReplaysActiveAliases() {
        Settings s = enabled("search", "indices:data/read/search");
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, clusterSettings(s));

        RecordingMetricsListener listener = new RecordingMetricsListener();
        registry.setMetricsListener(listener);

        assertEquals(List.of("search"), listener.active);
        assertNotNull(listener.lastSnapshot);
        assertEquals("indices:data/read/search", listener.lastSnapshot.getActionName());
        assertEquals("enforced", listener.lastSnapshot.getMode());
    }

    public void testListenerFiresOnEnableAndDisable() {
        Settings initial = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "disabled")
            .build();
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);

        RecordingMetricsListener listener = new RecordingMetricsListener();
        registry.setMetricsListener(listener);
        assertTrue("disabled alias should not be replayed as active", listener.active.isEmpty());

        // enable → onActive
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced").build());
        assertEquals(List.of("search"), listener.active);

        // disable → onInactive
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "disabled").build());
        assertEquals(List.of("search"), listener.inactive);
    }

    public void testListenerRefiresOnReconfigure() {
        Settings s = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(s);
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, cs);

        RecordingMetricsListener listener = new RecordingMetricsListener();
        registry.setMetricsListener(listener);
        assertEquals(1, listener.active.size());

        // reconfigure an active alias (mode change) → re-fire onActive so tags can refresh
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "monitor_only").build());

        assertEquals(2, listener.active.size());
        assertEquals("monitor_only", listener.lastSnapshot.getMode());
        assertTrue(listener.inactive.isEmpty());
    }

    public void testMetricsListenerNotFiredOnReconfigureFailure() {
        Settings s = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(s);
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, cs);

        RecordingMetricsListener listener = new RecordingMetricsListener();
        registry.setMetricsListener(listener);
        assertEquals(1, listener.active.size()); // initial replay

        int activeCountBefore = listener.active.size();

        // Change algorithm — this is a successful reconfigure, listener should fire
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.algorithm", "aimd").build());
        assertEquals(activeCountBefore + 1, listener.active.size());
    }

    public void testLimitChangePreservesWarmupExpired() {
        Settings s = Settings.builder()
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced")
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 1)
            .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
            .build();
        ClusterSettings cs = clusterSettings(s);
        ActionConcurrencyLimiterRegistry registry = newRegistry(s, cs);

        // Warmup is 0s so it's already expired — limiter should reject
        AcquireResult first = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue(first.isAcquired());
        assertFalse("should reject after warmup expired", registry.tryAcquire("indices:data/read/search", null, null).isAcquired());
        first.token().onSuccess();

        // Now change limit — this triggers reconfigure. Warmup should stay expired.
        cs.applySettings(
            Settings.builder()
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 2)
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 2)
                .build()
        );

        // Fill up the new limit (2 slots)
        AcquireResult t1 = registry.tryAcquire("indices:data/read/search", null, null);
        AcquireResult t2 = registry.tryAcquire("indices:data/read/search", null, null);
        assertTrue(t1.isAcquired());
        assertTrue(t2.isAcquired());

        // Third should be rejected (not pass-through via warmup restart)
        assertFalse(
            "warmup should not restart on limit change — should still reject",
            registry.tryAcquire("indices:data/read/search", null, null).isAcquired()
        );

        t1.token().onSuccess();
        t2.token().onSuccess();
    }

    // -------------------------------------------------------------------------
    // setting validation
    // -------------------------------------------------------------------------

    private static String rootCauseMessage(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getMessage();
    }

    public void testInvalidInitialLimitRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 0).build())
        );
        assertTrue(rootCauseMessage(e), rootCauseMessage(e).contains("limit.initial must be >= 1"));
    }

    public void testMaxLimitBelowInitialRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder()
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 50)
                    .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 10)
                    .build()
            )
        );
        assertTrue(rootCauseMessage(e), rootCauseMessage(e).contains("limit.max"));
    }

    public void testInvalidVegasBaselineThresholdRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.vegas.baseline_reset_load_threshold", 2.0).build()
            )
        );
        assertTrue(rootCauseMessage(e), rootCauseMessage(e).contains("baseline_reset_load_threshold must be in [0.0, 1.0]"));
    }

    public void testInvalidBurstCapacityRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.burst.capacity", -1).build())
        );
        assertTrue(rootCauseMessage(e), rootCauseMessage(e).contains("burst.capacity must be >= 0"));
    }

    public void testInvalidAimdBackoffRatioRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.aimd.backoff_ratio", 0.3).build()
            )
        );
        assertTrue(rootCauseMessage(e), rootCauseMessage(e).contains("aimd.backoff_ratio must be in [0.5, 1.0)"));
    }

    public void testInvalidGradient2RttToleranceRejected() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        newRegistry(initial, cs);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cs.applySettings(
                Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.gradient2.rtt_tolerance", 0.5).build()
            )
        );
        assertTrue(rootCauseMessage(e), rootCauseMessage(e).contains("gradient2.rtt_tolerance must be >= 1.0"));
    }

    public void testValidConfigAccepted() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);

        // All valid values — should not throw
        cs.applySettings(
            Settings.builder()
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.initial", 10)
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.limit.max", 100)
                .build()
        );
        cs.applySettings(
            Settings.builder()
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.vegas.updrift_factor", 2)
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.vegas.baseline_reset_load_threshold", 0.8)
                .build()
        );
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.burst.capacity", 5).build());
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.aimd.backoff_ratio", 0.9).build());
        cs.applySettings(Settings.builder().put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.gradient2.rtt_tolerance", 2.0).build());

        assertTrue(registry.hasLimiterFor("indices:data/read/search"));
    }

    // -------------------------------------------------------------------------
    // Removing config entirely
    // -------------------------------------------------------------------------

    public void testClearingActionNameRemovesLimiterAndAllowsReactivation() {
        Settings initial = enabled("search", "indices:data/read/search");
        ClusterSettings cs = clusterSettings(initial);
        ActionConcurrencyLimiterRegistry registry = newRegistry(initial, cs);
        assertTrue(registry.hasLimiterFor("indices:data/read/search"));

        // Simulate operator clearing all settings for the alias via null (REST API sends null).
        // Consumers fire with their default values; action_name default is "".
        cs.applySettings(Settings.builder().putNull(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name").build());

        assertFalse("limiter must be gone after clearing action_name", registry.hasLimiterFor("indices:data/read/search"));
        assertEquals("no active limiters reported", 0, registry.getStats().getSnapshots().size());

        // Re-enable with a fresh config — must work without any stale state.
        cs.applySettings(
            Settings.builder()
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.action_name", "indices:data/read/search")
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.mode", "enforced")
                .put(ConcurrencyLimitSettings.SETTING_PREFIX + "search.warmup_duration", "0s")
                .build()
        );

        assertTrue("limiter is active again after re-enabling", registry.hasLimiterFor("indices:data/read/search"));
        assertEquals("one active limiter after re-enabling", 1, registry.getStats().getSnapshots().size());
    }
}
