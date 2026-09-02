/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DslQuerySettingsTests extends OpenSearchTestCase {

    private static final String MAX_PARALLEL_KEY = "dsl.query.max_parallel_sub_plans";

    /**
     * The setting's upper bound. Mirrors {@code SubPlanParallelism.MAX_K_SETTING}, which is
     * package-private in another package; the two are pinned together by
     * {@code SubPlanParallelismTests#testTheSettingsBoundMatchesTheHardCeiling}.
     */
    private static final int CEILING = 5;

    // ── The setting descriptors ────────────────────────────────────────────

    public void testMaxParallelSubPlansDefaultIsOne() {
        assertEquals(MAX_PARALLEL_KEY, DslQuerySettings.MAX_PARALLEL_SUB_PLANS.getKey());
        assertEquals(Integer.valueOf(1), DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.EMPTY));
    }

    public void testMaxParallelSubPlansRejectsZero() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.builder().put(MAX_PARALLEL_KEY, 0).build())
        );
        assertTrue("expected a lower-bound message, got: " + e.getMessage(), e.getMessage().contains("must be >= 1"));
    }

    /**
     * The cap has to be enforced by the {@code Setting} itself, not by a downstream {@code min} — this
     * is the assertion that fails if someone "relaxes" it.
     */
    public void testMaxParallelSubPlansRejectsAboveTheCeiling() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.builder().put(MAX_PARALLEL_KEY, CEILING + 1).build())
        );
        assertTrue("expected an upper-bound message, got: " + e.getMessage(), e.getMessage().contains("must be <= " + CEILING));
    }

    /** Every width in range parses, so the bound is a ceiling and not an accidental exact-value check. */
    public void testMaxParallelSubPlansAcceptsEveryWidthUpToTheCeiling() {
        for (int k = 1; k <= CEILING; k++) {
            assertEquals(
                "width " + k + " must be accepted",
                Integer.valueOf(k),
                DslQuerySettings.MAX_PARALLEL_SUB_PLANS.get(Settings.builder().put(MAX_PARALLEL_KEY, k).build())
            );
        }
    }

    /**
     * Guards against a setting being added without {@code getSettings()} being updated: an unregistered key
     * is rejected in {@code opensearch.yml} and is a 400 through {@code _cluster/settings}, so a descriptor
     * that never reaches this list is not merely inert but unusable.
     */
    public void testAllContainsExactlyTheWidthSetting() {
        List<Setting<?>> all = DslQuerySettings.all();
        assertEquals("all() must contain exactly the width setting, got " + keysOf(all), 1, all.size());
        assertEquals(Set.of(MAX_PARALLEL_KEY), Set.copyOf(keysOf(all)));
    }

    /**
     * The width is the only knob. There is one execution shape and no launch-mode setting, so a stray
     * registration would turn an internal execution strategy into a public, permanently supported API —
     * this asserts on the registry the plugin hands the node, which is the layer that decides whether a
     * key works or is a 400.
     */
    public void testNoLaunchShapeSettingIsRegistered() {
        assertFalse(
            "no registered setting may own an execution shape, got " + keysOf(DslQuerySettings.all()),
            keysOf(DslQuerySettings.all()).stream().anyMatch(k -> k.contains("launch"))
        );
    }

    /** A lost {@code Dynamic} silently turns the width knob into a restart-only one. */
    public void testSettingsAreNodeScopeAndDynamic() {
        for (Setting<?> setting : DslQuerySettings.all()) {
            assertTrue(setting.getKey() + " must be NodeScope", setting.hasNodeScope());
            assertTrue(setting.getKey() + " must be Dynamic", setting.isDynamic());
            assertFalse(setting.getKey() + " must not be IndexScope", setting.hasIndexScope());
            assertFalse(setting.getKey() + " must not be Final", setting.getProperties().contains(Setting.Property.Final));
        }
    }

    // ── The live holder ────────────────────────────────────────────────────

    /**
     * The node-settings value differs from the default ({@code 1}) on purpose: at the default this test
     * would also pass against a holder that ignored the node settings entirely.
     */
    public void testHolderReadsInitialValueFromNodeSettings() {
        Settings nodeSettings = Settings.builder().put(MAX_PARALLEL_KEY, 2).build();
        DslQuerySettings holder = new DslQuerySettings(clusterService(nodeSettings));

        assertEquals(2, holder.maxParallelSubPlans());
    }

    /** Sequential-by-default: an unset key must not widen the fan-out. */
    public void testHolderDefaultsToSequential() {
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY));

        assertEquals(1, holder.maxParallelSubPlans());
    }

    public void testDynamicUpdateWritesVolatileMaxParallelSubPlans() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        assertEquals("default before the update", 1, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());

        assertEquals("update consumer must have written the volatile", 2, holder.maxParallelSubPlans());
    }

    /**
     * A one-directional test would pass against a latch-style bug (a consumer that only ever widens),
     * so assert narrowing back and the fall-back-to-default too. Narrowing is the operator's rollback
     * path for the fan-out now that there is no separate {@code enabled} lever here: {@code 1} is
     * byte-identical to sequential execution.
     */
    public void testDynamicUpdateWritesVolatileMaxParallelSubPlansBothWays() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        assertEquals("the sequential default", 1, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());
        assertEquals("widening must take effect", 2, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 1).build());
        assertEquals("narrowing back to sequential must take effect", 1, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());
        assertEquals("widening again must take effect", 2, holder.maxParallelSubPlans());

        clusterSettings.applySettings(Settings.EMPTY);
        assertEquals("clearing the transient value must fall back to the default", 1, holder.maxParallelSubPlans());
    }

    /** Fail secure: a rejected update must not partially apply. */
    public void testDynamicUpdateRejectsAboveTheCeilingAtClusterSettingsLayer() {
        ClusterSettings clusterSettings = registry();
        DslQuerySettings holder = new DslQuerySettings(clusterService(Settings.EMPTY, clusterSettings));

        clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, 2).build());
        assertEquals(2, holder.maxParallelSubPlans());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(MAX_PARALLEL_KEY, CEILING + 1).build())
        );
        // The updater wraps the Setting's own parse failure; the bound is in the cause.
        assertTrue("expected the rejected key to be named, got: " + e.getMessage(), e.getMessage().contains(MAX_PARALLEL_KEY));
        assertNotNull("the Setting itself must be the thing that rejected 3", e.getCause());
        assertTrue(
            "expected an upper-bound message on the cause, got: " + e.getCause().getMessage(),
            e.getCause().getMessage().contains("must be <= " + CEILING)
        );
        assertEquals("the rejected value must not have been applied", 2, holder.maxParallelSubPlans());
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static List<String> keysOf(List<Setting<?>> settings) {
        return settings.stream().map(Setting::getKey).collect(Collectors.toList());
    }

    private static ClusterSettings registry() {
        return new ClusterSettings(Settings.EMPTY, Set.copyOf(DslQuerySettings.all()));
    }

    private static ClusterService clusterService(Settings nodeSettings) {
        return clusterService(nodeSettings, new ClusterSettings(nodeSettings, Set.copyOf(DslQuerySettings.all())));
    }

    private static ClusterService clusterService(Settings nodeSettings, ClusterSettings clusterSettings) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }
}
