/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Locale;

/**
 * {@link DslGateInputs#deriveTargetPartitionsMirror(String, int)}, pinned against the formula it copies.
 *
 * <p><b>This is a copy, and nothing here verifies the original.</b> The mirror reproduces
 * {@code DatafusionSettings.deriveTargetPartitions} in the sibling {@code analytics-backend-datafusion}
 * plugin, whose classes this plugin deliberately does not depend on. The expectations below are therefore
 * the formula written out by hand, not read from the owner — so these tests catch an edit to <i>our</i>
 * copy, and cannot catch the owner changing <i>theirs</i>. If the backend's derivation changes, this file
 * stays green while the fan-out's {@code A = vCPU * multiplier / target_partitions} is computed from a
 * wrong divisor; re-check it by hand when touching either side.
 */
public class TargetPartitionsDriftTests extends OpenSearchTestCase {

    private static final String MODE_KEY = "search.concurrent_segment_search.mode";
    private static final String MAX_SLICE_COUNT_KEY = SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY;

    private static final String DRIFT_MESSAGE = "DslGateInputs.deriveTargetPartitionsMirror no longer matches the "
        + "DatafusionSettings.deriveTargetPartitions formula it copies — re-check the fan-out's "
        + "A = vCPU * multiplier / target_partitions";

    /** {@code Runtime.availableProcessors()} — the mirror reads it, so the expectations must too. */
    private static final int VCPU = Runtime.getRuntime().availableProcessors();

    /** Mode {@code none} disables concurrent segment search, so the engine plans a single partition. */
    public void testModeNoneIsAlwaysOnePartition() {
        for (int sliceCount : new int[] { 0, 1, 2, 8, 1024 }) {
            assertMirror(SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE, sliceCount, 1);
        }
    }

    /**
     * An unset slice count ({@code 0}) means "let the server decide", which is half the cores.
     *
     * <p>On a single-vCPU host {@code VCPU / 2} is 0 and the mirror returns 0 too, so the assertion would
     * hold without proving anything. The second half pins the halving itself against a value that cannot
     * collapse that way, and the accessor's {@code >= 1} clamp is covered by
     * {@link #testAccessorReadsTheModeAndSliceCountSettings}.
     */
    public void testZeroSliceCountIsHalfTheCores() {
        for (String mode : concurrentModes()) {
            assertMirror(mode, 0, VCPU / 2);
        }
        assumeTrue("halving is only observable above one core", VCPU > 1);
        assertTrue("half of " + VCPU + " cores must be below the core count", VCPU / 2 < VCPU);
        assertTrue("half of " + VCPU + " cores must be at least 1", VCPU / 2 >= 1);
    }

    /**
     * A set slice count is capped by the core count. Both sides of the {@code min} are covered: 1 and 2 are
     * below it on any host CI runs on, and 1024 is above it — so a dropped {@code min} fails here.
     */
    public void testSetSliceCountIsCappedByTheCoreCount() {
        for (String mode : concurrentModes()) {
            assertMirror(mode, 1, Math.min(1, VCPU));
            assertMirror(mode, 2, Math.min(2, VCPU));
            assertMirror(mode, VCPU, VCPU);
            assertMirror(mode, 1024, VCPU);
        }
        assertEquals("1024 must exceed the core count for the cap to be exercised", VCPU, Math.min(1024, VCPU));
    }

    /** The cell an operator actually runs: the server's own defaults for both settings. */
    public void testDefaultsResolveThroughTheMirror() {
        String defaultMode = SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_MODE.get(Settings.EMPTY);
        int defaultSliceCount = SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.get(Settings.EMPTY);

        assertEquals(
            DRIFT_MESSAGE + " [defaults: mode=" + defaultMode + " sliceCount=" + defaultSliceCount + "]",
            expected(defaultMode, defaultSliceCount),
            DslGateInputs.deriveTargetPartitionsMirror(defaultMode, defaultSliceCount)
        );
    }

    /**
     * The tests above pin the derivation <i>function</i>; this pins the two settings the accessor is
     * <i>fed from</i>. A read of the wrong key would resolve to that setting's default and diverge from
     * the value computed here from the node settings directly.
     */
    public void testAccessorReadsTheModeAndSliceCountSettings() {
        for (String mode : List.of(
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO,
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL,
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE
        )) {
            for (int sliceCount : new int[] { 0, 1, 2, 8, 1024 }) {
                Settings nodeSettings = Settings.builder().put(MODE_KEY, mode).put(MAX_SLICE_COUNT_KEY, sliceCount).build();
                // Both concurrent-segment-search settings are in BUILT_IN_CLUSTER_SETTINGS, so the accessor's
                // typed reads resolve against a plain built-in registry.
                DslGateInputs inputs = new DslGateInputs(new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

                assertEquals(
                    String.format(Locale.ROOT, "%s [accessor, mode=%s sliceCount=%d]", DRIFT_MESSAGE, mode, sliceCount),
                    Math.max(1, expected(mode, sliceCount)),
                    inputs.targetPartitions()
                );
            }
        }
    }

    private static List<String> concurrentModes() {
        return List.of(SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO, SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL);
    }

    private void assertMirror(String mode, int sliceCount, int expected) {
        assertEquals(
            String.format(Locale.ROOT, "%s [mode=%s sliceCount=%d]", DRIFT_MESSAGE, mode, sliceCount),
            expected,
            DslGateInputs.deriveTargetPartitionsMirror(mode, sliceCount)
        );
    }

    /**
     * The copied formula, written out independently of the mirror so the two can disagree. Kept in one
     * place because {@link #testAccessorReadsTheModeAndSliceCountSettings} needs it across a whole grid.
     */
    private static int expected(String mode, int sliceCount) {
        if (SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE.equals(mode)) {
            return 1;
        }
        return sliceCount == 0 ? VCPU / 2 : Math.min(sliceCount, VCPU);
    }
}
