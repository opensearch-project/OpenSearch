/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.settings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.SearchService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TreeMap;

/**
 * Unit coverage for the three gate-input reads. The cross-plugin registrations are faked here with
 * local descriptor copies — a unit test cannot prove the real classloader graph resolves the keys,
 * which is what {@code DslQuerySettingsRestIT} is for. What these tests do pin is the semantics:
 * absent multiplier is empty (never 1.0), the derived partition count is never 0, and the missing
 * shard-request cap falls back to {@code MAX_VALUE} (never a duplicated literal 5).
 */
public class DslGateInputsTests extends OpenSearchTestCase {

    private static final String MULTIPLIER_KEY = "datafusion.concurrency.fragment_executor_multiplier";
    private static final String SHARD_REQUEST_CAP_KEY = "analytics.query.max_concurrent_shard_requests_per_node";
    private static final String MAX_SLICE_COUNT_KEY = SearchService.CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT_KEY;
    private static final String MODE_KEY = "search.concurrent_segment_search.mode";

    /**
     * Same key/type/bounds/properties as {@code DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER} in the
     * sibling {@code analytics-backend-datafusion} plugin, transcribed by hand. Nothing verifies the two
     * still agree: this plugin does not depend on that one, so a rename or a changed default there leaves
     * these tests green while {@code DslGateInputs}' by-key read silently stops resolving and the fan-out's
     * gate term drops out. Re-check by hand when touching either side.
     */
    private static final Setting<Double> MULTIPLIER_DESCRIPTOR_COPY = Setting.doubleSetting(
        MULTIPLIER_KEY,
        1.5,
        0.1,
        10.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Same key/type/bounds/properties as
     * {@code AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE}, transcribed by hand and
     * unverified against the owner for the same reason as the multiplier above — a drift there makes this
     * read fall back to {@code Integer.MAX_VALUE}, i.e. the widest F and the widest fan-out.
     */
    private static final Setting<Integer> SHARD_REQUEST_CAP_DESCRIPTOR_COPY = Setting.intSetting(
        SHARD_REQUEST_CAP_KEY,
        5,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // ── B2.0 — the SC-9 surface ────────────────────────────────────────────

    /**
     * Mechanises the "no arithmetic in this class" acceptance check: a fourth public accessor (or a
     * changed return type) fails here, which is the signal that the shared-contract row needs
     * updating rather than being silently widened.
     */
    public void testGateInputsExposesExactlyTheThreeSc9Accessors() {
        Map<String, String> publicApi = new TreeMap<>();
        // getMethods() (not getDeclaredMethods(), which is a forbidden API here) returns inherited
        // members too, so filter down to what this class itself declares.
        for (Method method : DslGateInputs.class.getMethods()) {
            if (method.getDeclaringClass() == DslGateInputs.class && method.isSynthetic() == false) {
                publicApi.put(method.getName(), method.getReturnType().getName());
            }
        }

        assertEquals(
            "the SC-9 surface is exactly three accessors, got " + publicApi,
            Map.of(
                "fragmentExecutorMultiplier",
                OptionalDouble.class.getName(),
                "targetPartitions",
                "int",
                "maxConcurrentShardRequestsPerNode",
                "int"
            ),
            publicApi
        );
    }

    /** All three values come off one registry — no accessor needs its own wiring. */
    public void testAllThreeAccessorsReadFromTheSameClusterSettings() {
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, MULTIPLIER_DESCRIPTOR_COPY, SHARD_REQUEST_CAP_DESCRIPTOR_COPY));

        assertEquals(OptionalDouble.of(1.5), inputs.fragmentExecutorMultiplier());
        assertEquals(
            expectedTargetPartitions(SearchService.CONCURRENT_SEGMENT_SEARCH_DEFAULT_SLICE_COUNT_VALUE),
            inputs.targetPartitions()
        );
        assertEquals(5, inputs.maxConcurrentShardRequestsPerNode());
    }

    // ── B2.1 — the untyped multiplier read ────────────────────────────────

    public void testMultiplierReadWhenDatafusionSettingRegistered() {
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, MULTIPLIER_DESCRIPTOR_COPY));

        // 1.5 comes from the registered descriptor's own default, not from a literal in main code.
        assertEquals(OptionalDouble.of(1.5), inputs.fragmentExecutorMultiplier());
    }

    /**
     * The 1.5 asserted above <i>is</i> the canonical default, so a hardcoded
     * {@code return OptionalDouble.of(1.5)} would satisfy that test on the un-overridden read path — the
     * path a stock cluster actually takes. Registering a descriptor whose default is deliberately not the
     * canonical one pins where the value comes from: the owning plugin's own {@code Setting}, which is what
     * makes this half of the read path drift-proof rather than a duplicated literal.
     */
    public void testMultiplierComesFromTheRegisteredDescriptorNotALocalDefault() {
        Setting<Double> nonCanonicalDefault = Setting.doubleSetting(
            MULTIPLIER_KEY,
            2.5,
            0.1,
            10.0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, nonCanonicalDefault));

        OptionalDouble multiplier = inputs.fragmentExecutorMultiplier();
        assertEquals(OptionalDouble.of(2.5), multiplier);
        assertNotEquals("the owning plugin's default must not be duplicated here", OptionalDouble.of(1.5), multiplier);
    }

    /** The read must be live, not a startup snapshot — this key is swept while the node runs. */
    public void testMultiplierReadsDynamicOverride() {
        ClusterSettings clusterSettings = registry(Settings.EMPTY, MULTIPLIER_DESCRIPTOR_COPY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(MULTIPLIER_KEY, 3.0).build());

        assertEquals(OptionalDouble.of(3.0), inputs.fragmentExecutorMultiplier());
    }

    public void testMultiplierAbsentWhenSettingNotRegistered() {
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY));

        OptionalDouble multiplier = inputs.fragmentExecutorMultiplier();
        assertTrue("an unregistered key must read as empty, got " + multiplier, multiplier.isEmpty());
        assertNotEquals("an absent multiplier must never be synthesised as 1.0", OptionalDouble.of(1.0), multiplier);
    }

    public void testMultiplierEmptyWhenRegisteredTypeIsNotNumeric() {
        Setting<String> wrongType = Setting.simpleString(
            MULTIPLIER_KEY,
            "not-a-number",
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, wrongType));

        assertTrue(inputs.fragmentExecutorMultiplier().isEmpty());
    }

    /**
     * {@code NaN} is numeric-typed but unusable, and it is <b>reachable through the real descriptor</b>:
     * {@code Setting}'s double parser range-checks with {@code value < min} / {@code value > max}, and both
     * comparisons are false for {@code NaN}, so an operator PUT of {@code "NaN"} passes the owning plugin's
     * own 0.1-to-10.0 bounds. Handed on as {@code of(NaN)} it would poison every term derived from it and
     * print as {@code NaN} in the fan-out's observability line. It degrades to the same absent signal as an
     * unreadable value instead — <b>not</b> to 1.0, which is the one substitution this contract forbids.
     */
    public void testMultiplierEmptyWhenRegisteredValueIsNotFinite() {
        ClusterSettings clusterSettings = registry(Settings.EMPTY, MULTIPLIER_DESCRIPTOR_COPY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(MULTIPLIER_KEY, Double.NaN).build());

        // Pins that this is a real hazard and not a hypothetical one: the owning descriptor accepts it.
        assertTrue(
            "the owning Setting's own bounds are expected to accept NaN",
            Double.isNaN(clusterSettings.get(MULTIPLIER_DESCRIPTOR_COPY))
        );

        OptionalDouble multiplier = inputs.fragmentExecutorMultiplier();
        assertTrue("a non-finite multiplier must degrade to the absent signal, got " + multiplier, multiplier.isEmpty());
        assertNotEquals("and must never be substituted with 1.0", OptionalDouble.of(1.0), multiplier);
    }

    /**
     * A zero or negative multiplier is numeric, finite and unusable: the consuming gate term is
     * {@code vCPU * multiplier / target_partitions}, so {@code 0} kills the fan-out permanently and a
     * negative value makes the width negative — the two directions this read path must never fail into.
     * The relaxed descriptor is the same hypothesis the sibling accessor's {@code Math.max(1L, raw)}
     */
    public void testMultiplierEmptyWhenRegisteredValueIsNotPositive() {
        Setting<Double> negativeAllowingCopy = negativeAllowingMultiplierCopy();
        ClusterSettings clusterSettings = registry(Settings.EMPTY, negativeAllowingCopy);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        for (double unusable : new double[] { 0.0, -0.0, -1.5, -10.0 }) {
            clusterSettings.applySettings(Settings.builder().put(MULTIPLIER_KEY, unusable).build());

            // Pins that the guard in the accessor, not the descriptor's own bounds, is what rejects this.
            assertEquals(
                "the relaxed descriptor is expected to accept " + unusable,
                unusable,
                clusterSettings.get(negativeAllowingCopy),
                0.0
            );

            OptionalDouble multiplier = inputs.fragmentExecutorMultiplier();
            assertTrue("a non-positive multiplier must degrade to the absent signal, got " + multiplier, multiplier.isEmpty());
            assertNotEquals("and must never be substituted with 1.0", OptionalDouble.of(1.0), multiplier);
        }

        // The smallest value the real descriptor allows still reads through: the guard is on the sign.
        clusterSettings.applySettings(Settings.builder().put(MULTIPLIER_KEY, 0.1).build());
        assertEquals(OptionalDouble.of(0.1), inputs.fragmentExecutorMultiplier());
    }

    /**
     * The same guard reached through the other compile-invisible drift shape: the declaring side changing
     * the descriptor's type ({@code doubleSetting} to {@code intSetting}), which hands back a {@code Number}
     * that is not a {@code Double} and whose own default is out of the gate's domain.
     */
    public void testMultiplierEmptyWhenDeclaringTypeChangeYieldsZero() {
        Setting<Integer> intTypedCopy = Setting.intSetting(MULTIPLIER_KEY, 0, -10, Setting.Property.NodeScope, Setting.Property.Dynamic);
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, intTypedCopy));

        OptionalDouble multiplier = inputs.fragmentExecutorMultiplier();
        assertTrue("a zero multiplier must degrade to the absent signal, got " + multiplier, multiplier.isEmpty());
        assertNotEquals("and must never be substituted with 1.0", OptionalDouble.of(1.0), multiplier);
    }

    // ── B2.3 — the anti-clamp rule as its own contract ────────────────────

    /**
     * The paired half of this rule — the fan-out width computed with the gate term <b>dropped</b>
     * rather than with a gate term of 1 — is a unit test on the consuming side. Neither half may be
     * deleted alone.
     */
    public void testAbsentMultiplierIsEmptyNotOne() {
        ClusterSettings clusterSettings = registry(Settings.EMPTY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        for (String mode : List.of(
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO,
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_ALL,
            SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE
        )) {
            for (int sliceCount : new int[] { 0, 1, 2, 8, 1024 }) {
                clusterSettings.applySettings(Settings.builder().put(MODE_KEY, mode).put(MAX_SLICE_COUNT_KEY, sliceCount).build());

                OptionalDouble multiplier = inputs.fragmentExecutorMultiplier();
                String cell = String.format(Locale.ROOT, "mode=%s sliceCount=%d", mode, sliceCount);
                assertFalse("no code path may synthesise a multiplier (" + cell + ")", multiplier.isPresent());
                assertNotEquals("must never be clamped to 1.0 (" + cell + ")", OptionalDouble.of(1.0), multiplier);
            }
        }
    }

    // ── B2.2 — the locally re-derived target_partitions ───────────────────

    public void testTargetPartitionsIsOneWhenConcurrentSearchModeNone() {
        ClusterSettings clusterSettings = registry(Settings.EMPTY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        for (int sliceCount : new int[] { 0, 1, 2, 8, 1024 }) {
            clusterSettings.applySettings(
                Settings.builder()
                    .put(MODE_KEY, SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_NONE)
                    .put(MAX_SLICE_COUNT_KEY, sliceCount)
                    .build()
            );

            assertEquals("mode=none forces 1 regardless of slice count " + sliceCount, 1, inputs.targetPartitions());
        }
    }

    public void testTargetPartitionsCapsAtAvailableProcessors() {
        ClusterSettings clusterSettings = registry(Settings.EMPTY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(MAX_SLICE_COUNT_KEY, 1024).build());

        assertEquals(Runtime.getRuntime().availableProcessors(), inputs.targetPartitions());
    }

    public void testTargetPartitionsUsesHalfProcessorsWhenSliceCountZero() {
        ClusterSettings clusterSettings = registry(Settings.EMPTY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(MAX_SLICE_COUNT_KEY, 0).build());

        assertEquals(Math.max(1, Runtime.getRuntime().availableProcessors() / 2), inputs.targetPartitions());
    }

    /**
     * Documents the divide-by-zero hazard: the mirror is allowed to return 0 (1-vCPU host with
     * {@code maxSliceCount == 0}) and the accessor is the only thing that clamps it.
     */
    public void testTargetPartitionsNeverZero() {
        int mirrored = DslGateInputs.deriveTargetPartitionsMirror(SearchService.CONCURRENT_SEGMENT_SEARCH_MODE_AUTO, 0);
        assertEquals(
            "the mirror must stay bit-faithful, including the 0 it can return",
            Runtime.getRuntime().availableProcessors() / 2,
            mirrored
        );

        ClusterSettings clusterSettings = registry(Settings.EMPTY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);
        clusterSettings.applySettings(Settings.builder().put(MAX_SLICE_COUNT_KEY, 0).build());

        assertEquals(Math.max(1, mirrored), inputs.targetPartitions());
        assertTrue("the accessor consumers divide by must never be 0", inputs.targetPartitions() >= 1);
    }

    /**
     * Guards against a {@code clusterService.getSettings()} regression, which would keep returning the
     * node's static default. (On a 1-vCPU host every branch collapses to 1, so the discrimination
     * comes from the dynamic-override tests on the other two inputs.)
     */
    public void testTargetPartitionsFollowsDynamicUpdate() {
        int processors = Runtime.getRuntime().availableProcessors();
        ClusterSettings clusterSettings = registry(Settings.EMPTY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        int atDefault = inputs.targetPartitions();
        assertEquals(expectedTargetPartitions(SearchService.CONCURRENT_SEGMENT_SEARCH_DEFAULT_SLICE_COUNT_VALUE), atDefault);

        clusterSettings.applySettings(Settings.builder().put(MAX_SLICE_COUNT_KEY, 1024).build());

        assertEquals(Math.max(1, processors), inputs.targetPartitions());
        if (processors >= 2) {
            assertNotEquals("a static-snapshot read would still report the node default", atDefault, inputs.targetPartitions());
        }
    }

    // ── B2.4 — the shard-request cap ──────────────────────────────────────

    public void testShardRequestCapReadWhenAnalyticsSettingRegistered() {
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, SHARD_REQUEST_CAP_DESCRIPTOR_COPY));

        // 5 comes from the registered descriptor's own default, not from a literal in main code.
        assertEquals(5, inputs.maxConcurrentShardRequestsPerNode());
    }

    /**
     * Same shape as the multiplier's guard: the 5 asserted above is the canonical default, so a hardcoded
     * {@code return 5} would pass it on the un-overridden read path. This pins the value to the registered
     * descriptor, which is the whole reason the fallback below is {@code MAX_VALUE} and not a copy of 5.
     */
    public void testShardRequestCapComesFromTheRegisteredDescriptorNotALocalDefault() {
        Setting<Integer> nonCanonicalDefault = Setting.intSetting(
            SHARD_REQUEST_CAP_KEY,
            7,
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, nonCanonicalDefault));

        int cap = inputs.maxConcurrentShardRequestsPerNode();
        assertEquals(7, cap);
        assertNotEquals("the owning plugin's default must not be duplicated here", 5, cap);
    }

    public void testShardRequestCapReadsDynamicOverride() {
        ClusterSettings clusterSettings = registry(Settings.EMPTY, SHARD_REQUEST_CAP_DESCRIPTOR_COPY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(SHARD_REQUEST_CAP_KEY, 2).build());

        assertEquals(2, inputs.maxConcurrentShardRequestsPerNode());
    }

    public void testShardRequestCapFallsBackToMaxValueWhenKeyMissing() {
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY));

        int cap = inputs.maxConcurrentShardRequestsPerNode();
        assertEquals(Integer.MAX_VALUE, cap);
        assertNotEquals("the owning plugin's default must never be duplicated here", 5, cap);
    }

    public void testShardRequestCapMaxValueWhenRegisteredTypeIsNotNumeric() {
        Setting<String> wrongType = Setting.simpleString(
            SHARD_REQUEST_CAP_KEY,
            "not-a-number",
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, wrongType));

        assertEquals(Integer.MAX_VALUE, inputs.maxConcurrentShardRequestsPerNode());
    }

    /**
     * A cap above the {@code int} range must saturate, not wrap. The read is by string, so a type change
     * on the declaring side ({@code intSetting} to {@code longSetting}) is as invisible here as the rename
     * the {@code MAX_VALUE} fallback defends against — and {@code Number.intValue()} would turn such a
     * value into a small or negative cap, i.e. the <i>smallest</i> F and therefore the <i>widest</i>
     * gate-derived fan-out. That is the one direction this read path must never fail into, so the
     * assertion is on the saturated value and explicitly not on the wrapped one.
     */
    public void testShardRequestCapSaturatesInsteadOfWrappingAboveIntRange() {
        Setting<Long> longTypedCopy = Setting.longSetting(
            SHARD_REQUEST_CAP_KEY,
            5L,
            1L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        long aboveIntRange = Integer.MAX_VALUE + 1_000L;
        ClusterSettings clusterSettings = registry(Settings.EMPTY, longTypedCopy);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(SHARD_REQUEST_CAP_KEY, aboveIntRange).build());

        assertEquals("a cap above the int range must saturate at MAX_VALUE", Integer.MAX_VALUE, inputs.maxConcurrentShardRequestsPerNode());
        assertNotEquals(
            "intValue() would wrap this to " + (int) aboveIntRange + ", which clamps to the narrowest cap and widens the fan-out",
            1,
            inputs.maxConcurrentShardRequestsPerNode()
        );
    }

    /** Pins the consumer's {@code F >= 1} from this side even if the declaring plugin relaxes its min. */
    public void testShardRequestCapNeverBelowOne() {
        Setting<Integer> zeroAllowingCopy = Setting.intSetting(
            SHARD_REQUEST_CAP_KEY,
            5,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        ClusterSettings clusterSettings = registry(Settings.EMPTY, zeroAllowingCopy);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);

        clusterSettings.applySettings(Settings.builder().put(SHARD_REQUEST_CAP_KEY, 0).build());

        assertEquals(1, inputs.maxConcurrentShardRequestsPerNode());
    }

    // ── The one-shot diagnostics — one latch per condition ────────────────

    private static final String LOGGER_NAME = "org.opensearch.dsl.settings.DslGateInputs";
    private static final String TEST_LOGGING = LOGGER_NAME + ":DEBUG";
    private static final String LOGGING_REASON = "the absence diagnostics are DEBUG-level and latched, so they need the level raised";

    private static final String MULTIPLIER_UNREGISTERED_MESSAGE = "is not registered on this node (no gated backend installed); "
        + "the concurrency-gate term is dropped";
    private static final String MULTIPLIER_NON_NUMERIC_MESSAGE = "is registered with a non-numeric value [not-a-number]; "
        + "the concurrency-gate term is dropped";
    private static final String MULTIPLIER_NON_FINITE_MESSAGE = "is registered with a non-finite value [NaN]; "
        + "the concurrency-gate term is dropped";
    private static final String MULTIPLIER_NON_POSITIVE_MESSAGE = "is registered with a non-positive value [0.0]; "
        + "the concurrency-gate term is dropped";
    private static final String CAP_UNREGISTERED_MESSAGE = "is not registered on this node; treating the per-node shard-request cap "
        + "as unbounded";
    private static final String CAP_NON_NUMERIC_MESSAGE = "is registered with a non-numeric value [not-a-number]; treating the "
        + "per-node shard-request cap as unbounded";

    /**
     * The unregistered branch must report the condition that is actually true, and must do so once —
     * this accessor runs on every fan-out decision, so an unlatched line would be per-query noise.
     */
    @TestLogging(reason = LOGGING_REASON, value = TEST_LOGGING)
    public void testUnregisteredMultiplierLogsOnlyTheUnregisteredMessageOnce() throws Exception {
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY));

        assertLogsOnFirstReadOnly(
            () -> assertTrue(inputs.fragmentExecutorMultiplier().isEmpty()),
            MULTIPLIER_UNREGISTERED_MESSAGE,
            MULTIPLIER_NON_NUMERIC_MESSAGE
        );
    }

    /**
     * The non-numeric branch has its own latch, so it reports the wrong-type cause rather than
     * inheriting the unregistered branch's wording.
     */
    @TestLogging(reason = LOGGING_REASON, value = TEST_LOGGING)
    public void testNonNumericMultiplierLogsOnlyTheNonNumericMessageOnce() throws Exception {
        Setting<String> wrongType = Setting.simpleString(
            MULTIPLIER_KEY,
            "not-a-number",
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, wrongType));

        assertLogsOnFirstReadOnly(
            () -> assertTrue(inputs.fragmentExecutorMultiplier().isEmpty()),
            MULTIPLIER_NON_NUMERIC_MESSAGE,
            MULTIPLIER_UNREGISTERED_MESSAGE
        );
    }

    /**
     * The non-finite branch has its own latch too, so the log names the cause that is actually true: a
     * value that is there and numeric but unusable, not a key that is missing.
     */
    @TestLogging(reason = LOGGING_REASON, value = TEST_LOGGING)
    public void testNonFiniteMultiplierLogsOnlyTheNonFiniteMessageOnce() throws Exception {
        ClusterSettings clusterSettings = registry(Settings.EMPTY, MULTIPLIER_DESCRIPTOR_COPY);
        DslGateInputs inputs = new DslGateInputs(clusterSettings);
        clusterSettings.applySettings(Settings.builder().put(MULTIPLIER_KEY, Double.NaN).build());

        assertLogsOnFirstReadOnly(
            () -> assertTrue(inputs.fragmentExecutorMultiplier().isEmpty()),
            MULTIPLIER_NON_FINITE_MESSAGE,
            MULTIPLIER_UNREGISTERED_MESSAGE
        );
    }

    /**
     * The non-positive branch has its own latch too, so an operator raising the level sees the domain
     * failure named rather than the non-finite branch's wording.
     */
    @TestLogging(reason = LOGGING_REASON, value = TEST_LOGGING)
    public void testNonPositiveMultiplierLogsOnlyTheNonPositiveMessageOnce() throws Exception {
        ClusterSettings clusterSettings = registry(Settings.EMPTY, negativeAllowingMultiplierCopy());
        DslGateInputs inputs = new DslGateInputs(clusterSettings);
        clusterSettings.applySettings(Settings.builder().put(MULTIPLIER_KEY, 0.0).build());

        assertLogsOnFirstReadOnly(
            () -> assertTrue(inputs.fragmentExecutorMultiplier().isEmpty()),
            MULTIPLIER_NON_POSITIVE_MESSAGE,
            MULTIPLIER_NON_FINITE_MESSAGE
        );
    }

    @TestLogging(reason = LOGGING_REASON, value = TEST_LOGGING)
    public void testUnregisteredShardRequestCapLogsOnlyTheUnregisteredMessageOnce() throws Exception {
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY));

        assertLogsOnFirstReadOnly(
            () -> assertEquals(Integer.MAX_VALUE, inputs.maxConcurrentShardRequestsPerNode()),
            CAP_UNREGISTERED_MESSAGE,
            CAP_NON_NUMERIC_MESSAGE
        );
    }

    @TestLogging(reason = LOGGING_REASON, value = TEST_LOGGING)
    public void testNonNumericShardRequestCapLogsOnlyTheNonNumericMessageOnce() throws Exception {
        Setting<String> wrongType = Setting.simpleString(
            SHARD_REQUEST_CAP_KEY,
            "not-a-number",
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY, wrongType));

        assertLogsOnFirstReadOnly(
            () -> assertEquals(Integer.MAX_VALUE, inputs.maxConcurrentShardRequestsPerNode()),
            CAP_NON_NUMERIC_MESSAGE,
            CAP_UNREGISTERED_MESSAGE
        );
    }

    /**
     * The latch must not be spendable by a read that logs nothing. Every test above raises the level
     * <i>before</i> the first read, which is the one ordering a production node never uses: it runs at
     * INFO, takes its first fan-out decision at startup, and only then does an operator raise the level
     * to find out why the fan-out is narrow. A latch tripped before the level check is already gone by
     * then and the diagnostic is unreachable for the life of the node.
     */
    public void testAbsenceDiagnosticSurvivesAReadAtInfo() throws Exception {
        Logger logger = LogManager.getLogger(LOGGER_NAME);
        Level original = logger.getLevel();
        try {
            Loggers.setLevel(logger, Level.INFO);
            DslGateInputs inputs = new DslGateInputs(registry(Settings.EMPTY));

            // The read a production node makes first, at the level a production node runs at.
            assertTrue(inputs.fragmentExecutorMultiplier().isEmpty());
            assertEquals(Integer.MAX_VALUE, inputs.maxConcurrentShardRequestsPerNode());

            Loggers.setLevel(logger, Level.DEBUG);
            try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
                appender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        MULTIPLIER_UNREGISTERED_MESSAGE,
                        LOGGER_NAME,
                        Level.DEBUG,
                        MULTIPLIER_UNREGISTERED_MESSAGE
                    )
                );
                appender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(CAP_UNREGISTERED_MESSAGE, LOGGER_NAME, Level.DEBUG, CAP_UNREGISTERED_MESSAGE)
                );

                assertTrue(inputs.fragmentExecutorMultiplier().isEmpty());
                assertEquals(Integer.MAX_VALUE, inputs.maxConcurrentShardRequestsPerNode());

                appender.assertAllExpectationsMatched();
            }
        } finally {
            Loggers.setLevel(logger, original);
        }
    }

    // ── Registered but unreadable — no throw onto the query path ──────────

    /**
     * The cast guard covers a value of the wrong <i>type</i>; this covers a value of the right type that
     * the owning {@code Setting} refuses to hand over. {@code ClusterSettings.get(Setting)} delegates
     * straight to {@code setting.get(lastSettingsApplied, settings)}, so a stored value that fails the
     * descriptor's own parse/validate surfaces as an {@code IllegalArgumentException} on the caller's
     * thread — which, for these accessors, is a SEARCH thread mid-query.
     */
    public void testMultiplierEmptyWhenRegisteredValueCannotBeParsed() {
        DslGateInputs inputs = new DslGateInputs(unparseableRegistry(MULTIPLIER_KEY, MULTIPLIER_DESCRIPTOR_COPY));

        assertTrue("an unreadable value must degrade to the absent signal", inputs.fragmentExecutorMultiplier().isEmpty());
    }

    public void testShardRequestCapMaxValueWhenRegisteredValueCannotBeParsed() {
        DslGateInputs inputs = new DslGateInputs(unparseableRegistry(SHARD_REQUEST_CAP_KEY, SHARD_REQUEST_CAP_DESCRIPTOR_COPY));

        assertEquals(Integer.MAX_VALUE, inputs.maxConcurrentShardRequestsPerNode());
    }

    /**
     * Pins that this is a real hazard and not a hypothetical one: the same registry, read through the
     * typed API the accessors use internally, does throw.
     */
    public void testUnparseableValueReallyThrowsThroughTheTypedRead() {
        ClusterSettings clusterSettings = unparseableRegistry(MULTIPLIER_KEY, MULTIPLIER_DESCRIPTOR_COPY);

        expectThrows(IllegalArgumentException.class, () -> clusterSettings.get(MULTIPLIER_DESCRIPTOR_COPY));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /**
     * The multiplier key re-declared with a relaxed minimum, standing in for the declaring plugin loosening
     * its own {@code 0.1} bound. The accessor's sign guard exists for exactly this: the read is by string, so
     * a bounds change on that side reaches this plugin without a compile error.
     */
    private static Setting<Double> negativeAllowingMultiplierCopy() {
        return Setting.doubleSetting(MULTIPLIER_KEY, 1.5, -10.0, 10.0, Setting.Property.NodeScope, Setting.Property.Dynamic);
    }

    /**
     * A registry where {@code key} is registered but its stored value is not of the descriptor's type.
     * The value goes in through the node settings rather than {@code applySettings} on purpose —
     * {@code applySettings} validates and would reject it, which is exactly why the accessors cannot
     * assume a registered key is a readable one.
     */
    private static ClusterSettings unparseableRegistry(String key, Setting<?> descriptor) {
        return registry(Settings.builder().put(key, "not-parseable-as-a-number").build(), descriptor);
    }

    /**
     * Reads twice: the first read must emit {@code expected} and never {@code otherCondition}'s wording,
     * and the second must be silent because the condition's own latch has tripped.
     */
    private static void assertLogsOnFirstReadOnly(Runnable read, String expected, String otherCondition) throws Exception {
        Logger logger = LogManager.getLogger(LOGGER_NAME);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(new MockLogAppender.SeenEventExpectation(expected, LOGGER_NAME, Level.DEBUG, expected));
            appender.addExpectation(new MockLogAppender.UnseenEventExpectation(otherCondition, LOGGER_NAME, Level.DEBUG, otherCondition));

            read.run();

            appender.assertAllExpectationsMatched();
        }

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(new MockLogAppender.UnseenEventExpectation(expected, LOGGER_NAME, Level.DEBUG, expected));

            read.run();

            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * Builds the one shared registry every accessor reads through: the server's built-in settings
     * (which already include both concurrent-segment-search settings) plus any local descriptor
     * copies standing in for another plugin's registration.
     */
    private static ClusterSettings registry(Settings nodeSettings, Setting<?>... extras) {
        Set<Setting<?>> registered = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        registered.addAll(Set.of(extras));
        return new ClusterSettings(nodeSettings, registered);
    }

    private static int expectedTargetPartitions(int maxSliceCount) {
        return Math.max(1, Math.min(maxSliceCount, Runtime.getRuntime().availableProcessors()));
    }
}
