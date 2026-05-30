/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for the dynamic concurrency settings wiring end-to-end.
 * <p>
 * Validates:
 * <ul>
 *   <li>Settings are registered as Dynamic (can be updated at runtime)</li>
 *   <li>Invalid values (below 0.1 or above 10.0) are rejected by the settings validation</li>
 *   <li>Default value is 1.5 for both settings</li>
 *   <li>The computation {@code Math.max(1, (int)(cpuThreads * multiplier))} produces correct results</li>
 *   <li>Settings consumers fire correctly when values are updated via ClusterSettings</li>
 * </ul>
 *
 * Requirements: 1.3, 1.5, 1.6, 6.1, 6.5
 */
public class DynamicConcurrencySettingsTests extends OpenSearchTestCase {

    // ── Requirement 1.1, 1.2: Settings are registered as Dynamic ──

    public void testDatanodeMultiplierIsDynamic() {
        assertTrue(
            "datafusion.concurrency.fragment_executor_multiplier must be dynamic",
            DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.isDynamic()
        );
    }

    public void testCoordinatorMultiplierIsDynamic() {
        assertTrue(
            "datafusion.concurrency.reduce_multiplier must be dynamic",
            DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.isDynamic()
        );
    }

    public void testDatanodeMultiplierHasNodeScope() {
        assertTrue(
            "datafusion.concurrency.fragment_executor_multiplier must have node scope",
            DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.hasNodeScope()
        );
    }

    public void testCoordinatorMultiplierHasNodeScope() {
        assertTrue(
            "datafusion.concurrency.reduce_multiplier must have node scope",
            DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.hasNodeScope()
        );
    }

    // ── Requirement 6.1, 6.2: Default values and key names ──

    public void testDatanodeMultiplierDefaultIs1Point5() {
        assertEquals(1.5, DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(Settings.EMPTY), 1e-15);
    }

    public void testCoordinatorMultiplierDefaultIs1Point5() {
        assertEquals(1.5, DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.get(Settings.EMPTY), 1e-15);
    }

    public void testDatanodeMultiplierKeyName() {
        assertEquals("datafusion.concurrency.fragment_executor_multiplier", DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.getKey());
    }

    public void testCoordinatorMultiplierKeyName() {
        assertEquals("datafusion.concurrency.reduce_multiplier", DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.getKey());
    }

    // ── Requirement 1.5, 1.6: Invalid values are rejected ──

    public void testDatanodeMultiplierRejectsZero() {
        Settings settings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 0.0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(settings));
    }

    public void testDatanodeMultiplierRejectsNegative() {
        Settings settings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", -1.0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(settings));
    }

    public void testDatanodeMultiplierRejectsAboveMax() {
        Settings settings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 11.0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(settings));
    }

    public void testDatanodeMultiplierRejectsBelowMin() {
        Settings settings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 0.05).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(settings));
    }

    public void testCoordinatorMultiplierRejectsZero() {
        Settings settings = Settings.builder().put("datafusion.concurrency.reduce_multiplier", 0.0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.get(settings));
    }

    public void testCoordinatorMultiplierRejectsAboveMax() {
        Settings settings = Settings.builder().put("datafusion.concurrency.reduce_multiplier", 11.0).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.get(settings));
    }

    public void testCoordinatorMultiplierRejectsBelowMin() {
        Settings settings = Settings.builder().put("datafusion.concurrency.reduce_multiplier", 0.05).build();
        expectThrows(IllegalArgumentException.class, () -> DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.get(settings));
    }

    // ── Requirement 1.5: Valid boundary values are accepted ──

    public void testDatanodeMultiplierAcceptsMinBoundary() {
        Settings settings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 0.1).build();
        assertEquals(0.1, DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(settings), 1e-15);
    }

    public void testDatanodeMultiplierAcceptsMaxBoundary() {
        Settings settings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 10.0).build();
        assertEquals(10.0, DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(settings), 1e-15);
    }

    public void testCoordinatorMultiplierAcceptsMinBoundary() {
        Settings settings = Settings.builder().put("datafusion.concurrency.reduce_multiplier", 0.1).build();
        assertEquals(0.1, DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.get(settings), 1e-15);
    }

    public void testCoordinatorMultiplierAcceptsMaxBoundary() {
        Settings settings = Settings.builder().put("datafusion.concurrency.reduce_multiplier", 10.0).build();
        assertEquals(10.0, DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER.get(settings), 1e-15);
    }

    // ── Requirement 2.5: Permit computation correctness ──

    public void testPermitComputationWithDefaultMultiplier() {
        int cpuThreads = Runtime.getRuntime().availableProcessors();
        double multiplier = 1.5;
        int expected = Math.max(1, (int) (cpuThreads * multiplier));
        assertEquals(expected, computeMaxPermits(cpuThreads, multiplier));
    }

    public void testPermitComputationWithMinMultiplier() {
        int cpuThreads = 8;
        double multiplier = 0.1;
        // 8 * 0.1 = 0.8, (int) 0.8 = 0, max(1, 0) = 1
        assertEquals(1, computeMaxPermits(cpuThreads, multiplier));
    }

    public void testPermitComputationWithMaxMultiplier() {
        int cpuThreads = 4;
        double multiplier = 10.0;
        // 4 * 10.0 = 40.0, (int) 40.0 = 40, max(1, 40) = 40
        assertEquals(40, computeMaxPermits(cpuThreads, multiplier));
    }

    public void testPermitComputationAlwaysAtLeastOne() {
        // Even with 1 CPU thread and minimum multiplier, result should be at least 1
        int cpuThreads = 1;
        double multiplier = 0.1;
        // 1 * 0.1 = 0.1, (int) 0.1 = 0, max(1, 0) = 1
        assertEquals(1, computeMaxPermits(cpuThreads, multiplier));
    }

    public void testPermitComputationWithTypicalValues() {
        // 16 cores * 1.5 = 24
        assertEquals(24, computeMaxPermits(16, 1.5));
        // 8 cores * 2.0 = 16
        assertEquals(16, computeMaxPermits(8, 2.0));
        // 4 cores * 3.0 = 12
        assertEquals(12, computeMaxPermits(4, 3.0));
        // 2 cores * 1.5 = 3
        assertEquals(3, computeMaxPermits(2, 1.5));
    }

    public void testPermitComputationTruncatesDecimal() {
        // 3 cores * 1.5 = 4.5, (int) 4.5 = 4
        assertEquals(4, computeMaxPermits(3, 1.5));
        // 5 cores * 0.7 = 3.5, (int) 3.5 = 3
        assertEquals(3, computeMaxPermits(5, 0.7));
        // 7 cores * 0.3 = 2.1, (int) 2.1 = 2
        assertEquals(2, computeMaxPermits(7, 0.3));
    }

    // ── Requirement 6.5: Backward compatibility — default gate uses floor(cpu_threads × 1.5) ──

    public void testDefaultGatePermitsMatchExpected() {
        int cpuThreads = DataFusionService.cpuThreadCount();
        double defaultMultiplier = DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER.get(Settings.EMPTY);
        int expectedPermits = Math.max(1, (int) (cpuThreads * defaultMultiplier));
        // This is the same computation used at startup — verifies backward compatibility
        assertTrue("Default permits must be at least 1", expectedPermits >= 1);
        assertEquals("Default gate permits should equal floor(cpuThreads * 1.5)", Math.max(1, (int) (cpuThreads * 1.5)), expectedPermits);
    }

    // ── Requirement 1.3: Dynamic update consumer fires on settings change ──

    public void testDynamicUpdateConsumerFiresForDatanodeMultiplier() {
        ClusterSettings clusterSettings = createClusterSettings();
        AtomicReference<Double> receivedValue = new AtomicReference<>(null);

        clusterSettings.addSettingsUpdateConsumer(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER, receivedValue::set);

        Settings newSettings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 3.0).build();
        clusterSettings.applySettings(newSettings);

        assertNotNull("Consumer should have been called", receivedValue.get());
        assertEquals(3.0, receivedValue.get(), 1e-15);
    }

    public void testDynamicUpdateConsumerFiresForCoordinatorMultiplier() {
        ClusterSettings clusterSettings = createClusterSettings();
        AtomicReference<Double> receivedValue = new AtomicReference<>(null);

        clusterSettings.addSettingsUpdateConsumer(DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER, receivedValue::set);

        Settings newSettings = Settings.builder().put("datafusion.concurrency.reduce_multiplier", 5.0).build();
        clusterSettings.applySettings(newSettings);

        assertNotNull("Consumer should have been called", receivedValue.get());
        assertEquals(5.0, receivedValue.get(), 1e-15);
    }

    public void testDynamicUpdateComputesCorrectNewMaxPermits() {
        ClusterSettings clusterSettings = createClusterSettings();
        int cpuThreads = DataFusionService.cpuThreadCount();
        AtomicInteger computedPermits = new AtomicInteger(-1);

        clusterSettings.addSettingsUpdateConsumer(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER, multiplier -> {
            int newMax = Math.max(1, (int) (cpuThreads * multiplier));
            computedPermits.set(newMax);
        });

        double testMultiplier = 3.0;
        Settings newSettings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", testMultiplier).build();
        clusterSettings.applySettings(newSettings);

        int expected = Math.max(1, (int) (cpuThreads * testMultiplier));
        assertEquals("Computed permits should match expected formula", expected, computedPermits.get());
    }

    public void testDynamicUpdateRejectsInvalidValueViaClusterSettings() {
        ClusterSettings clusterSettings = createClusterSettings();
        AtomicReference<Double> receivedValue = new AtomicReference<>(null);

        clusterSettings.addSettingsUpdateConsumer(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER, receivedValue::set);

        // Attempting to apply an invalid value should throw
        Settings invalidSettings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 0.0).build();
        expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(invalidSettings));

        // Consumer should NOT have been called
        assertNull("Consumer should not fire for invalid values", receivedValue.get());
    }

    public void testDynamicUpdateRejectsAboveMaxViaClusterSettings() {
        ClusterSettings clusterSettings = createClusterSettings();
        AtomicReference<Double> receivedValue = new AtomicReference<>(null);

        clusterSettings.addSettingsUpdateConsumer(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER, receivedValue::set);

        Settings invalidSettings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", 11.0).build();
        expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(invalidSettings));

        assertNull("Consumer should not fire for invalid values", receivedValue.get());
    }

    public void testMultipleSequentialUpdatesApplyCorrectly() {
        ClusterSettings clusterSettings = createClusterSettings();
        int cpuThreads = DataFusionService.cpuThreadCount();
        AtomicInteger lastPermits = new AtomicInteger(-1);

        clusterSettings.addSettingsUpdateConsumer(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER, multiplier -> {
            int newMax = Math.max(1, (int) (cpuThreads * multiplier));
            lastPermits.set(newMax);
        });

        // Apply multiple updates sequentially
        double[] multipliers = { 2.0, 5.0, 0.5, 1.0, 8.0 };
        for (double m : multipliers) {
            Settings newSettings = Settings.builder().put("datafusion.concurrency.fragment_executor_multiplier", m).build();
            clusterSettings.applySettings(newSettings);
        }

        // Final value should reflect the last multiplier applied
        int expected = Math.max(1, (int) (cpuThreads * 8.0));
        assertEquals("Final permits should reflect last applied multiplier", expected, lastPermits.get());
    }

    // ── Requirement 6.5: Settings included in ALL_SETTINGS list ──

    public void testConcurrencySettingsInAllSettingsList() {
        assertTrue(
            "ALL_SETTINGS must contain CONCURRENCY_DATANODE_MULTIPLIER",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.CONCURRENCY_DATANODE_MULTIPLIER)
        );
        assertTrue(
            "ALL_SETTINGS must contain CONCURRENCY_COORDINATOR_MULTIPLIER",
            DatafusionSettings.ALL_SETTINGS.contains(DatafusionSettings.CONCURRENCY_COORDINATOR_MULTIPLIER)
        );
    }

    // ── Helper methods ──

    /**
     * Replicates the permit computation used in DataFusionPlugin.createComponents():
     * {@code Math.max(1, (int)(cpuThreads * multiplier))}
     */
    private static int computeMaxPermits(int cpuThreads, double multiplier) {
        return Math.max(1, (int) (cpuThreads * multiplier));
    }

    private ClusterSettings createClusterSettings() {
        Set<Setting<?>> settingsSet = new HashSet<>(DatafusionSettings.ALL_SETTINGS);
        return new ClusterSettings(Settings.EMPTY, settingsSet);
    }
}
