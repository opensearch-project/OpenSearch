/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.wlm.WorkloadManagementSettings.NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME;
import static org.opensearch.wlm.WorkloadManagementSettings.NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME;
import static org.opensearch.wlm.WorkloadManagementSettings.NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME;
import static org.opensearch.wlm.WorkloadManagementSettings.NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME;
import static org.hamcrest.Matchers.containsString;

public class WorkloadManagementSettingsTests extends OpenSearchTestCase {

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.memory_rejection_threshold}
     * When the value is set more than {@code wlm.workload_group.node.memory_cancellation_threshold} accidentally during
     * new feature development. This test is to ensure that {@link WorkloadManagementSettings} holds the
     * invariant {@code nodeLevelRejectionThreshold < nodeLevelCancellationThreshold}
     */
    public void testInvalidMemoryInstantiationOfWorkloadManagementSettings() {
        Settings settings = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.8)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.7)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        assertThrows(IllegalArgumentException.class, () -> new WorkloadManagementSettings(settings, cs));
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.cpu_rejection_threshold}
     * When the value is set more than {@code wlm.workload_group.node.cpu_cancellation_threshold} accidentally during
     * new feature development. This test is to ensure that {@link WorkloadManagementSettings} holds the
     * invariant {@code nodeLevelRejectionThreshold < nodeLevelCancellationThreshold}
     */
    public void testInvalidCpuInstantiationOfWorkloadManagementSettings() {
        Settings settings = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.8)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.7)
            .build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        assertThrows(IllegalArgumentException.class, () -> new WorkloadManagementSettings(settings, cs));
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.cpu_rejection_threshold}
     * Using setNodeLevelCpuRejectionThreshold function
     */
    public void testValidNodeLevelCpuRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelCpuRejectionThreshold(0.7);
        assertEquals(0.7, workloadManagementSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.cpu_rejection_threshold}
     */
    public void testValidNodeLevelCpuRejectionThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.79).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.79, workloadManagementSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.cpu_rejection_threshold}
     * When the value is set more than {@literal 0.9}. The max is enforced at settings-update validation time.
     */
    public void testInvalidNodeLevelCpuRejectionThresholdCase1() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.95).build();
        assertThrows(IllegalArgumentException.class, () -> cs.validate(update, true));
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.cpu_rejection_threshold}
     * When the value is set more than {@code wlm.workload_group.node.cpu_cancellation_threshold}. The ordering
     * invariant is enforced at settings-update validation time (not in the setter), so it is exercised here through
     * the validation path.
     */
    public void testInvalidNodeLevelCpuRejectionThresholdCase2() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        // rejection 0.85 > cancellation 0.8 violates the invariant
        Settings update = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.85)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();
        assertThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.cpu_cancellation_threshold}
     */
    public void testValidNodeLevelCpuCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.8, workloadManagementSettings.getNodeLevelCpuRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.cpu_cancellation_threshold}
     * Using setNodeLevelCpuCancellationThreshold function
     */
    public void testValidNodeLevelCpuCancellationThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelCpuCancellationThreshold(0.83);
        assertEquals(0.83, workloadManagementSettings.getNodeLevelCpuCancellationThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.cpu_cancellation_threshold}
     * When the value is set more than {@literal 0.95}. The max is enforced at settings-update validation time.
     */
    public void testInvalidNodeLevelCpuCancellationThresholdCase1() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.96).build();
        assertThrows(IllegalArgumentException.class, () -> cs.validate(update, true));
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.cpu_cancellation_threshold}
     * When the value is set less than {@code wlm.workload_group.node.cpu_rejection_threshold}. The ordering invariant
     * is enforced at settings-update validation time (not in the setter), so it is exercised here through the
     * validation path.
     */
    public void testInvalidNodeLevelCpuCancellationThresholdCase2() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        // cancellation 0.65 < rejection 0.7 violates the invariant
        Settings update = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.7)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.65)
            .build();
        assertThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.memory_cancellation_threshold}
     */
    public void testValidNodeLevelMemoryCancellationThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.8, workloadManagementSettings.getNodeLevelMemoryCancellationThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.memory_cancellation_threshold}
     * Using setNodeLevelMemoryCancellationThreshold function
     */
    public void testValidNodeLevelMemoryCancellationThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.8).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelMemoryCancellationThreshold(0.83);
        assertEquals(0.83, workloadManagementSettings.getNodeLevelMemoryCancellationThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.memory_cancellation_threshold}
     * When the value is set more than {@literal 0.95}. The max is enforced at settings-update validation time.
     */
    public void testInvalidNodeLevelMemoryCancellationThresholdCase1() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.96).build();
        assertThrows(IllegalArgumentException.class, () -> cs.validate(update, true));
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.memory_cancellation_threshold}
     * When the value is set less than {@code wlm.workload_group.node.memory_rejection_threshold}. The ordering
     * invariant is enforced at settings-update validation time (not in the setter), so it is exercised here through
     * the validation path.
     */
    public void testInvalidNodeLevelMemoryCancellationThresholdCase2() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        // cancellation 0.65 < rejection 0.7 violates the invariant
        Settings update = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.7)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.65)
            .build();
        assertThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.memory_rejection_threshold}
     */
    public void testValidNodeLevelMemoryRejectionThresholdCase1() {
        Settings settings = Settings.builder().put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.79).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        assertEquals(0.79, workloadManagementSettings.getNodeLevelMemoryRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the valid value for {@code wlm.workload_group.node.memory_rejection_threshold}
     * Using setNodeLevelMemoryRejectionThreshold function
     */
    public void testValidNodeLevelMemoryRejectionThresholdCase2() {
        Settings settings = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.9).build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(settings, cs);
        workloadManagementSettings.setNodeLevelMemoryRejectionThreshold(0.86);
        assertEquals(0.86, workloadManagementSettings.getNodeLevelMemoryRejectionThreshold(), 1e-9);
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.memory_rejection_threshold}
     * When the value is set more than {@literal 0.9}. The max is enforced at settings-update validation time.
     */
    public void testInvalidNodeLevelMemoryRejectionThresholdCase1() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder().put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.92).build();
        assertThrows(IllegalArgumentException.class, () -> cs.validate(update, true));
    }

    /**
     * Tests the invalid value for {@code wlm.workload_group.node.memory_rejection_threshold}
     * When the value is set more than {@code wlm.workload_group.node.memory_cancellation_threshold}. The ordering
     * invariant is enforced at settings-update validation time (not in the setter), so it is exercised here through
     * the validation path.
     */
    public void testInvalidNodeLevelMemoryRejectionThresholdCase2() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        // rejection 0.85 > cancellation 0.8 violates the invariant
        Settings update = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.85)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();
        assertThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
    }

    /**
     * Reproduces the production incident where a dynamic cluster settings update set
     * {@code wlm.workload_group.node.cpu_cancellation_threshold} above its maximum. Prior to the fix, the value passed
     * the settings-update dry-run validation ({@link ClusterSettings#validateUpdate}) and was committed to cluster state,
     * only to throw while the new state was being applied on every node. That apply-time failure destabilized the
     * cluster-manager. This test asserts the update is instead rejected up-front at validation time.
     */
    public void testCpuCancellationThresholdAboveMaxRejectedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.98).build();

        // validateUpdate is the dry-run the cluster settings API runs before committing the new cluster state.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
        assertThat(e.getMessage(), containsString(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME));

        // validate(...) is the other check the settings API applies to the final persistent/transient settings.
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> cs.validate(update, true));
        assertThat(e2.getMessage(), containsString(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME));
    }

    /**
     * A value at exactly the maximum must be accepted through the settings-update validation path.
     */
    public void testCpuCancellationThresholdAtMaxAcceptedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings wlm = new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder()
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, WorkloadManagementSettings.NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE)
            .build();

        cs.validateUpdate(update);
        cs.validate(update, true);
        cs.applySettings(update);
        assertEquals(
            WorkloadManagementSettings.NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE,
            wlm.getNodeLevelCpuCancellationThreshold(),
            1e-9
        );
    }

    /**
     * The same up-front rejection must hold for the memory cancellation threshold.
     */
    public void testMemoryCancellationThresholdAboveMaxRejectedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder().put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.98).build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
        assertThat(e.getMessage(), containsString(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME));
    }

    /**
     * The rejection-vs-cancellation ordering invariant must also be enforced at settings-update validation time,
     * not only via the setters.
     */
    public void testCpuCancellationBelowRejectionRejectedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        // rejection (0.85) > cancellation (0.8) violates the invariant
        Settings update = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.85)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();

        assertThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
    }

    /**
     * The rejection-threshold max (0.90) must be enforced through the validation path for both cpu and memory, so a
     * mis-wired rejection validator (wrong MAX constant / not attached) would be caught. The paired cancellation is
     * raised to 0.95 in the same update so that the ordering invariant is satisfied and the rejection max is the sole
     * trigger.
     */
    public void testRejectionThresholdAboveMaxRejectedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);

        Settings cpu = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.95) // > 0.90 rejection max
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.95) // keeps rejection <= cancellation
            .build();
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> cs.validateUpdate(cpu));
        assertThat(e1.getMessage(), containsString(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME));

        Settings memory = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.95)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.95)
            .build();
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> cs.validateUpdate(memory));
        assertThat(e2.getMessage(), containsString(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME));
    }

    /**
     * The memory ordering violation must be rejected via the validation path (mirror of the cpu ordering test), so a
     * regression swapping the memory validators' dependency setting or argument order is caught.
     */
    public void testMemoryCancellationBelowRejectionRejectedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.85)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();
        assertThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
    }

    /**
     * Boundary: rejection == cancellation is consistent (invariant is {@code rejection <= cancellation}) and must be
     * accepted, so the ordering check is not wrongly strict.
     */
    public void testEqualRejectionAndCancellationAcceptedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings wlm = new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.8)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.8)
            .build();

        cs.validateUpdate(update);
        cs.validate(update, true);
        cs.applySettings(update); // must not throw

        assertEquals(0.8, wlm.getNodeLevelCpuRejectionThreshold(), 1e-9);
        assertEquals(0.8, wlm.getNodeLevelCpuCancellationThreshold(), 1e-9);
    }

    /**
     * Lowering BOTH thresholds in a single update to a new, consistent state (rejection stays below cancellation) must
     * succeed end-to-end. This guards against a regression where the ordering check ran in the apply-time setter against
     * a stale sibling field: the cancellation consumer applied first would compare the new cancellation against the old
     * (higher) rejection and throw while applying committed state, destabilizing the cluster-manager.
     */
    public void testLoweringBothCpuThresholdsTogetherIsAppliedCleanly() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings wlm = new WorkloadManagementSettings(Settings.EMPTY, cs); // rejection=0.8, cancellation=0.9
        Settings update = Settings.builder()
            .put(NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME, 0.6)
            .put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.7)
            .build();

        cs.validateUpdate(update);
        cs.validate(update, true);
        cs.applySettings(update); // must not throw

        assertEquals(0.6, wlm.getNodeLevelCpuRejectionThreshold(), 1e-9);
        assertEquals(0.7, wlm.getNodeLevelCpuCancellationThreshold(), 1e-9);
    }

    /**
     * Same as above for the memory pair.
     */
    public void testLoweringBothMemoryThresholdsTogetherIsAppliedCleanly() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings wlm = new WorkloadManagementSettings(Settings.EMPTY, cs);
        Settings update = Settings.builder()
            .put(NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME, 0.6)
            .put(NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME, 0.7)
            .build();

        cs.validateUpdate(update);
        cs.validate(update, true);
        cs.applySettings(update); // must not throw

        assertEquals(0.6, wlm.getNodeLevelMemoryRejectionThreshold(), 1e-9);
        assertEquals(0.7, wlm.getNodeLevelMemoryCancellationThreshold(), 1e-9);
    }

    /**
     * The setters run as the settings-update consumers at apply time and only assign; all bounds and ordering are
     * enforced up front by {@code NodeLevelThresholdValidator} at validation time. This guards against reintroducing an
     * apply-time throw in a setter (which, for the cross-setting ordering invariant, destabilized the cluster-manager).
     */
    public void testSettersOnlyAssignAndDoNotThrow() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        WorkloadManagementSettings wlm = new WorkloadManagementSettings(Settings.EMPTY, cs);

        // Values that validation would reject (out of range, and cancellation < rejection) are still assigned by the
        // raw setters without throwing, because the setters no longer validate.
        wlm.setNodeLevelCpuCancellationThreshold(0.99);
        assertEquals(0.99, wlm.getNodeLevelCpuCancellationThreshold(), 1e-9);
        wlm.setNodeLevelCpuRejectionThreshold(-0.1);
        assertEquals(-0.1, wlm.getNodeLevelCpuRejectionThreshold(), 1e-9);
        wlm.setNodeLevelMemoryCancellationThreshold(0.1);
        wlm.setNodeLevelMemoryRejectionThreshold(0.9); // rejection > cancellation; setter does not enforce ordering
        assertEquals(0.9, wlm.getNodeLevelMemoryRejectionThreshold(), 1e-9);
    }

    /**
     * A negative threshold is nonsensical and must be rejected at settings-update validation time by the validator's
     * lower bound, for each of the four node threshold settings.
     */
    public void testNegativeNodeThresholdsRejectedAtSettingsUpdateValidation() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new WorkloadManagementSettings(Settings.EMPTY, cs);

        for (String key : new String[] {
            NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME,
            NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME,
            NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME }) {
            Settings update = Settings.builder().put(key, -0.1).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> cs.validateUpdate(update));
            assertThat(e.getMessage(), containsString(key));
        }
    }

    /**
     * A cluster that already carries an out-of-range value in persisted cluster state (as in the incident, where
     * {@code 0.98} was committed before the fix) must be able to recover: on state recovery the invalid setting is
     * archived rather than applied. This verifies the recovery/self-heal path enabled by the validator.
     */
    public void testOutOfRangeCpuCancellationThresholdIsArchivedOnRecovery() {
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings poisoned = Settings.builder().put(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME, 0.98).build();

        Settings archived = cs.archiveUnknownOrInvalidSettings(poisoned, e -> {}, (e, ex) -> {});

        // The active (non-archived) key is dropped and preserved under the archived prefix for visibility.
        assertNull(archived.get(NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME));
        assertEquals("0.98", archived.get("archived." + NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME));
    }
}
