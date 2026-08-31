/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotResilienceSettingsIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(FeatureFlags.SNAPSHOT_RESILIENCE, true).build();
    }

    public void testDynamicUpdateWithFeatureFlagEnabled() {
        internalCluster().startNode();
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put("snapshot.repository.io_timeout", "10m")
                    .put("snapshot.repository.max_outstanding_ops", 2)
                    .put("snapshot.delete.cleanup_stale_blobs", false)
                    .build()
            )
            .get();
        assertTrue(response.isAcknowledged());
    }

    public void testSettingsReflectUpdatedValues() {
        internalCluster().startNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put("snapshot.repository.io_timeout", "20m")
                    .put("snapshot.repository.max_outstanding_ops", 6)
                    .put("snapshot.delete.cleanup_stale_blobs", false)
                    .build()
            )
            .get();

        Settings settings = client().admin().cluster().prepareState().get().getState().metadata().transientSettings();
        assertEquals("20m", settings.get("snapshot.repository.io_timeout"));
        assertEquals("6", settings.get("snapshot.repository.max_outstanding_ops"));
        assertEquals("false", settings.get("snapshot.delete.cleanup_stale_blobs"));
    }

    public void testIoTimeoutRejectsInvalidValue() {
        internalCluster().startNode();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("snapshot.repository.io_timeout", "0s").build())
                .get()
        );
        assertThat(e.getMessage(), containsString("snapshot.repository.io_timeout"));
    }

    public void testMaxOutstandingOpsRejectsInvalidValue() {
        internalCluster().startNode();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("snapshot.repository.max_outstanding_ops", 0).build())
                .get()
        );
        assertThat(e.getMessage(), containsString("snapshot.repository.max_outstanding_ops"));
    }

    public void testIoTimeoutConsumerReceivesDynamicUpdate() {
        internalCluster().startNode();
        SnapshotsService snapshotsService = internalCluster().getCurrentClusterManagerNodeInstance(SnapshotsService.class);
        assertEquals(TimeValue.timeValueMinutes(30), snapshotsService.getIoTimeout());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("snapshot.repository.io_timeout", "45s").build())
            .get();

        assertEquals(TimeValue.timeValueSeconds(45), snapshotsService.getIoTimeout());
    }

    public void testIoTimeoutConsumerPicksUpNodeSetting() {
        internalCluster().startNode(Settings.builder().put("snapshot.repository.io_timeout", "2m").build());
        SnapshotsService snapshotsService = internalCluster().getCurrentClusterManagerNodeInstance(SnapshotsService.class);
        assertEquals(TimeValue.timeValueMinutes(2), snapshotsService.getIoTimeout());
    }

    public void testNodeStartsWithFeatureFlagDisabled() {
        // SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING carries a validator that throws while the flag is disabled, and Setting#get
        // runs validators. A node with the flag off must still start, holding the setting default.
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SNAPSHOT_RESILIENCE, false).build());
        SnapshotsService snapshotsService = internalCluster().getCurrentClusterManagerNodeInstance(SnapshotsService.class);
        assertEquals(TimeValue.timeValueMinutes(30), snapshotsService.getIoTimeout());
    }

    public void testUnrelatedSettingsUpdateSucceedsWithFeatureFlagDisabled() {
        // Regression test. AbstractScopedSettings#validateUpdate calls getValue on every registered settings updater for every
        // cluster settings update, and getValue runs the setting's validator. Registering an io_timeout updater on a flag-off
        // node therefore used to make every unrelated cluster settings update fail with
        // "illegal value can't update [snapshot.repository.io_timeout]".
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SNAPSHOT_RESILIENCE, false).build());

        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("snapshot.max_concurrent_operations", 500).build())
            .get();
        assertTrue(response.isAcknowledged());
    }

    public void testSettingsRejectedWhenFlagDisabled() {
        // Start a node with the flag explicitly disabled
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SNAPSHOT_RESILIENCE, false).build());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("snapshot.repository.io_timeout", "10m").build())
                .get()
        );
        assertThat(e.getMessage(), containsString("feature flag"));
        assertThat(e.getMessage(), containsString("disabled"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("snapshot.repository.max_outstanding_ops", 2).build())
                .get()
        );
        assertThat(e.getMessage(), containsString("feature flag"));
        assertThat(e.getMessage(), containsString("disabled"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("snapshot.delete.cleanup_stale_blobs", false).build())
                .get()
        );
        assertThat(e.getMessage(), containsString("feature flag"));
        assertThat(e.getMessage(), containsString("disabled"));
    }
}
