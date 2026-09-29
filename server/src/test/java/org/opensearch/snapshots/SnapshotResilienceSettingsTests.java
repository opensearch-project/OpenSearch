/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchTestCase;

public class SnapshotResilienceSettingsTests extends OpenSearchTestCase {

    public void testIoTimeoutDefault() {
        assertEquals(TimeValue.timeValueMinutes(30), SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getDefault(Settings.EMPTY));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testIoTimeoutRoundTrip() {
        Settings settings = Settings.builder().put("snapshot.repository.io_timeout", "15m").build();
        assertEquals(TimeValue.timeValueMinutes(15), SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(settings));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testIoTimeoutRejectsZero() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(
                Settings.builder().put("snapshot.repository.io_timeout", "0s").build()
            )
        );
        assertTrue(e.getMessage().contains("snapshot.repository.io_timeout"));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testIoTimeoutAcceptsMinimum() {
        Settings settings = Settings.builder().put("snapshot.repository.io_timeout", "1s").build();
        assertEquals(TimeValue.timeValueSeconds(1), SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(settings));
    }

    @LockFeatureFlag(FeatureFlags.SNAPSHOT_RESILIENCE)
    public void testIoTimeoutIsRegisteredAndDynamicallyUpdatable() {
        assertTrue(
            "io_timeout should be registered",
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING)
        );

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings newSettings = Settings.builder().put("snapshot.repository.io_timeout", "10m").build();
        clusterSettings.applySettings(newSettings);
        assertEquals(TimeValue.timeValueMinutes(10), clusterSettings.get(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING));
    }

    public void testSettingsRejectedWhenFeatureFlagDisabled() {
        // Without @LockFeatureFlag, the flag is off by default
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(
                Settings.builder().put("snapshot.repository.io_timeout", "10m").build()
            )
        );
        assertTrue(e.getMessage().contains("feature flag"));
        assertTrue(e.getMessage().contains("disabled"));
    }

    public void testWithdrawnSettingsAreNotRegistered() {
        assertUnknownNodeSetting("snapshot.repository.max_outstanding_ops", "2");
        assertUnknownNodeSetting("snapshot.delete.cleanup_stale_blobs", "false");
    }

    private void assertUnknownNodeSetting(String key, String value) {
        Settings settings = Settings.builder().put(key, value).build();
        SettingsException e = expectThrows(SettingsException.class, () -> new SettingsModule(settings));
        assertTrue(e.getMessage(), e.getMessage().contains("unknown setting [" + key + "]"));
    }
}
