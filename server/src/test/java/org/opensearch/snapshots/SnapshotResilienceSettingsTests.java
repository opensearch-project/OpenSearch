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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class SnapshotResilienceSettingsTests extends OpenSearchTestCase {

    public void testIoTimeoutDefault() {
        assertEquals(TimeValue.timeValueMinutes(30), SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.getDefault(Settings.EMPTY));
    }

    public void testMaxOutstandingOpsDefault() {
        assertEquals(4, (int) SnapshotsService.SNAPSHOT_REPOSITORY_MAX_OUTSTANDING_OPS_SETTING.getDefault(Settings.EMPTY));
    }

    public void testCleanupStaleBlobsDefault() {
        assertTrue(SnapshotsService.SNAPSHOT_DELETE_CLEANUP_STALE_BLOBS_SETTING.getDefault(Settings.EMPTY));
    }

    public void testIoTimeoutRoundTrip() {
        Settings settings = Settings.builder().put("snapshot.repository.io_timeout", "15m").build();
        assertEquals(TimeValue.timeValueMinutes(15), SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(settings));
    }

    public void testMaxOutstandingOpsRoundTrip() {
        Settings settings = Settings.builder().put("snapshot.repository.max_outstanding_ops", 8).build();
        assertEquals(8, (int) SnapshotsService.SNAPSHOT_REPOSITORY_MAX_OUTSTANDING_OPS_SETTING.get(settings));
    }

    public void testCleanupStaleBlobsRoundTrip() {
        Settings settings = Settings.builder().put("snapshot.delete.cleanup_stale_blobs", false).build();
        assertFalse(SnapshotsService.SNAPSHOT_DELETE_CLEANUP_STALE_BLOBS_SETTING.get(settings));
    }

    public void testIoTimeoutRejectsZero() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(
                Settings.builder().put("snapshot.repository.io_timeout", "0s").build()
            )
        );
        assertTrue(e.getMessage().contains("snapshot.repository.io_timeout"));
    }

    public void testIoTimeoutAcceptsMinimum() {
        Settings settings = Settings.builder().put("snapshot.repository.io_timeout", "1s").build();
        assertEquals(TimeValue.timeValueSeconds(1), SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING.get(settings));
    }

    public void testMaxOutstandingOpsRejectsZero() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SnapshotsService.SNAPSHOT_REPOSITORY_MAX_OUTSTANDING_OPS_SETTING.get(
                Settings.builder().put("snapshot.repository.max_outstanding_ops", 0).build()
            )
        );
        assertTrue(e.getMessage().contains("snapshot.repository.max_outstanding_ops"));
    }

    public void testSettingsRegisteredInClusterSettings() {
        assertTrue(
            "io_timeout should be registered",
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SnapshotsService.SNAPSHOT_REPOSITORY_IO_TIMEOUT_SETTING)
        );
        assertTrue(
            "max_outstanding_ops should be registered",
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SnapshotsService.SNAPSHOT_REPOSITORY_MAX_OUTSTANDING_OPS_SETTING)
        );
        assertTrue(
            "cleanup_stale_blobs should be registered",
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(SnapshotsService.SNAPSHOT_DELETE_CLEANUP_STALE_BLOBS_SETTING)
        );
    }

    public void testDynamicUpdateAccepted() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings newSettings = Settings.builder()
            .put("snapshot.repository.io_timeout", "10m")
            .put("snapshot.repository.max_outstanding_ops", 2)
            .put("snapshot.delete.cleanup_stale_blobs", false)
            .build();
        clusterSettings.applySettings(newSettings);
    }
}
