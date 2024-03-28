/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING;

public class RemoteStoreSettingsDynamicUpdateTests extends OpenSearchTestCase {
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(Settings.EMPTY, clusterSettings);

    public void testSegmentMetadataRetention() {
        // Default value
        assertEquals(10, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value < default (10)
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), 5)
                .build()
        );
        assertEquals(5, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting min value
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), -1)
                .build()
        );
        assertEquals(-1, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value > default (10)
        clusterSettings.applySettings(
            Settings.builder()
                .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), 15)
                .build()
        );
        assertEquals(15, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value to 0 should fail and retain the existing value
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder()
                    .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), 0)
                    .build()
            )
        );
        assertEquals(15, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());

        // Setting value < -1 should fail and retain the existing value
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder()
                    .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), -5)
                    .build()
            )
        );
        assertEquals(15, remoteStoreSettings.getMinRemoteSegmentMetadataFiles());
    }

    public void testClusterRemoteTranslogTransferTimeout() {
        // Test default value
        assertEquals(TimeValue.timeValueSeconds(30), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());

        // Test override with valid value
        clusterSettings.applySettings(Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.getKey(), "40s").build());
        assertEquals(TimeValue.timeValueSeconds(40), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());

        // Test override with value less than minimum
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.getKey(), "10s").build()
            )
        );
        assertEquals(TimeValue.timeValueSeconds(40), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());

        // Test override with invalid time value
        assertThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.getKey(), "123").build()
            )
        );
        assertEquals(TimeValue.timeValueSeconds(40), remoteStoreSettings.getClusterRemoteTranslogTransferTimeout());
    }
}
