/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;

/**
 * Settings for remote store
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class RemoteStoreSettings {

    /**
     * Used to specify the default translog buffer interval for remote store backed indexes.
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.buffer_interval",
        IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        IndexSettings.MINIMUM_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls minimum number of metadata files to keep in remote segment store.
     * {@code value < 1} will disable deletion of stale segment metadata files.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING = Setting.intSetting(
        "cluster.remote_store.index.segment_metadata.retention.max_count",
        10,
        -1,
        v -> {
            if (v == 0) {
                throw new IllegalArgumentException(
                    "Value 0 is not allowed for this setting as it would delete all the data from remote segment store"
                );
            }
        },
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls timeout value while uploading translog and checkpoint files to remote translog
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.translog.transfer_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(30),
        Property.NodeScope,
        Property.Dynamic
    );

    private volatile TimeValue clusterRemoteTranslogBufferInterval;
    private volatile int minRemoteSegmentMetadataFiles;
    private volatile TimeValue clusterRemoteTranslogTransferTimeout;

    public RemoteStoreSettings(Settings settings, ClusterSettings clusterSettings) {
        this.clusterRemoteTranslogBufferInterval = CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
            this::setClusterRemoteTranslogBufferInterval
        );

        minRemoteSegmentMetadataFiles = CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING,
            this::setMinRemoteSegmentMetadataFiles
        );

        this.clusterRemoteTranslogTransferTimeout = CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING,
            this::setClusterRemoteTranslogTransferTimeout
        );
    }

    public TimeValue getClusterRemoteTranslogBufferInterval() {
        return clusterRemoteTranslogBufferInterval;
    }

    private void setClusterRemoteTranslogBufferInterval(TimeValue clusterRemoteTranslogBufferInterval) {
        this.clusterRemoteTranslogBufferInterval = clusterRemoteTranslogBufferInterval;
    }

    private void setMinRemoteSegmentMetadataFiles(int minRemoteSegmentMetadataFiles) {
        this.minRemoteSegmentMetadataFiles = minRemoteSegmentMetadataFiles;
    }

    public int getMinRemoteSegmentMetadataFiles() {
        return this.minRemoteSegmentMetadataFiles;
    }

    public TimeValue getClusterRemoteTranslogTransferTimeout() {
        return clusterRemoteTranslogTransferTimeout;
    }

    private void setClusterRemoteTranslogTransferTimeout(TimeValue clusterRemoteTranslogTransferTimeout) {
        this.clusterRemoteTranslogTransferTimeout = clusterRemoteTranslogTransferTimeout;
    }
}
