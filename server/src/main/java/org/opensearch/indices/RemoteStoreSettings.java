/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.remote.RemoteStoreEnums;

/**
 * Settings for remote store
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class RemoteStoreSettings {
    private static final int MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS = 100;

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

    /**
     * This setting is used to set the remote store blob store path type strategy. This setting is effective only for
     * remote store enabled cluster.
     */
    @ExperimentalApi
    public static final Setting<RemoteStoreEnums.PathType> CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING = new Setting<>(
        "cluster.remote_store.index.path.type",
        RemoteStoreEnums.PathType.FIXED.toString(),
        RemoteStoreEnums.PathType::parseString,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * This setting is used to disable uploading translog.ckp file as metadata to translog.tlog. This setting is effective only for
     * repositories that supports metadata read and write with metadata and is applicable for only remote store enabled clusters.
     */
    @ExperimentalApi
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_TRANSLOG_METADATA = Setting.boolSetting(
        "cluster.remote_store.index.translog.translog_metadata",
        true,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * This setting is used to set the remote store blob store path hash algorithm strategy. This setting is effective only for
     * remote store enabled cluster. This setting will come to effect if the {@link #CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING}
     * is either {@code HASHED_PREFIX} or {@code HASHED_INFIX}.
     */
    @ExperimentalApi
    public static final Setting<RemoteStoreEnums.PathHashAlgorithm> CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING = new Setting<>(
        "cluster.remote_store.index.path.hash_algorithm",
        RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString(),
        RemoteStoreEnums.PathHashAlgorithm::parseString,
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls the maximum referenced remote translog files. If breached the shard will be flushed.
     */
    public static final Setting<Integer> CLUSTER_REMOTE_MAX_TRANSLOG_READERS = Setting.intSetting(
        "cluster.remote_store.translog.max_readers",
        1000,
        -1,
        v -> {
            if (v != -1 && v < MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS) {
                throw new IllegalArgumentException("Cannot set value lower than " + MIN_CLUSTER_REMOTE_MAX_TRANSLOG_READERS);
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Controls timeout value while uploading segment files to remote segment store
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.segment.transfer_timeout",
        TimeValue.timeValueMinutes(30),
        TimeValue.timeValueMinutes(10),
        Property.NodeScope,
        Property.Dynamic
    );

    /**
     * Controls pinned timestamp feature enablement
     */
    public static final Setting<Boolean> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED = Setting.boolSetting(
        "cluster.remote_store.pinned_timestamps.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Controls pinned timestamp scheduler interval
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_SCHEDULER_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.pinned_timestamps.scheduler_interval",
        TimeValue.timeValueMinutes(3),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope
    );

    /**
     * Controls allowed timestamp values to be pinned from past
     */
    public static final Setting<TimeValue> CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_LOOKBACK_INTERVAL = Setting.timeSetting(
        "cluster.remote_store.pinned_timestamps.lookback_interval",
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    /**
     * Controls the fixed prefix for the translog path on remote store.
     */
    public static final Setting<String> CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.translog.path.prefix",
        "",
        Property.NodeScope,
        Property.Final
    );

    /**
     * Controls the fixed prefix for the segments path on remote store.
     */
    public static final Setting<String> CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX = Setting.simpleString(
        "cluster.remote_store.segments.path.prefix",
        "",
        Property.NodeScope,
        Property.Final
    );

    private volatile TimeValue clusterRemoteTranslogBufferInterval;
    private volatile int minRemoteSegmentMetadataFiles;
    private volatile TimeValue clusterRemoteTranslogTransferTimeout;
    private volatile TimeValue clusterRemoteSegmentTransferTimeout;
    private volatile RemoteStoreEnums.PathType pathType;
    private volatile RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm;
    private volatile int maxRemoteTranslogReaders;
    private volatile boolean isTranslogMetadataEnabled;
    private static volatile boolean isPinnedTimestampsEnabled;
    private static volatile TimeValue pinnedTimestampsSchedulerInterval;
    private static volatile TimeValue pinnedTimestampsLookbackInterval;
    private final String translogPathFixedPrefix;
    private final String segmentsPathFixedPrefix;

    public RemoteStoreSettings(Settings settings, ClusterSettings clusterSettings) {
        clusterRemoteTranslogBufferInterval = CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
            this::setClusterRemoteTranslogBufferInterval
        );

        minRemoteSegmentMetadataFiles = CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING,
            this::setMinRemoteSegmentMetadataFiles
        );

        clusterRemoteTranslogTransferTimeout = CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_TRANSLOG_TRANSFER_TIMEOUT_SETTING,
            this::setClusterRemoteTranslogTransferTimeout
        );

        pathType = clusterSettings.get(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING, this::setPathType);

        isTranslogMetadataEnabled = clusterSettings.get(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_TRANSLOG_METADATA, this::setTranslogMetadataEnabled);

        pathHashAlgorithm = clusterSettings.get(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_STORE_PATH_HASH_ALGORITHM_SETTING, this::setPathHashAlgorithm);

        maxRemoteTranslogReaders = CLUSTER_REMOTE_MAX_TRANSLOG_READERS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_REMOTE_MAX_TRANSLOG_READERS, this::setMaxRemoteTranslogReaders);

        clusterRemoteSegmentTransferTimeout = CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_REMOTE_SEGMENT_TRANSFER_TIMEOUT_SETTING,
            this::setClusterRemoteSegmentTransferTimeout
        );

        pinnedTimestampsSchedulerInterval = CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_SCHEDULER_INTERVAL.get(settings);
        pinnedTimestampsLookbackInterval = CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_LOOKBACK_INTERVAL.get(settings);
        isPinnedTimestampsEnabled = CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.get(settings);

        translogPathFixedPrefix = CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(settings);
        segmentsPathFixedPrefix = CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(settings);
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

    public TimeValue getClusterRemoteSegmentTransferTimeout() {
        return clusterRemoteSegmentTransferTimeout;
    }

    private void setClusterRemoteTranslogTransferTimeout(TimeValue clusterRemoteTranslogTransferTimeout) {
        this.clusterRemoteTranslogTransferTimeout = clusterRemoteTranslogTransferTimeout;
    }

    private void setClusterRemoteSegmentTransferTimeout(TimeValue clusterRemoteSegmentTransferTimeout) {
        this.clusterRemoteSegmentTransferTimeout = clusterRemoteSegmentTransferTimeout;
    }

    @ExperimentalApi
    public RemoteStoreEnums.PathType getPathType() {
        return pathType;
    }

    @ExperimentalApi
    public RemoteStoreEnums.PathHashAlgorithm getPathHashAlgorithm() {
        return pathHashAlgorithm;
    }

    private void setPathType(RemoteStoreEnums.PathType pathType) {
        this.pathType = pathType;
    }

    private void setTranslogMetadataEnabled(boolean isTranslogMetadataEnabled) {
        this.isTranslogMetadataEnabled = isTranslogMetadataEnabled;
    }

    public boolean isTranslogMetadataEnabled() {
        return isTranslogMetadataEnabled;
    }

    private void setPathHashAlgorithm(RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm) {
        this.pathHashAlgorithm = pathHashAlgorithm;
    }

    public int getMaxRemoteTranslogReaders() {
        return maxRemoteTranslogReaders;
    }

    private void setMaxRemoteTranslogReaders(int maxRemoteTranslogReaders) {
        this.maxRemoteTranslogReaders = maxRemoteTranslogReaders;
    }

    public static TimeValue getPinnedTimestampsSchedulerInterval() {
        return pinnedTimestampsSchedulerInterval;
    }

    public static TimeValue getPinnedTimestampsLookbackInterval() {
        return pinnedTimestampsLookbackInterval;
    }

    // Visible for testing
    public static void setPinnedTimestampsLookbackInterval(TimeValue pinnedTimestampsLookbackInterval) {
        RemoteStoreSettings.pinnedTimestampsLookbackInterval = pinnedTimestampsLookbackInterval;
    }

    public static boolean isPinnedTimestampsEnabled() {
        return isPinnedTimestampsEnabled;
    }

    public String getTranslogPathFixedPrefix() {
        return translogPathFixedPrefix;
    }

    public String getSegmentsPathFixedPrefix() {
        return segmentsPathFixedPrefix;
    }
}
