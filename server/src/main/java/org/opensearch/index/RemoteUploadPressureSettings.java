/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Remote upload pressure settings.
 */
public class RemoteUploadPressureSettings {

    private static class Defaults {
        private static final long MIN_SEQ_NO_LAG_LIMIT = 5;
        private static final double BYTES_LAG_VARIANCE_THRESHOLD = 2.0;
        private static final double TIME_LAG_VARIANCE_THRESHOLD = 2.0;
        private static final int MIN_CONSECUTIVE_FAILURES_LIMIT = 10;
        private static final int UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int UPLOAD_BYTES_PER_SECOND_MOVING_AVERAGE_WINDOW_SIZE = 20;
        private static final int UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE = 20;
    }

    public static final Setting<Boolean> REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED = Setting.boolSetting(
        "remote_store.segment_upload.pressure.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> MIN_SEQ_NO_LAG_LIMIT = Setting.longSetting(
        "remote_store.segment_upload.pressure.seq_no_lag.limit",
        Defaults.MIN_SEQ_NO_LAG_LIMIT,
        2L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> BYTES_LAG_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "remote_store.segment_upload.pressure.bytes_lag.variance",
        Defaults.BYTES_LAG_VARIANCE_THRESHOLD,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> TIME_LAG_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "remote_store.segment_upload.pressure.time_lag.variance",
        Defaults.TIME_LAG_VARIANCE_THRESHOLD,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MIN_CONSECUTIVE_FAILURES_LIMIT = Setting.intSetting(
        "remote_store.segment_upload.pressure.consecutive_failures.limit",
        Defaults.MIN_CONSECUTIVE_FAILURES_LIMIT,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.segment_upload.pressure.upload_bytes_moving_average_window_size",
        Defaults.UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> UPLOAD_BYTES_PER_SECOND_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.segment_upload.pressure.upload_bytes_per_sec_moving_average_window_size",
        Defaults.UPLOAD_BYTES_PER_SECOND_MOVING_AVERAGE_WINDOW_SIZE,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.segment_upload.pressure.upload_time_moving_average_window_size",
        Defaults.UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean remoteSegmentUploadPressureEnabled;

    private volatile long minSeqNoLagLimit;

    private volatile double bytesLagVarianceThreshold;

    private volatile double timeLagVarianceThreshold;

    private volatile int minConsecutiveFailuresLimit;

    private volatile int uploadBytesMovingAverageWindowSize;

    private volatile int uploadBytesPerSecMovingAverageWindowSize;

    private volatile int uploadTimeMovingAverageWindowSize;

    public RemoteUploadPressureSettings(
        ClusterService clusterService,
        Settings settings,
        RemoteUploadPressureService remoteUploadPressureService
    ) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.remoteSegmentUploadPressureEnabled = REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED, this::setRemoteSegmentUploadPressureEnabled);

        this.minSeqNoLagLimit = MIN_SEQ_NO_LAG_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_SEQ_NO_LAG_LIMIT, this::setMinSeqNoLagLimit);

        this.bytesLagVarianceThreshold = BYTES_LAG_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(BYTES_LAG_VARIANCE_THRESHOLD, this::setBytesLagVarianceThreshold);

        this.timeLagVarianceThreshold = TIME_LAG_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(TIME_LAG_VARIANCE_THRESHOLD, this::setTimeLagVarianceThreshold);

        this.minConsecutiveFailuresLimit = MIN_CONSECUTIVE_FAILURES_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_CONSECUTIVE_FAILURES_LIMIT, this::setMinConsecutiveFailuresLimit);

        this.uploadBytesMovingAverageWindowSize = UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE,
            remoteUploadPressureService::updateUploadBytesMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(UPLOAD_BYTES_MOVING_AVERAGE_WINDOW_SIZE, this::setUploadBytesMovingAverageWindowSize);

        this.uploadBytesPerSecMovingAverageWindowSize = UPLOAD_BYTES_PER_SECOND_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_BYTES_PER_SECOND_MOVING_AVERAGE_WINDOW_SIZE,
            remoteUploadPressureService::updateUploadBytesPerSecMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_BYTES_PER_SECOND_MOVING_AVERAGE_WINDOW_SIZE,
            this::setUploadBytesPerSecMovingAverageWindowSize
        );

        this.uploadTimeMovingAverageWindowSize = UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE,
            remoteUploadPressureService::updateUploadTimeMovingAverageWindowSize
        );
        clusterSettings.addSettingsUpdateConsumer(UPLOAD_TIME_MOVING_AVERAGE_WINDOW_SIZE, this::setUploadTimeMovingAverageWindowSize);
    }

    public boolean isRemoteSegmentUploadPressureEnabled() {
        return remoteSegmentUploadPressureEnabled;
    }

    public void setRemoteSegmentUploadPressureEnabled(boolean remoteSegmentUploadPressureEnabled) {
        this.remoteSegmentUploadPressureEnabled = remoteSegmentUploadPressureEnabled;
    }

    public long getMinSeqNoLagLimit() {
        return minSeqNoLagLimit;
    }

    public void setMinSeqNoLagLimit(long minSeqNoLagLimit) {
        this.minSeqNoLagLimit = minSeqNoLagLimit;
    }

    public double getBytesLagVarianceThreshold() {
        return bytesLagVarianceThreshold;
    }

    public void setBytesLagVarianceThreshold(double bytesLagVarianceThreshold) {
        this.bytesLagVarianceThreshold = bytesLagVarianceThreshold;
    }

    public double getTimeLagVarianceThreshold() {
        return timeLagVarianceThreshold;
    }

    public void setTimeLagVarianceThreshold(double timeLagVarianceThreshold) {
        this.timeLagVarianceThreshold = timeLagVarianceThreshold;
    }

    public int getMinConsecutiveFailuresLimit() {
        return minConsecutiveFailuresLimit;
    }

    public void setMinConsecutiveFailuresLimit(int minConsecutiveFailuresLimit) {
        this.minConsecutiveFailuresLimit = minConsecutiveFailuresLimit;
    }

    public int getUploadBytesMovingAverageWindowSize() {
        return uploadBytesMovingAverageWindowSize;
    }

    public void setUploadBytesMovingAverageWindowSize(int uploadBytesMovingAverageWindowSize) {
        this.uploadBytesMovingAverageWindowSize = uploadBytesMovingAverageWindowSize;
    }

    public int getUploadBytesPerSecMovingAverageWindowSize() {
        return uploadBytesPerSecMovingAverageWindowSize;
    }

    public void setUploadBytesPerSecMovingAverageWindowSize(int uploadBytesPerSecMovingAverageWindowSize) {
        this.uploadBytesPerSecMovingAverageWindowSize = uploadBytesPerSecMovingAverageWindowSize;
    }

    public int getUploadTimeMovingAverageWindowSize() {
        return uploadTimeMovingAverageWindowSize;
    }

    public void setUploadTimeMovingAverageWindowSize(int uploadTimeMovingAverageWindowSize) {
        this.uploadTimeMovingAverageWindowSize = uploadTimeMovingAverageWindowSize;
    }
}
