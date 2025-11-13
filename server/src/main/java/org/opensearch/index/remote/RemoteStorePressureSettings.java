/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Settings related to back pressure on account of segments upload failures / lags.
 *
 * @opensearch.internal
 */
public class RemoteStorePressureSettings {

    static class Defaults {
        private static final double BYTES_LAG_VARIANCE_FACTOR = 10.0;
        private static final double UPLOAD_TIME_LAG_VARIANCE_FACTOR = 10.0;
        private static final double VARIANCE_FACTOR_MIN_VALUE = 1.0;
        private static final int MIN_CONSECUTIVE_FAILURES_LIMIT = 5;
        private static final int MIN_CONSECUTIVE_FAILURES_LIMIT_MIN_VALUE = 1;
    }

    public static final Setting<Boolean> REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED = Setting.boolSetting(
        "remote_store.segment.pressure.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> BYTES_LAG_VARIANCE_FACTOR = Setting.doubleSetting(
        "remote_store.segment.pressure.bytes_lag.variance_factor",
        Defaults.BYTES_LAG_VARIANCE_FACTOR,
        Defaults.VARIANCE_FACTOR_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> UPLOAD_TIME_LAG_VARIANCE_FACTOR = Setting.doubleSetting(
        "remote_store.segment.pressure.time_lag.variance_factor",
        Defaults.UPLOAD_TIME_LAG_VARIANCE_FACTOR,
        Defaults.VARIANCE_FACTOR_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MIN_CONSECUTIVE_FAILURES_LIMIT = Setting.intSetting(
        "remote_store.segment.pressure.consecutive_failures.limit",
        Defaults.MIN_CONSECUTIVE_FAILURES_LIMIT,
        Defaults.MIN_CONSECUTIVE_FAILURES_LIMIT_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean remoteRefreshSegmentPressureEnabled;

    private volatile long minRefreshSeqNoLagLimit;

    private volatile double bytesLagVarianceFactor;

    private volatile double uploadTimeLagVarianceFactor;

    private volatile int minConsecutiveFailuresLimit;

    public RemoteStorePressureSettings(
        ClusterService clusterService,
        Settings settings,
        RemoteStorePressureService remoteStorePressureService
    ) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.remoteRefreshSegmentPressureEnabled = REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED, this::setRemoteRefreshSegmentPressureEnabled);

        this.bytesLagVarianceFactor = BYTES_LAG_VARIANCE_FACTOR.get(settings);
        clusterSettings.addSettingsUpdateConsumer(BYTES_LAG_VARIANCE_FACTOR, this::setBytesLagVarianceFactor);

        this.uploadTimeLagVarianceFactor = UPLOAD_TIME_LAG_VARIANCE_FACTOR.get(settings);
        clusterSettings.addSettingsUpdateConsumer(UPLOAD_TIME_LAG_VARIANCE_FACTOR, this::setUploadTimeLagVarianceFactor);

        this.minConsecutiveFailuresLimit = MIN_CONSECUTIVE_FAILURES_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_CONSECUTIVE_FAILURES_LIMIT, this::setMinConsecutiveFailuresLimit);
    }

    boolean isRemoteRefreshSegmentPressureEnabled() {
        return remoteRefreshSegmentPressureEnabled;
    }

    private void setRemoteRefreshSegmentPressureEnabled(boolean remoteRefreshSegmentPressureEnabled) {
        this.remoteRefreshSegmentPressureEnabled = remoteRefreshSegmentPressureEnabled;
    }

    long getMinRefreshSeqNoLagLimit() {
        return minRefreshSeqNoLagLimit;
    }

    double getBytesLagVarianceFactor() {
        return bytesLagVarianceFactor;
    }

    private void setBytesLagVarianceFactor(double bytesLagVarianceFactor) {
        this.bytesLagVarianceFactor = bytesLagVarianceFactor;
    }

    double getUploadTimeLagVarianceFactor() {
        return uploadTimeLagVarianceFactor;
    }

    private void setUploadTimeLagVarianceFactor(double uploadTimeLagVarianceFactor) {
        this.uploadTimeLagVarianceFactor = uploadTimeLagVarianceFactor;
    }

    int getMinConsecutiveFailuresLimit() {
        return minConsecutiveFailuresLimit;
    }

    private void setMinConsecutiveFailuresLimit(int minConsecutiveFailuresLimit) {
        this.minConsecutiveFailuresLimit = minConsecutiveFailuresLimit;
    }
}
