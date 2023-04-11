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

    public static final Setting<Boolean> REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED = Setting.boolSetting(
        "remote_store.segment_upload.pressure.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> MIN_SEQ_NO_LAG_LIMIT = Setting.longSetting(
        "remote_store.segment_upload.pressure.seq_no.lag.limit",
        5L,
        2L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> BYTES_BEHIND_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "remote_store.segment_upload.pressure.bytes_behind.variance",
        2.0,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> TIME_BEHIND_VARIANCE_THRESHOLD = Setting.doubleSetting(
        "remote_store.segment_upload.pressure.time_behind.variance",
        2.0,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MIN_CONSECUTIVE_FAILURES_LIMIT = Setting.intSetting(
        "remote_store.segment_upload.pressure.consecutive_failures.limit",
        10,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean remoteSegmentUploadPressureEnabled;

    private volatile long minSeqNoLagLimit;

    private volatile double bytesBehindVarianceThreshold;

    private volatile double timeBehindVarianceThreshold;

    private volatile int minConsecutiveFailuresLimit;

    public RemoteUploadPressureSettings(ClusterService clusterService, Settings settings) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.remoteSegmentUploadPressureEnabled = REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED, this::setRemoteSegmentUploadPressureEnabled);

        this.minSeqNoLagLimit = MIN_SEQ_NO_LAG_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_SEQ_NO_LAG_LIMIT, this::setMinSeqNoLagLimit);

        this.bytesBehindVarianceThreshold = BYTES_BEHIND_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(BYTES_BEHIND_VARIANCE_THRESHOLD, this::setBytesBehindVarianceThreshold);

        this.timeBehindVarianceThreshold = TIME_BEHIND_VARIANCE_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(TIME_BEHIND_VARIANCE_THRESHOLD, this::setTimeBehindVarianceThreshold);

        this.minConsecutiveFailuresLimit = MIN_CONSECUTIVE_FAILURES_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_CONSECUTIVE_FAILURES_LIMIT, this::setMinConsecutiveFailuresLimit);
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

    public double getBytesBehindVarianceThreshold() {
        return bytesBehindVarianceThreshold;
    }

    public void setBytesBehindVarianceThreshold(double bytesBehindVarianceThreshold) {
        this.bytesBehindVarianceThreshold = bytesBehindVarianceThreshold;
    }

    public double getTimeBehindVarianceThreshold() {
        return timeBehindVarianceThreshold;
    }

    public void setTimeBehindVarianceThreshold(double timeBehindVarianceThreshold) {
        this.timeBehindVarianceThreshold = timeBehindVarianceThreshold;
    }

    public int getMinConsecutiveFailuresLimit() {
        return minConsecutiveFailuresLimit;
    }

    public void setMinConsecutiveFailuresLimit(int minConsecutiveFailuresLimit) {
        this.minConsecutiveFailuresLimit = minConsecutiveFailuresLimit;
    }
}
