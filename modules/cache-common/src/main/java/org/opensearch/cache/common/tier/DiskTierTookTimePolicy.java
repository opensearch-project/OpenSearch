/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.CachePolicyInfoWrapper;
import org.opensearch.common.cache.CacheTierPolicy;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;

import java.util.function.Function;

/**
 * A cache tier policy which accepts queries whose took time is greater than some threshold,
 * which is specified as a dynamic cluster-level setting. The threshold should be set to approximately
 * the time it takes to get a result from the cache tier.
 * The policy expects to be able to read a CachePolicyInfoWrapper from the start of the BytesReference.
 */
public class DiskTierTookTimePolicy implements CacheTierPolicy<BytesReference> {
    public static final Setting<TimeValue> DISK_TOOKTIME_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "indices.requests.cache.disk.tooktime.threshold",
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    ); // Set this to TimeValue.ZERO to let all data through

    private TimeValue threshold;
    private final Function<BytesReference, CachePolicyInfoWrapper> getPolicyInfoFn;

    public DiskTierTookTimePolicy(
        Settings settings,
        ClusterSettings clusterSettings,
        Function<BytesReference, CachePolicyInfoWrapper> getPolicyInfoFn
    ) {
        this.threshold = DISK_TOOKTIME_THRESHOLD_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(DISK_TOOKTIME_THRESHOLD_SETTING, this::setThreshold);
        this.getPolicyInfoFn = getPolicyInfoFn;
    }

    protected void setThreshold(TimeValue threshold) { // protected so that we can manually set value in unit test
        this.threshold = threshold;
    }

    @Override
    public boolean checkData(BytesReference data) {
        Long tookTimeNanos;
        try {
            tookTimeNanos = getPolicyInfoFn.apply(data).getTookTimeNanos();
        } catch (Exception e) {
            // If we can't read a CachePolicyInfoWrapper from the BytesReference, reject the data
            return false;
        }

        if (tookTimeNanos == null) {
            // If the wrapper contains null took time, reject the data
            // This can happen if no CachePolicyInfoWrapper was written to the BytesReference, as the wrapper's constructor
            // reads an optional long, which will end up as null in this case. This is why we should reject it.
            return false;
        }

        if (threshold.equals(TimeValue.ZERO)) {
            // If the policy is set to zero, admit any well-formed data
            return true;
        }
        TimeValue tookTime = TimeValue.timeValueNanos(tookTimeNanos);
        if (tookTime.compareTo(threshold) < 0) { // negative -> tookTime is shorter than threshold
            return false;
        }
        return true;
    }
}
