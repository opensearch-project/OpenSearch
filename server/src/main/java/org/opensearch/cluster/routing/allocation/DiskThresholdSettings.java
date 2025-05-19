/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A container to keep settings for disk thresholds up to date with cluster setting changes.
 *
 * @opensearch.internal
 */
public class DiskThresholdSettings {
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.disk.threshold_enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_WARM_DISK_THRESHOLD_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.disk.warm_threshold_enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> ENABLE_FOR_SINGLE_DATA_NODE = Setting.boolSetting(
        "cluster.routing.allocation.disk.watermark.enable_for_single_data_node",
        false,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.low",
        "85%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.low"),
        new LowDiskWatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.high",
        "90%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.high"),
        new HighDiskWatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage",
        "95%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.flood_stage"),
        new FloodStageValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.disk.include_relocations",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "cluster.routing.allocation.disk.reroute_interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE = Setting.boolSetting(
        "cluster.blocks.create_index.auto_release",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile String lowWatermarkRaw;
    private volatile String highWatermarkRaw;
    private volatile Double freeDiskThresholdLow;
    private volatile Double freeDiskThresholdHigh;
    private volatile ByteSizeValue freeBytesThresholdLow;
    private volatile ByteSizeValue freeBytesThresholdHigh;
    private volatile boolean includeRelocations;
    private volatile boolean createIndexBlockAutoReleaseEnabled;
    private volatile boolean enabled;
    private volatile boolean warmThresholdEnabled;
    private volatile TimeValue rerouteInterval;
    private volatile Double freeDiskThresholdFloodStage;
    private volatile ByteSizeValue freeBytesThresholdFloodStage;

    static {
        assert Version.CURRENT.major == Version.V_2_0_0.major + 1; // this check is unnecessary in v4
        final String AUTO_RELEASE_INDEX_ENABLED_KEY = "opensearch.disk.auto_release_flood_stage_block";

        final String property = System.getProperty(AUTO_RELEASE_INDEX_ENABLED_KEY);
        if (property != null) {
            throw new IllegalArgumentException(
                "system property [" + AUTO_RELEASE_INDEX_ENABLED_KEY + "] has been removed in 3.0.0 and is not supported anymore"
            );
        }
    }

    public DiskThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        final String lowWatermark = CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.get(settings);
        final String highWatermark = CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings);
        final String floodStage = CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.get(settings);
        setHighWatermark(highWatermark);
        setLowWatermark(lowWatermark);
        setFloodStage(floodStage);
        this.includeRelocations = CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING.get(settings);
        this.rerouteInterval = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(settings);
        this.enabled = CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        this.warmThresholdEnabled = CLUSTER_ROUTING_ALLOCATION_WARM_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        this.createIndexBlockAutoReleaseEnabled = CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, this::setLowWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, this::setHighWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING, this::setFloodStage);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING, this::setIncludeRelocations);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING, this::setRerouteInterval);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, this::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_WARM_DISK_THRESHOLD_ENABLED_SETTING,
            this::setWarmThresholdEnabled
        );
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE, this::setCreateIndexBlockAutoReleaseEnabled);
    }

    /**
     * Validates a low disk watermark.
     *
     * @opensearch.internal
     */
    static final class LowDiskWatermarkValidator implements Setting.Validator<String> {

        @Override
        public void validate(String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String highWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            final String floodStageRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            doValidate(value, highWatermarkRaw, floodStageRaw);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
            );
            return settings.iterator();
        }

    }

    /**
     * Validates a high disk watermark.
     *
     * @opensearch.internal
     */
    static final class HighDiskWatermarkValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String lowWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING);
            final String floodStageRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            doValidate(lowWatermarkRaw, value, floodStageRaw);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
            );
            return settings.iterator();
        }

    }

    /**
     * Validates the flood stage.
     *
     * @opensearch.internal
     */
    static final class FloodStageValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String lowWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING);
            final String highWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            doValidate(lowWatermarkRaw, highWatermarkRaw, value);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING
            );
            return settings.iterator();
        }

    }

    private static void doValidate(String low, String high, String flood) {
        try {
            doValidateAsPercentage(low, high, flood);
            return; // early return so that we do not try to parse as bytes
        } catch (final OpenSearchParseException e) {
            // swallow as we are now going to try to parse as bytes
        }
        try {
            doValidateAsBytes(low, high, flood);
        } catch (final OpenSearchParseException e) {
            final String message = String.format(
                Locale.ROOT,
                "unable to consistently parse [%s=%s], [%s=%s], and [%s=%s] as percentage or bytes",
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                low,
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                high,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                flood
            );
            throw new IllegalArgumentException(message, e);
        }
    }

    private static void doValidateAsPercentage(final String low, final String high, final String flood) {
        final double lowWatermarkThreshold = thresholdPercentageFromWatermark(low, false);
        final double highWatermarkThreshold = thresholdPercentageFromWatermark(high, false);
        final double floodThreshold = thresholdPercentageFromWatermark(flood, false);
        if (lowWatermarkThreshold > highWatermarkThreshold) {
            throw new IllegalArgumentException("low disk watermark [" + low + "] more than high disk watermark [" + high + "]");
        }
        if (highWatermarkThreshold > floodThreshold) {
            throw new IllegalArgumentException("high disk watermark [" + high + "] more than flood stage disk watermark [" + flood + "]");
        }
    }

    private static void doValidateAsBytes(final String low, final String high, final String flood) {
        final ByteSizeValue lowWatermarkBytes = thresholdBytesFromWatermark(
            low,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
            false
        );
        final ByteSizeValue highWatermarkBytes = thresholdBytesFromWatermark(
            high,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
            false
        );
        final ByteSizeValue floodStageBytes = thresholdBytesFromWatermark(
            flood,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
            false
        );
        if (lowWatermarkBytes.getBytes() < highWatermarkBytes.getBytes()) {
            throw new IllegalArgumentException("low disk watermark [" + low + "] less than high disk watermark [" + high + "]");
        }
        if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) {
            throw new IllegalArgumentException("high disk watermark [" + high + "] less than flood stage disk watermark [" + flood + "]");
        }
    }

    private void setIncludeRelocations(boolean includeRelocations) {
        this.includeRelocations = includeRelocations;
    }

    private void setRerouteInterval(TimeValue rerouteInterval) {
        this.rerouteInterval = rerouteInterval;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setWarmThresholdEnabled(boolean enabled) {
        this.warmThresholdEnabled = enabled;
    }

    private void setLowWatermark(String lowWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.lowWatermarkRaw = lowWatermark;
        this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(lowWatermark);
        this.freeBytesThresholdLow = thresholdBytesFromWatermark(
            lowWatermark,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()
        );
    }

    private void setHighWatermark(String highWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.highWatermarkRaw = highWatermark;
        this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(
            highWatermark,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey()
        );
    }

    private void setFloodStage(String floodStageRaw) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdFloodStage = 100.0 - thresholdPercentageFromWatermark(floodStageRaw);
        this.freeBytesThresholdFloodStage = thresholdBytesFromWatermark(
            floodStageRaw,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey()
        );
    }

    private void setCreateIndexBlockAutoReleaseEnabled(boolean createIndexBlockAutoReleaseEnabled) {
        this.createIndexBlockAutoReleaseEnabled = createIndexBlockAutoReleaseEnabled;
    }

    /**
     * Gets the raw (uninterpreted) low watermark value as found in the settings.
     */
    public String getLowWatermarkRaw() {
        return lowWatermarkRaw;
    }

    /**
     * Gets the raw (uninterpreted) high watermark value as found in the settings.
     */
    public String getHighWatermarkRaw() {
        return highWatermarkRaw;
    }

    public Double getFreeDiskThresholdLow() {
        return freeDiskThresholdLow;
    }

    public Double getFreeDiskThresholdHigh() {
        return freeDiskThresholdHigh;
    }

    public ByteSizeValue getFreeBytesThresholdLow() {
        return freeBytesThresholdLow;
    }

    public ByteSizeValue getFreeBytesThresholdHigh() {
        return freeBytesThresholdHigh;
    }

    public Double getFreeDiskThresholdFloodStage() {
        return freeDiskThresholdFloodStage;
    }

    public ByteSizeValue getFreeBytesThresholdFloodStage() {
        return freeBytesThresholdFloodStage;
    }

    public boolean includeRelocations() {
        return includeRelocations;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isWarmThresholdEnabled() {
        return warmThresholdEnabled;
    }

    public TimeValue getRerouteInterval() {
        return rerouteInterval;
    }

    public boolean isCreateIndexBlockAutoReleaseEnabled() {
        return createIndexBlockAutoReleaseEnabled;
    }

    String describeLowThreshold() {
        return freeBytesThresholdLow.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdLow, "%")
            : freeBytesThresholdLow.toString();
    }

    String describeHighThreshold() {
        return freeBytesThresholdHigh.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdHigh, "%")
            : freeBytesThresholdHigh.toString();
    }

    String describeFloodStageThreshold() {
        return freeBytesThresholdFloodStage.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdFloodStage, "%")
            : freeBytesThresholdFloodStage.toString();
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    private static double thresholdPercentageFromWatermark(String watermark) {
        return thresholdPercentageFromWatermark(watermark, true);
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if it can not be parsed and the specified lenient parameter is
     * true, otherwise throwing an {@link OpenSearchParseException}.
     *
     * @param watermark the watermark to parse as a percentage
     * @param lenient true if lenient parsing should be applied
     * @return the parsed percentage
     */
    private static double thresholdPercentageFromWatermark(String watermark, boolean lenient) {
        try {
            return RatioValue.parseRatioValue(watermark).getAsPercent();
        } catch (OpenSearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return 100.0;
            }
            throw ex;
        }
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    private static ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName) {
        return thresholdBytesFromWatermark(watermark, settingName, true);
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning zero bytes if it can not be parsed and the specified lenient
     * parameter is true, otherwise throwing an {@link OpenSearchParseException}.
     *
     * @param watermark the watermark to parse as a byte size
     * @param settingName the name of the setting
     * @param lenient true if lenient parsing should be applied
     * @return the parsed byte size value
     */
    private static ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName, boolean lenient) {
        try {
            return ByteSizeValue.parseBytesSizeValue(watermark, settingName);
        } catch (OpenSearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return ByteSizeValue.parseBytesSizeValue("0b", settingName);
            }
            throw ex;
        }
    }

    /**
     * Checks if a watermark string is a valid percentage or byte size value,
     * @return the watermark value given
     */
    private static String validWatermarkSetting(String watermark, String settingName) {
        try {
            RatioValue.parseRatioValue(watermark);
        } catch (OpenSearchParseException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(watermark, settingName);
            } catch (OpenSearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
        return watermark;
    }
}
