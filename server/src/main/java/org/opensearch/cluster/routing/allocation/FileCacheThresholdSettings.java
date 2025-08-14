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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A container to keep settings for file cache thresholds up to date with cluster setting changes.
 *
 * @opensearch.internal
 */
public class FileCacheThresholdSettings {

    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.filecache.watermark.high",
        "90%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.filecache.watermark.high"),
        new HighDiskWatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.filecache.watermark.flood_stage",
        "100%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.filecache.watermark.flood_stage"),
        new FloodStageValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile Double freeFileCacheThresholdHigh;
    private volatile ByteSizeValue freeBytesThresholdHigh;
    private volatile Double freeFileCacheThresholdFloodStage;
    private volatile ByteSizeValue freeBytesThresholdFloodStage;

    public FileCacheThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        final String highWatermark = CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING.get(settings);
        final String floodStage = CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING.get(settings);
        setHighWatermark(highWatermark);
        setFloodStage(floodStage);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING, this::setHighWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING, this::setFloodStage);
    }

    /**
     * Validates a high file cache watermark.
     *
     * @opensearch.internal
     */
    static final class HighDiskWatermarkValidator implements Setting.Validator<String> {
        @Override
        public void validate(final String value) {}

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String floodStageRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING);
            doValidate(value, floodStageRaw);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING);
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
        public void validate(final String value) {}

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String highWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING);
            doValidate(highWatermarkRaw, value);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Arrays.asList(CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING);
            return settings.iterator();
        }
    }

    private static void doValidate(String high, String flood) {
        try {
            doValidateAsPercentage(high, flood);
            return; // early return so that we do not try to parse as bytes
        } catch (final OpenSearchParseException e) {
            // swallow as we are now going to try to parse as bytes
        }
        try {
            doValidateAsBytes(high, flood);
        } catch (final OpenSearchParseException e) {
            final String message = String.format(
                Locale.ROOT,
                "unable to consistently parse [%s=%s], [%s=%s], and [%s=%s] as percentage or bytes",
                CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING.getKey(),
                high,
                CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                flood
            );
            throw new IllegalArgumentException(message, e);
        }
    }

    private static void doValidateAsPercentage(final String high, final String flood) {
        final double highWatermarkThreshold = thresholdPercentageFromWatermark(high, false);
        final double floodThreshold = thresholdPercentageFromWatermark(flood, false);
        if (highWatermarkThreshold > floodThreshold) {
            throw new IllegalArgumentException(
                "high file cache watermark [" + high + "] more than flood stage file cache watermark [" + flood + "]"
            );
        }
    }

    private static void doValidateAsBytes(final String high, final String flood) {
        final ByteSizeValue highWatermarkBytes = thresholdBytesFromWatermark(
            high,
            CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING.getKey(),
            false
        );
        final ByteSizeValue floodStageBytes = thresholdBytesFromWatermark(
            flood,
            CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
            false
        );
        if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) {
            throw new IllegalArgumentException(
                "high file cache watermark [" + high + "] less than flood stage file cache watermark [" + flood + "]"
            );
        }
    }

    private void setHighWatermark(String highWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeFileCacheThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(
            highWatermark,
            CLUSTER_ROUTING_ALLOCATION_HIGH_FILECACHE_WATERMARK_SETTING.getKey()
        );
    }

    private void setFloodStage(String floodStageRaw) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeFileCacheThresholdFloodStage = 100.0 - thresholdPercentageFromWatermark(floodStageRaw);
        this.freeBytesThresholdFloodStage = thresholdBytesFromWatermark(
            floodStageRaw,
            CLUSTER_ROUTING_ALLOCATION_FILECACHE_FLOOD_STAGE_WATERMARK_SETTING.getKey()
        );
    }

    public Double getFreeFileCacheThresholdHigh() {
        return freeFileCacheThresholdHigh;
    }

    public ByteSizeValue getFreeBytesThresholdHigh() {
        return freeBytesThresholdHigh;
    }

    public Double getFreeFileCacheThresholdFloodStage() {
        return freeFileCacheThresholdFloodStage;
    }

    public ByteSizeValue getFreeBytesThresholdFloodStage() {
        return freeBytesThresholdFloodStage;
    }

    String describeHighThreshold() {
        return freeBytesThresholdHigh.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeFileCacheThresholdHigh, "%")
            : freeBytesThresholdHigh.toString();
    }

    String describeFloodStageThreshold() {
        return freeBytesThresholdFloodStage.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeFileCacheThresholdFloodStage, "%")
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
