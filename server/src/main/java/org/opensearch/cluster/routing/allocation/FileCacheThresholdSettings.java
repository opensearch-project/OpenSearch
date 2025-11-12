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
import org.opensearch.core.common.unit.ByteSizeValue;

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

    public static final Setting<Boolean> CLUSTER_FILECACHE_ACTIVEUSAGE_THRESHOLD_ENABLED_SETTING = Setting.boolSetting(
        "cluster.filecache.activeusage.threshold.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING = new Setting<>(
        "cluster.filecache.activeusage.indexing.threshold",
        "90%",
        (s) -> validThresholdSetting(s, "cluster.filecache.activeusage.indexing.threshold"),
        new IndexThresholdBreachValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING = new Setting<>(
        "cluster.filecache.activeusage.search.threshold",
        "100%",
        (s) -> validThresholdSetting(s, "cluster.filecache.activeusage.search.threshold"),
        new SearchThresholdBreachValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile Double fileCacheIndexThresholdPercentage;
    private volatile Double fileCacheSearchThresholdPercentage;
    private volatile ByteSizeValue fileCacheIndexThresholdBytes;
    private volatile ByteSizeValue fileCacheSearchThresholdBytes;
    private volatile boolean enabled;

    public FileCacheThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        final String index = CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.get(settings);
        final String searchStage = CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.get(settings);
        final boolean enabled = CLUSTER_FILECACHE_ACTIVEUSAGE_THRESHOLD_ENABLED_SETTING.get(settings);
        setIndexThreshold(index);
        setSearchThreshold(searchStage);
        setEnabled(enabled);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_FILECACHE_ACTIVEUSAGE_THRESHOLD_ENABLED_SETTING, this::setEnabled);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING, this::setIndexThreshold);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING, this::setSearchThreshold);
    }

    /**
     * Validates a index file cache threshold.
     *
     * @opensearch.internal
     */
    static final class IndexThresholdBreachValidator implements Setting.Validator<String> {
        @Override
        public void validate(final String value) {}

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String searchThreshold = (String) settings.get(CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING);
            doValidate(value, searchThreshold);

        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING);
            return settings.iterator();
        }
    }

    /**
     * Validates the search file cache threshold.
     *
     * @opensearch.internal
     */
    static final class SearchThresholdBreachValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {}

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String indexThreshold = (String) settings.get(CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING);
            doValidate(indexThreshold, value);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING);
            return settings.iterator();
        }
    }

    private static void doValidate(String index, String search) {
        try {
            doValidateAsPercentage(index, search);
            return;
        } catch (final OpenSearchParseException e) {
            // swallow as we are now going to try to parse as bytes
        }
        try {
            doValidateAsBytes(index, search);
        } catch (final OpenSearchParseException e) {
            final String message = String.format(
                Locale.ROOT,
                "unable to consistently parse [%s=%s], [%s=%s], and [%s=%s] as percentage or bytes",
                CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey(),
                index,
                CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey(),
                search
            );
            throw new IllegalArgumentException(message, e);
        }
    }

    private static void doValidateAsPercentage(final String index, final String search) {
        final double indexThreshold = thresholdPercentageFromValue(index, false);
        final double searchThreshold = thresholdPercentageFromValue(search, false);
        if (indexThreshold > searchThreshold) {
            throw new IllegalArgumentException(
                "index file cache threshold [" + index + "] more than search file cache threshold [" + search + "]"
            );
        }
    }

    private static void doValidateAsBytes(final String index, final String search) {
        final ByteSizeValue indexBytes = thresholdBytesFromValue(
            index,
            CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey(),
            false
        );
        final ByteSizeValue searchBytes = thresholdBytesFromValue(
            search,
            CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey(),
            false
        );
        if (indexBytes.getBytes() > searchBytes.getBytes()) {
            throw new IllegalArgumentException(
                "index file cache threshold [" + index + "] less than search file cache threshold [" + search + "]"
            );
        }
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    private static ByteSizeValue thresholdBytesFromSettings(String value, String settingName) {
        return thresholdBytesFromSettings(value, settingName, true);
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning zero bytes if it can not be parsed and the specified lenient
     * parameter is true, otherwise throwing an {@link OpenSearchParseException}.
     *
     * @param value       the watermark to parse as a byte size
     * @param settingName the name of the setting
     * @param lenient     true if lenient parsing should be applied
     * @return the parsed byte size value
     */
    private static ByteSizeValue thresholdBytesFromSettings(String value, String settingName, boolean lenient) {
        try {
            return ByteSizeValue.parseBytesSizeValue(value, settingName);
        } catch (OpenSearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return ByteSizeValue.parseBytesSizeValue("0b", settingName);
            }
            throw ex;
        }
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setIndexThreshold(String index) {
        this.fileCacheIndexThresholdPercentage = thresholdPercentageFromValue(index);
        this.fileCacheIndexThresholdBytes = thresholdBytesFromSettings(
            index,
            CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey()
        );
    }

    private void setSearchThreshold(String search) {
        this.fileCacheSearchThresholdPercentage = thresholdPercentageFromValue(search);
        this.fileCacheSearchThresholdBytes = thresholdBytesFromSettings(
            search,
            CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey()
        );
    }

    public Boolean isEnabled() {
        return enabled;
    }

    public Double getFileCacheIndexThresholdPercentage() {
        return fileCacheIndexThresholdPercentage;
    }

    public Double getFileCacheSearchThresholdPercentage() {
        return fileCacheSearchThresholdPercentage;
    }

    public ByteSizeValue getFileCacheIndexThresholdBytes() {
        return fileCacheIndexThresholdBytes;
    }

    public ByteSizeValue getFileCacheSearchThresholdBytes() {
        return fileCacheSearchThresholdBytes;
    }

    String describeIndexThreshold() {
        return fileCacheIndexThresholdBytes.equals(ByteSizeValue.ZERO)
            ? fileCacheIndexThresholdPercentage + "%"
            : fileCacheIndexThresholdBytes.toString();
    }

    String describeSearchThreshold() {
        return fileCacheSearchThresholdBytes.equals(ByteSizeValue.ZERO)
            ? fileCacheSearchThresholdPercentage + "%"
            : fileCacheSearchThresholdBytes.toString();
    }

    /**
     * Attempts to parse the into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    private static double thresholdPercentageFromValue(String value) {
        return thresholdPercentageFromValue(value, true);
    }

    /**
     * Attempts to parse the into a percentage, returning 100.0% if it can not be parsed and the specified lenient parameter is
     * true, otherwise throwing an {@link OpenSearchParseException}.
     *
     * @param value   the to parse as a percentage
     * @param lenient true if lenient parsing should be applied
     * @return the parsed percentage
     */
    private static double thresholdPercentageFromValue(String value, boolean lenient) {
        try {
            return RatioValue.parseRatioValue(value).getAsPercent();
        } catch (OpenSearchParseException ex) {
            if (lenient) {
                return 0.0;
            }
            throw ex;
        }
    }

    /**
     * Attempts to parse the into a {@link ByteSizeValue}, returning zero bytes if it can not be parsed and the specified lenient
     * parameter is true, otherwise throwing an {@link OpenSearchParseException}.
     *
     * @param value       the to parse as a byte size
     * @param settingName the name of the setting
     * @param lenient     true if lenient parsing should be applied
     * @return the parsed byte size value
     */
    private static ByteSizeValue thresholdBytesFromValue(String value, String settingName, boolean lenient) {
        try {
            return ByteSizeValue.parseBytesSizeValue(value, settingName);
        } catch (OpenSearchParseException ex) {
            if (lenient) {
                return ByteSizeValue.parseBytesSizeValue("0b", settingName);
            }
            throw ex;
        }
    }

    /**
     * Checks if a string is a valid percentage or byte size value,
     *
     * @return the value given
     */
    private static String validThresholdSetting(String value, String settingName) {
        try {
            RatioValue.parseRatioValue(value);
        } catch (OpenSearchParseException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(value, settingName);
            } catch (OpenSearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
        return value;
    }
}
