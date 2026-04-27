/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Settings for tiered storage prefetch behavior including read-ahead block count
 * and stored fields prefetch configuration.
 *
 * @opensearch.experimental
 */
public class TieredStoragePrefetchSettings {

    /** Default number of blocks to read ahead */
    public static final int DEFAULT_READ_AHEAD_BLOCK_COUNT = 4;
    /** Doc values data file suffix */
    public static final String DVD_FILE_SUFFIX = "dvd";
    /** Compound file suffix */
    public static final String CFS_FILE_SUFFIX = "cfs";

    /** Cluster setting for the number of blocks to read ahead during prefetch */
    public static final Setting<Integer> READ_AHEAD_BLOCK_COUNT = Setting.intSetting(
        "tiering.service.prefetch.read_ahead.block_count",
        DEFAULT_READ_AHEAD_BLOCK_COUNT,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** Cluster setting to enable or disable stored fields prefetch */
    public static final Setting<Boolean> STORED_FIELDS_PREFETCH_ENABLED_SETTING = Setting.boolSetting(
        "tiering.service.prefetch.stored_fields.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** File formats for which read-ahead is enabled */
    public static final List<String> READ_AHEAD_ENABLE_FILE_FORMATS = List.of(DVD_FILE_SUFFIX);
    private volatile int readAheadBlockCount;
    private final List<String> readAheadEnableFileFormats;
    private volatile boolean storedFieldsPrefetchEnabled;

    /**
     * Creates a new TieredStoragePrefetchSettings instance.
     * @param clusterSettings the cluster settings
     */
    public TieredStoragePrefetchSettings(ClusterSettings clusterSettings) {
        this.readAheadBlockCount = clusterSettings.get(READ_AHEAD_BLOCK_COUNT);
        clusterSettings.addSettingsUpdateConsumer(READ_AHEAD_BLOCK_COUNT, this::setReadAheadBlockCount);
        this.readAheadEnableFileFormats = READ_AHEAD_ENABLE_FILE_FORMATS;
        this.storedFieldsPrefetchEnabled = clusterSettings.get(STORED_FIELDS_PREFETCH_ENABLED_SETTING);
        clusterSettings.addSettingsUpdateConsumer(STORED_FIELDS_PREFETCH_ENABLED_SETTING, this::setStoredFieldsPrefetchEnabled);
    }

    /**
     * Sets the read-ahead block count.
     * @param readAheadBlockCount the number of blocks to read ahead
     */
    public void setReadAheadBlockCount(int readAheadBlockCount) {
        this.readAheadBlockCount = readAheadBlockCount;
    }

    /**
     * Sets whether stored fields prefetch is enabled.
     * @param storedFieldsPrefetchEnabled true to enable stored fields prefetch
     */
    public void setStoredFieldsPrefetchEnabled(boolean storedFieldsPrefetchEnabled) {
        this.storedFieldsPrefetchEnabled = storedFieldsPrefetchEnabled;
    }

    /**
     * Returns whether stored fields prefetch is enabled.
     * @return true if stored fields prefetch is enabled
     */
    public boolean isStoredFieldsPrefetchEnabled() {
        return storedFieldsPrefetchEnabled;
    }

    /**
     * Returns the read-ahead block count.
     * @return the number of blocks to read ahead
     */
    public int getReadAheadBlockCount() {
        return this.readAheadBlockCount;
    }

    /**
     * Returns the file formats for which read-ahead is enabled.
     * @return the list of file format suffixes
     */
    public List<String> getReadAheadEnableFileFormats() {
        return this.readAheadEnableFileFormats;
    }
}
