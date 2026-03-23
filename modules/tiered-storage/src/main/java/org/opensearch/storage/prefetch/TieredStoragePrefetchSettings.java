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
 * Settings for tiered storage prefetch behavior.
 */
public class TieredStoragePrefetchSettings {

    /** Default number of blocks to read ahead during prefetch. */
    public static final int DEFAULT_READ_AHEAD_BLOCK_COUNT = 4;
    /** File suffix for DVD format files. */
    public static final String DVD_FILE_SUFFIX = "dvd";
    /** File suffix for CFS format files. */
    public static final String CFS_FILE_SUFFIX = "cfs";
    /** Setting for the number of blocks to read ahead. */
    public static final Setting<Integer> READ_AHEAD_BLOCK_COUNT = Setting.intSetting(
        "tiering.service.prefetch.read_ahead.block_count",
        DEFAULT_READ_AHEAD_BLOCK_COUNT,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** Setting to enable or disable stored fields prefetch. */
    public static final Setting<Boolean> STORED_FIELDS_PREFETCH_ENABLED_SETTING = Setting.boolSetting(
        "tiering.service.prefetch.stored_fields.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /** List of file formats for which read-ahead is enabled. */
    public static final List<String> READ_AHEAD_ENABLE_FILE_FORMATS = List.of(DVD_FILE_SUFFIX);
    private int readAheadBlockCount;
    private final List<String> readAheadEnableFileFormats;
    private boolean storedFieldsPrefetchEnabled;

    /**
     * Creates a new TieredStoragePrefetchSettings instance.
     * @param clusterSettings the cluster settings to read prefetch configuration from
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
     * @param storedFieldsPrefetchEnabled true to enable, false to disable
     */
    public void setStoredFieldsPrefetchEnabled(boolean storedFieldsPrefetchEnabled) {
        this.storedFieldsPrefetchEnabled = storedFieldsPrefetchEnabled;
    }

    /**
     * Returns whether stored fields prefetch is enabled.
     * @return true if enabled
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
     * Returns the list of file formats for which read-ahead is enabled.
     * @return list of file format suffixes
     */
    public List<String> getReadAheadEnableFileFormats() {
        return this.readAheadEnableFileFormats;
    }
}
