/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Keeps track of all the search related node level settings which can be accessed via static methods
 */
public class SearchBootstrapSettings {
    // settings to configure maximum slice created per search request using OS custom slice computation mechanism. Default lucene
    // mechanism will not be used if this setting is set with value > 0
    public static final String CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_KEY = "search.concurrent.max_slice";
    public static final int CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_DEFAULT_VALUE = -1;

    // value <= 0 means lucene slice computation will be used
    public static final Setting<Integer> CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING = Setting.intSetting(
        CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_KEY,
        CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_DEFAULT_VALUE,
        Setting.Property.NodeScope
    );
    private static Settings settings;

    public static void initialize(Settings openSearchSettings) {
        settings = openSearchSettings;
    }

    public static int getValueAsInt(String settingName, int defaultValue) {
        return (settings != null) ? settings.getAsInt(settingName, defaultValue) : defaultValue;
    }
}
