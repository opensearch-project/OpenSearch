/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class SearchBackpressureSettingsTests extends OpenSearchTestCase {

    /**
     * Validate proper construction of SearchBackpressureSettings object with a valid mode.
     */
    public void testSearchBackpressureSettings() {
        Settings settings = Settings.builder().put("search_backpressure.mode", "monitor_only").build();
        ClusterSettings cs = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchBackpressureSettings sbs = new SearchBackpressureSettings(settings, cs);
        assertEquals(SearchBackpressureMode.MONITOR_ONLY, sbs.getMode());
        assertEquals(settings, sbs.getSettings());
        assertEquals(cs, sbs.getClusterSettings());
    }

    /**
     * Validate construction of SearchBackpressureSettings object gets rejected
     * on invalid SearchBackpressureMode value.
     */
    public void testSearchBackpressureSettingValidateInvalidMode() {
        Settings settings = Settings.builder().put("search_backpressure.mode", "foo").build();
        assertThrows(
            IllegalArgumentException.class,
            () -> new SearchBackpressureSettings(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }
}
