/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.junit.After;
import org.junit.Before;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

/**
 * Base class for running the tests with parameterization of the dynamic settings
 * For any class that wants to use parameterization, use @ParametersFactory to generate
 * different params only for dynamic settings. Refer SearchCancellationIT for an example.
 * Note: this doesn't work for the parameterization of feature flag/static settings.
 */
public abstract class ParameterizedOpenSearchIntegTestCase extends OpenSearchIntegTestCase {

    private final Settings dynamicSettings;

    public ParameterizedOpenSearchIntegTestCase(Settings dynamicSettings) {
        this.dynamicSettings = dynamicSettings;
    }

    @Before
    public void beforeTests() {
        SettingsModule settingsModule = new SettingsModule(dynamicSettings);
        for (String key : dynamicSettings.keySet()) {
            assertTrue(
                settingsModule.getClusterSettings().isDynamicSetting(key) || settingsModule.getIndexScopedSettings().isDynamicSetting(key)
            );
        }
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(dynamicSettings).get();
    }

    @After
    public void afterTests() {
        final Settings.Builder settingsToUnset = Settings.builder();
        dynamicSettings.keySet().forEach(settingsToUnset::putNull);
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settingsToUnset).get();
    }

    // This method shouldn't be called in setupSuiteScopeCluster(). Only call this method inside single test.
    public void indexRandomForConcurrentSearch(String... indices) throws InterruptedException {
        if (dynamicSettings.get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey()).equals("true")) {
            indexRandomForMultipleSlices(indices);
        }
    }
}
