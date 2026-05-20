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

/**
 * Base class for running the tests with parameterization using dynamic settings: the cluster will be created once before the test suite and the
 * settings will be applied dynamically, please notice that not all settings could be changed dynamically (consider using {@link ParameterizedStaticSettingsOpenSearchIntegTestCase}
 * instead).
 * <p>
 * Here is the simple illustration on of the execution flow per parameters combination:
 * <ul>
 *   <li><b>suite scope</b>: create cluster -&gt; for each test method { apply settings -&gt; run test method -&gt; unapply settings } -&gt; shutdown cluster</li>
 *   <li><b>test scope</b>: for each test method { create cluster -&gt;  apply settings -&gt; run test method -&gt; unapply settings -&gt; shutdown cluster }</li>
 * </ul>
 */
public abstract class ParameterizedDynamicSettingsOpenSearchIntegTestCase extends ParameterizedOpenSearchIntegTestCase {
    public ParameterizedDynamicSettingsOpenSearchIntegTestCase(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @Before
    public void beforeTests() {
        SettingsModule settingsModule = new SettingsModule(settings);
        for (String key : settings.keySet()) {
            assertTrue(
                settingsModule.getClusterSettings().isDynamicSetting(key) || settingsModule.getIndexScopedSettings().isDynamicSetting(key)
            );
        }
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
    }

    @After
    public void afterTests() {
        final Settings.Builder settingsToUnset = Settings.builder();
        settings.keySet().forEach(settingsToUnset::putNull);
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settingsToUnset).get();
    }

    @Override
    boolean hasSameParametersAs(ParameterizedOpenSearchIntegTestCase obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        return true;
    }
}
