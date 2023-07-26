/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.settings;

import org.junit.Assert;
import org.junit.Before;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.correlation.EventsCorrelationPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for Correlation Engine settings
 */
public class EventsCorrelationSettingsTests extends OpenSearchTestCase {

    private EventsCorrelationPlugin plugin;

    @Before
    public void setup() {
        plugin = new EventsCorrelationPlugin();
    }

    /**
     * test all plugin settings returned
     */
    public void testAllPluginSettingsReturned() {
        List<Object> expectedSettings = List.of(
            EventsCorrelationSettings.IS_CORRELATION_INDEX_SETTING,
            EventsCorrelationSettings.CORRELATION_TIME_WINDOW
        );

        List<Setting<?>> settings = plugin.getSettings();
        Assert.assertTrue(settings.containsAll(expectedSettings));
    }

    /**
     * test settings get value
     */
    public void testSettingsGetValue() {
        Settings settings = Settings.builder().put("index.correlation", true).build();
        Assert.assertEquals(EventsCorrelationSettings.IS_CORRELATION_INDEX_SETTING.get(settings), true);
        settings = Settings.builder()
            .put("plugins.security_analytics.correlation_time_window", new TimeValue(10, TimeUnit.MINUTES))
            .build();
        Assert.assertEquals(EventsCorrelationSettings.CORRELATION_TIME_WINDOW.get(settings), new TimeValue(10, TimeUnit.MINUTES));
    }
}
