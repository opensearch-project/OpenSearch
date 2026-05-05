/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class WorkloadGroupSearchSettingsTests extends OpenSearchTestCase {

    public void testWlmSearchTimeoutSettingExists() {
        assertNotNull(WorkloadGroupSearchSettings.WLM_SEARCH_TIMEOUT);
        assertEquals("search.default_search_timeout", WorkloadGroupSearchSettings.WLM_SEARCH_TIMEOUT.getKey());
    }

    public void testValidateSettingsValid() {
        Settings settings = Settings.builder().put("search.default_search_timeout", "30s").build();
        WorkloadGroupSearchSettings.validate(settings);
    }

    public void testValidateSettingsValidTimeValues() {
        for (String timeVal : new String[] { "30s", "5m", "1h", "500ms" }) {
            Settings settings = Settings.builder().put("search.default_search_timeout", timeVal).build();
            WorkloadGroupSearchSettings.validate(settings);
        }
    }

    public void testValidateSettingsUnknownKey() {
        Settings settings = Settings.builder().put("unknown_key", "value").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Unknown WLM setting: unknown_key"));
    }

    public void testValidateSettingsInvalidValue() {
        Settings settings = Settings.builder().put("search.default_search_timeout", "not_a_time").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.default_search_timeout"));
    }

    public void testValidateSettingsNull() {
        WorkloadGroupSearchSettings.validate(null);
    }

    public void testValidateSettingsEmpty() {
        WorkloadGroupSearchSettings.validate(Settings.EMPTY);
    }

    public void testGetRegisteredSettings() {
        assertNotNull(WorkloadGroupSearchSettings.getRegisteredSettings());
        assertTrue(WorkloadGroupSearchSettings.getRegisteredSettings().containsKey("search.default_search_timeout"));
    }

    public void testLegacyTimeoutKeyRejected() {
        Settings settings = Settings.builder().put("timeout", "30s").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Unknown WLM setting: timeout"));
    }
}
