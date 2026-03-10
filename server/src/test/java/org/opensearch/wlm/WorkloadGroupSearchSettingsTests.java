/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class WorkloadGroupSearchSettingsTests extends OpenSearchTestCase {

    public void testEnumSettingNames() {
        assertEquals("timeout", WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.getSettingName());
    }

    public void testFromKeyValidSettings() {
        assertEquals(WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT, WorkloadGroupSearchSettings.WlmSearchSetting.fromKey("timeout"));
    }

    public void testFromKeyInvalidSetting() {
        assertNull(WorkloadGroupSearchSettings.WlmSearchSetting.fromKey("invalid_setting"));
        assertNull(WorkloadGroupSearchSettings.WlmSearchSetting.fromKey(""));
        assertNull(WorkloadGroupSearchSettings.WlmSearchSetting.fromKey(null));
    }

    public void testValidateTimeValue() {
        WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.validate("30s");
        WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.validate("5m");
        WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.validate("1h");
    }

    public void testValidateInvalidTimeValue() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.validate("invalid")
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
    }

    public void testValidateSearchSettingsValid() {
        Map<String, String> settings = new HashMap<>();
        settings.put("timeout", "30s");

        // Should not throw exception
        WorkloadGroupSearchSettings.validateSearchSettings(settings);
    }

    public void testValidateSearchSettingsUnknownSetting() {
        Map<String, String> settings = new HashMap<>();
        settings.put("unknown_setting", "true");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validateSearchSettings(settings)
        );
        assertTrue(exception.getMessage().contains("Unknown search setting: unknown_setting"));
    }

    public void testValidateSearchSettingsInvalidValue() {
        Map<String, String> settings = new HashMap<>();
        settings.put("timeout", "invalid_time");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validateSearchSettings(settings)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
    }

    public void testValidateSearchSettingsNull() {
        // Should not throw exception for null map
        WorkloadGroupSearchSettings.validateSearchSettings(null);
    }

    public void testValidateSearchSettingsNullKey() {
        Map<String, String> settings = new HashMap<>();
        settings.put(null, "30s");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validateSearchSettings(settings)
        );
        assertTrue(exception.getMessage().contains("Search setting key cannot be null"));
    }

    public void testValidateSearchSettingsNullValue() {
        Map<String, String> settings = new HashMap<>();
        settings.put("timeout", null);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validateSearchSettings(settings)
        );
        assertTrue(exception.getMessage().contains("Search setting value cannot be null"));
    }

    public void testValidateSearchSettingsEmpty() {
        Map<String, String> settings = new HashMap<>();

        // Should not throw exception for empty map
        WorkloadGroupSearchSettings.validateSearchSettings(settings);
    }
}
