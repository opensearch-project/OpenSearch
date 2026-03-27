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
        assertEquals("batched_reduce_size", WorkloadGroupSearchSettings.WlmSearchSetting.BATCHED_REDUCE_SIZE.getSettingName());
        assertEquals(
            "cancel_after_time_interval",
            WorkloadGroupSearchSettings.WlmSearchSetting.CANCEL_AFTER_TIME_INTERVAL.getSettingName()
        );
        assertEquals(
            "max_concurrent_shard_requests",
            WorkloadGroupSearchSettings.WlmSearchSetting.MAX_CONCURRENT_SHARD_REQUESTS.getSettingName()
        );
        assertEquals("timeout", WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.getSettingName());
    }

    public void testFromKeyValidSettings() {
        assertEquals(
            WorkloadGroupSearchSettings.WlmSearchSetting.BATCHED_REDUCE_SIZE,
            WorkloadGroupSearchSettings.WlmSearchSetting.fromKey("batched_reduce_size")
        );
        assertEquals(
            WorkloadGroupSearchSettings.WlmSearchSetting.CANCEL_AFTER_TIME_INTERVAL,
            WorkloadGroupSearchSettings.WlmSearchSetting.fromKey("cancel_after_time_interval")
        );
        assertEquals(
            WorkloadGroupSearchSettings.WlmSearchSetting.MAX_CONCURRENT_SHARD_REQUESTS,
            WorkloadGroupSearchSettings.WlmSearchSetting.fromKey("max_concurrent_shard_requests")
        );
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
        WorkloadGroupSearchSettings.WlmSearchSetting.CANCEL_AFTER_TIME_INTERVAL.validate("1h");
    }

    public void testValidateInvalidTimeValue() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.TIMEOUT.validate("invalid")
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
    }

    public void testValidatePositiveInt() {
        WorkloadGroupSearchSettings.WlmSearchSetting.MAX_CONCURRENT_SHARD_REQUESTS.validate("1");
        WorkloadGroupSearchSettings.WlmSearchSetting.MAX_CONCURRENT_SHARD_REQUESTS.validate("100");
    }

    public void testValidateInvalidPositiveInt() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.MAX_CONCURRENT_SHARD_REQUESTS.validate("0")
        );
        assertTrue(exception.getMessage().contains("must be >= 1"));

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.MAX_CONCURRENT_SHARD_REQUESTS.validate("-1")
        );
        assertTrue(exception.getMessage().contains("must be >= 1"));

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.MAX_CONCURRENT_SHARD_REQUESTS.validate("abc")
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
    }

    public void testValidateSearchSettingsValid() {
        Map<String, String> settings = new HashMap<>();
        settings.put("timeout", "30s");
        settings.put("max_concurrent_shard_requests", "5");

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

    public void testValidateBatchedReduceSize() {
        WorkloadGroupSearchSettings.WlmSearchSetting.BATCHED_REDUCE_SIZE.validate("2");
        WorkloadGroupSearchSettings.WlmSearchSetting.BATCHED_REDUCE_SIZE.validate("512");
    }

    public void testValidateInvalidBatchedReduceSize() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.BATCHED_REDUCE_SIZE.validate("1")
        );
        assertTrue(exception.getMessage().contains("must be >= 2"));

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.BATCHED_REDUCE_SIZE.validate("0")
        );
        assertTrue(exception.getMessage().contains("must be >= 2"));

        exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.WlmSearchSetting.BATCHED_REDUCE_SIZE.validate("abc")
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
    }
}
