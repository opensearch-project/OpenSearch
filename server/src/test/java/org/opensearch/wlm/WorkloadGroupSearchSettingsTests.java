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

    public void testWlmCancelAfterTimeIntervalSettingExists() {
        assertNotNull(WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL);
        assertEquals("search.cancel_after_time_interval", WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey());
    }

    public void testWlmMaxConcurrentShardRequestsSettingExists() {
        assertNotNull(WorkloadGroupSearchSettings.WLM_MAX_CONCURRENT_SHARD_REQUESTS);
        assertEquals("search.max_concurrent_shard_requests", WorkloadGroupSearchSettings.WLM_MAX_CONCURRENT_SHARD_REQUESTS.getKey());
    }

    public void testWlmBatchedReduceSizeSettingExists() {
        assertNotNull(WorkloadGroupSearchSettings.WLM_BATCHED_REDUCE_SIZE);
        assertEquals("search.batched_reduce_size", WorkloadGroupSearchSettings.WLM_BATCHED_REDUCE_SIZE.getKey());
    }

    public void testWlmOverrideRequestValuesSettingExists() {
        assertNotNull(WorkloadGroupSearchSettings.WLM_OVERRIDE_REQUEST_VALUES);
        assertEquals("override_request_values", WorkloadGroupSearchSettings.WLM_OVERRIDE_REQUEST_VALUES.getKey());
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

    public void testValidateCancelAfterTimeInterval() {
        Settings settings = Settings.builder().put("search.cancel_after_time_interval", "1m").build();
        WorkloadGroupSearchSettings.validate(settings);

        settings = Settings.builder().put("search.cancel_after_time_interval", "30s").build();
        WorkloadGroupSearchSettings.validate(settings);
    }

    public void testValidateMaxConcurrentShardRequests() {
        Settings settings = Settings.builder().put("search.max_concurrent_shard_requests", "1").build();
        WorkloadGroupSearchSettings.validate(settings);

        settings = Settings.builder().put("search.max_concurrent_shard_requests", "100").build();
        WorkloadGroupSearchSettings.validate(settings);
    }

    public void testValidateMaxConcurrentShardRequestsInvalid() {
        Settings settings = Settings.builder().put("search.max_concurrent_shard_requests", "0").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.max_concurrent_shard_requests"));

        Settings settings2 = Settings.builder().put("search.max_concurrent_shard_requests", "-1").build();
        exception = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupSearchSettings.validate(settings2));
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.max_concurrent_shard_requests"));
    }

    public void testValidateBatchedReduceSize() {
        Settings settings = Settings.builder().put("search.batched_reduce_size", "2").build();
        WorkloadGroupSearchSettings.validate(settings);

        settings = Settings.builder().put("search.batched_reduce_size", "512").build();
        WorkloadGroupSearchSettings.validate(settings);
    }

    public void testValidateBatchedReduceSizeInvalid() {
        Settings settings = Settings.builder().put("search.batched_reduce_size", "1").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.batched_reduce_size"));

        Settings settings2 = Settings.builder().put("search.batched_reduce_size", "0").build();
        exception = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupSearchSettings.validate(settings2));
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.batched_reduce_size"));
    }

    public void testValidateOverrideRequestValues() {
        Settings settings = Settings.builder().put("override_request_values", "true").build();
        WorkloadGroupSearchSettings.validate(settings);

        settings = Settings.builder().put("override_request_values", "false").build();
        WorkloadGroupSearchSettings.validate(settings);
    }

    public void testValidateMultipleSettings() {
        Settings settings = Settings.builder()
            .put("search.default_search_timeout", "30s")
            .put("search.cancel_after_time_interval", "1m")
            .put("search.max_concurrent_shard_requests", "5")
            .put("search.batched_reduce_size", "256")
            .put("override_request_values", "true")
            .build();
        WorkloadGroupSearchSettings.validate(settings);
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

    public void testLegacyTimeoutKeyRejected() {
        Settings settings = Settings.builder().put("timeout", "30s").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Unknown WLM setting: timeout"));
    }

    public void testValidateNonNumericIntSetting() {
        Settings settings = Settings.builder().put("search.max_concurrent_shard_requests", "abc").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.max_concurrent_shard_requests"));

        Settings settings2 = Settings.builder().put("search.batched_reduce_size", "xyz").build();
        exception = expectThrows(IllegalArgumentException.class, () -> WorkloadGroupSearchSettings.validate(settings2));
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.batched_reduce_size"));
    }

    public void testValidateInvalidCancelAfterTimeInterval() {
        Settings settings = Settings.builder().put("search.cancel_after_time_interval", "not_a_time").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("search.cancel_after_time_interval"));
    }

    public void testValidateInvalidOverrideRequestValues() {
        Settings settings = Settings.builder().put("override_request_values", "not_a_boolean").build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroupSearchSettings.validate(settings)
        );
        assertTrue(exception.getMessage().contains("Invalid value"));
        assertTrue(exception.getMessage().contains("override_request_values"));
    }
}
