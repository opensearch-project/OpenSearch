/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.telemetry.OTelTelemetrySettings.OTEL_SERVICE_NAME_SETTING;

public class OTelTelemetrySettingsTests extends OpenSearchTestCase {

    public void testOtelServiceNameDefaultsToOpenSearch() {
        assertEquals("OpenSearch", OTEL_SERVICE_NAME_SETTING.get(Settings.EMPTY));
    }

    public void testOtelServiceNameUsesConfiguredValue() {
        Settings settings = Settings.builder().put(OTEL_SERVICE_NAME_SETTING.getKey(), "jira-search").build();
        assertEquals("jira-search", OTEL_SERVICE_NAME_SETTING.get(settings));
    }

    public void testOtelServiceNameRejectsBlankValue() {
        for (String invalid : new String[] { "", " ", "\t" }) {
            Settings settings = Settings.builder().put(OTEL_SERVICE_NAME_SETTING.getKey(), invalid).build();
            expectThrows(IllegalArgumentException.class, () -> OTEL_SERVICE_NAME_SETTING.get(settings));
        }
    }
}
