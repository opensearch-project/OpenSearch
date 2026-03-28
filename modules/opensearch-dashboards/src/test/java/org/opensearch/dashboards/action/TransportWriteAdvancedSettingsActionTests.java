/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class TransportWriteAdvancedSettingsActionTests extends OpenSearchTestCase {

    public void testBuildCreateDocumentSeedsBuildNumAndUpgradesPriorConfig() {
        Map<String, Object> priorDoc = new HashMap<>();
        priorDoc.put("type", "config");
        priorDoc.put("config", Map.of("buildNum", 3050099, "theme:darkMode", true, "dateFormat:tz", "UTC"));
        priorDoc.put("references", List.of());
        priorDoc.put("updated_at", "old");

        Map<String, Object> newSettings = Map.of("dateFormat:tz", "America/New_York");

        Map<String, Object> doc = TransportWriteAdvancedSettingsAction.buildCreateDocument(newSettings, priorDoc, "now", 3060099);

        assertThat(doc, hasEntry("type", (Object) "config"));
        assertThat(doc, hasEntry("references", (Object) List.of()));
        assertThat(doc, hasEntry("updated_at", (Object) "now"));

        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) doc.get("config");
        assertThat(config, hasEntry("buildNum", (Object) 3060099));
        assertThat(config, hasEntry("theme:darkMode", (Object) true));
        assertThat(config, hasEntry("dateFormat:tz", (Object) "America/New_York"));
    }

    public void testBuildUpdatedDocumentPreservesRootMetadataAndMergesConfig() {
        Map<String, Object> existingDoc = new HashMap<>();
        existingDoc.put("type", "config");
        existingDoc.put("config", Map.of("buildNum", 3060099, "theme:darkMode", true, "dateFormat:tz", "UTC"));
        existingDoc.put("references", List.of());
        existingDoc.put("migrationVersion", Map.of("config", "1.0.0"));
        existingDoc.put("permissions", Map.of("read", List.of("*")));
        existingDoc.put("updated_at", "old");

        Map<String, Object> doc = TransportWriteAdvancedSettingsAction.buildUpdatedDocument(
            existingDoc,
            Map.of("dateFormat:tz", "America/Los_Angeles"),
            "now"
        );

        assertThat(doc, hasEntry("migrationVersion", (Object) Map.of("config", "1.0.0")));
        assertThat(doc, hasEntry("permissions", (Object) Map.of("read", List.of("*"))));
        assertThat(doc, hasEntry("updated_at", (Object) "now"));

        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) doc.get("config");
        assertThat(config, hasEntry("buildNum", (Object) 3060099));
        assertThat(config, hasEntry("theme:darkMode", (Object) true));
        assertThat(config, hasEntry("dateFormat:tz", (Object) "America/Los_Angeles"));
    }

    public void testExtractConfigVersion() {
        assertThat(TransportWriteAdvancedSettingsAction.extractConfigVersion("config:3.6.0"), equalTo("3.6.0"));
        assertThat(TransportWriteAdvancedSettingsAction.extractConfigVersion("_dashboard_admin"), is((String) null));
    }

    public void testIsConfigVersionUpgradeable() {
        assertThat(TransportWriteAdvancedSettingsAction.isConfigVersionUpgradeable("3.5.0", "3.6.0"), is(true));
        assertThat(TransportWriteAdvancedSettingsAction.isConfigVersionUpgradeable("3.6.0-rc1", "3.6.0"), is(true));
        assertThat(TransportWriteAdvancedSettingsAction.isConfigVersionUpgradeable("3.6.0", "3.6.0"), is(false));
        assertThat(TransportWriteAdvancedSettingsAction.isConfigVersionUpgradeable("3.6.0-SNAPSHOT", "3.6.0"), is(false));
        assertThat(TransportWriteAdvancedSettingsAction.isConfigVersionUpgradeable("7.10.2", "3.6.0"), is(true));
        assertThat(TransportWriteAdvancedSettingsAction.isConfigVersionUpgradeable("7.11.0", "3.6.0"), is(false));
    }

    public void testBuildCreateDocumentWithoutPriorConfigStillCreatesEnvelope() {
        Map<String, Object> doc = TransportWriteAdvancedSettingsAction.buildCreateDocument(
            Map.of("dateFormat:tz", "UTC"),
            null,
            "now",
            3060099
        );

        assertThat(doc, hasKey("config"));

        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) doc.get("config");
        assertThat(config, hasEntry("buildNum", (Object) 3060099));
        assertThat(config, hasEntry("dateFormat:tz", (Object) "UTC"));
        assertThat(config, not(hasKey("theme:darkMode")));
    }
}
