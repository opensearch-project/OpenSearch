/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class RemotePublicationClusterStateIT extends AbstractRollingTestCase {
    private static final String templateName = "my_template";
    private static final String indexName = "test_cluster_state";
    private static final String componentTemplateName = "test_component_template";

    public void testUpgradeWithRemotePublicationEnabled() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            verifyRemotePublicationEnabled();
            Settings indexSettings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put("index.refresh_interval", "5s").build();
            String indexMappings = "\"properties\": {\"name\": {\"type\":\"text\"}}";
            String aliases = "\"main\":{\"is_write_index\":true}";
            createIndex(indexName, indexSettings, indexMappings, aliases);
            createIndexTemplate();
            createComponentTemplate();
            createComposableTemplate();
            addPersistentSettings();
            addTransientSettings();

            Request request = new Request("GET", "_cluster/state");
            Response response = client().performRequest(request);
            assertOK(response);

            verifyIndexInClusterState(response);
            verifyTemplateMetadataInClusterState(response);
            verifyComponentTemplateInClusterState(response);
            verifyComposableTemplateInClusterState(response);
            verifySettingsInClusterState();
        } else {
            verifyRemotePublicationEnabled();

            Request request = new Request("GET", "_cluster/state");
            Response response = client().performRequest(request);
            assertOK(response);

            verifyIndexInClusterState(response);
            verifyTemplateMetadataInClusterState(response);
            verifyComponentTemplateInClusterState(response);
            verifyComposableTemplateInClusterState(response);
        }
    }

    private static void createIndexTemplate() throws Exception {
        Request putIndexTemplate = new Request("PUT", "_template/" + templateName);
        String indexTemplateJson = "{\"index_patterns\": [\"pattern-*\", \"log-*\"],\"settings\": {\"number_of_shards\": 3,\"number_of_replicas\": 1,\"index.refresh_interval\": \"5s\"},\"mappings\": {\"properties\": {\"timestamp\": {\"type\": \"date\"},\"message\": {\"type\": \"text\",\"fields\": {\"keyword\": {\"type\": \"keyword\",\"ignore_above\": 256}}},\"level\": {\"type\": \"keyword\"}}},\"aliases\": {\"mydata\": {}},\"version\": 1}";
        putIndexTemplate.setJsonEntity(indexTemplateJson);
        client().performRequest(putIndexTemplate);
    }

    private static void createComponentTemplate() throws Exception {
        Request putComponentTemplate = new Request("PUT", "_component_template/" + componentTemplateName);
        String componentTemplateJson = "{\"template\": {\"mappings\": {\"properties\": {\"name\": {\"type\": \"text\"}}}},\"version\": 1}";
        putComponentTemplate.setJsonEntity(componentTemplateJson);
        client().performRequest(putComponentTemplate);
    }

    private static void createComposableTemplate() throws Exception {
        Request putComposableTemplate = new Request("PUT", "_index_template/composable_template");
        String composableTemplateJson = "{\"index_patterns\": [\"te-*\", \"bar-*\"],\"template\": {\"mappings\": {\"properties\": {\"email\": {\"type\": \"keyword\"}}}},\"version\": 1,\"composed_of\": [\"" + componentTemplateName + "\"]}";
        putComposableTemplate.setJsonEntity(composableTemplateJson);
        client().performRequest(putComposableTemplate);
    }

    private static void addPersistentSettings() throws Exception {
        Request putSettings = new Request("PUT", "_cluster/settings");
        String settingsJson = "{\"persistent\": {\"cluster\": {\"remote\": {\"cluster\": {\"seeds\": [\"127.0.0.1:9300\"]}}}}}";
        putSettings.setJsonEntity(settingsJson);
        assertOK(client().performRequest(putSettings));
    }

    private static void addTransientSettings() throws Exception {
        Request putSettings = new Request("PUT", "_cluster/settings");
        String settingsJson = "{\"transient\": {\"cluster\": {\"remote\": {\"cluster\": {\"seeds\": [\"127.0.0.1:9300\"]}}}}}";
        putSettings.setJsonEntity(settingsJson);
        assertOK(client().performRequest(putSettings));
    }

    private static void verifyIndexInClusterState(Response clusterStateResponse) throws Exception {
        Map<String, Object> responseMap = entityAsMap(clusterStateResponse);
        Map<String, Object> metadata = (Map<String, Object>) responseMap.get("metadata");
        assertNotNull("Metadata should exist in response", metadata);
        Map<String, Object> indices = (Map<String, Object>) metadata.get("indices");
        assertNotNull("Indices should exist in metadata", indices);
        Map<String, Object> index = (Map<String, Object>) indices.get(indexName);
        assertNotNull("Index " + indexName + " should exist in cluster state", index);
        Map<String, Object> settings = (Map<String, Object>) index.get("settings");
        assertNotNull("Settings should exist in index", settings);
        Map<String, Object> mappings = (Map<String, Object>) index.get("mappings");
        assertNotNull("Mappings should exist in index", mappings);
    }

    private static void verifyTemplateMetadataInClusterState(Response clusterStateResponse) throws Exception {
        Map<String, Object> responseMap = entityAsMap(clusterStateResponse);
        Map<String, Object> metadata = (Map<String, Object>) responseMap.get("metadata");
        assertNotNull("Metadata should exist in response", metadata);
        Map<String, Object> templates = (Map<String, Object>) metadata.get("templates");
        assertNotNull("Templates should exist in metadata", templates);
        Map<String, Object> templateMetadata = (Map<String, Object>) templates.get(templateName);
        assertNotNull("Template " + templateName + " should exist in cluster state", templateMetadata);
        List<String> indexPatterns = (List<String>) templateMetadata.get("index_patterns");
        assertEquals("Index patterns should match", Arrays.asList("pattern-*", "log-*"), indexPatterns);
    }

    private static void verifyComponentTemplateInClusterState(Response clusterStateResponse) throws Exception {
        Map<String, Object> responseMap = entityAsMap(clusterStateResponse);
        Map<String, Object> metadata = (Map<String, Object>) responseMap.get("metadata");
        assertNotNull("Metadata should exist in response", metadata);
        Map<String, Object> componentTemplate = (Map<String, Object>) getNestedValue(metadata, "component_template", "component_template", componentTemplateName);
        assertNotNull("Component template should exist in metadata", componentTemplate);
    }

    private static void verifyComposableTemplateInClusterState(Response clusterStateResponse) throws Exception {
        Map<String, Object> responseMap = entityAsMap(clusterStateResponse);
        Map<String, Object> metadata = (Map<String, Object>) responseMap.get("metadata");
        assertNotNull("Metadata should exist in response", metadata);
        Map<String, Object> indexTemplates = (Map<String, Object>) getNestedValue(metadata, "index_template", "index_template");
        assertNotNull("Index templates should exist in metadata", indexTemplates);
        Map<String, Object> composableTemplate = (Map<String, Object>) indexTemplates.get("composable_template");
        assertNotNull("Composable template should exist in metadata", composableTemplate);
    }

    private static void verifySettingsInClusterState() throws Exception {
        Request getSettingRequest = new Request("GET", "/_cluster/settings");
        Response response = client().performRequest(getSettingRequest);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> persistent = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull("Persistent settings should exist in metadata", persistent);
    }

    private static Object getNestedValue(Map<String, Object> map, String... keys) {
        Object current = map;
        for (String key : keys) {
            if (!(current instanceof Map)) return null;
            current = ((Map<String, Object>) current).get(key);
        }
        return current;
    }

    private void verifyRemotePublicationEnabled() throws Exception {
        Request getSettingRequest = new Request("GET", "/_cluster/settings");
        getSettingRequest.addParameter("include_defaults", "true");
        Response response = client().performRequest(getSettingRequest);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> defaultsSettings = (Map<String, Object>) responseMap.get("defaults");
        Object enabled = getNestedValue(defaultsSettings, "cluster", "remote_store", "publication", "enabled");
        assertEquals("true", enabled);
    }
}
