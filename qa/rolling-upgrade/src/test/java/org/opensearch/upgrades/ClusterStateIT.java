/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ClusterStateIT extends AbstractRollingTestCase{
    private static final String templateName = "my_template";
    private static final String indexName = "test_cluster_state";
    private static final String componentTemplateName = "test_component_template";

    // Add different types of metadata to Cluster State to ensure the new versions understand these metadata types in future versions
    public void testUpgradeWithRemotePublicationEnabled() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // verify that Remote Publication is Enabled on the cluster
            verifyRemotePublicationEnabled();
            // Create Index
            Settings indexSettings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .put("index.refresh_interval", "5s").build();
            String indexMappings = "\"properties\": {\"name\": {\"type\":\"text\"}}";
            String aliases = "\"main\":{\"is_write_index\":true}";
            createIndex(indexName, indexSettings, indexMappings, aliases);
            // Create Index Template
            createIndexTemplate();
            // Create Component Template
            createComponentTemplate();
            // Create Composable Template
            createComposableTemplate();
            // Add Settings
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
//            Settings are required to wiped after Tests, so Settings are not present after rolling upgrade starts
//            verifySettingsInClusterState();
        }
    }

    public void testTemplateMetadataUpgrades() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createIndexTemplate();
            Request request = new Request("GET", "_cluster/state/metadata");
            Response response = client().performRequest(request);
            assertOK(response);

            verifyTemplateMetadataInClusterState(response);
        } else {
            Request request = new Request("GET", "_cluster/state/metadata");
            Response response = client().performRequest(request);
            assertOK(response);

            verifyTemplateMetadataInClusterState(response);
        }
    }

    private static void createIndexTemplate() throws Exception {
        Request putIndexTemplate = new Request("PUT", "_template/" + templateName);
        String indexTemplateJson = "{\n" +
            "  \"index_patterns\": [\"pattern-*\", \"log-*\"],\n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 3,\n" +
            "    \"number_of_replicas\": 1,\n" +
            "    \"index.refresh_interval\": \"5s\"\n" +
            "  },\n" +
            "  \"mappings\": {\n" +
            "    \"properties\": {\n" +
            "      \"timestamp\": {\n" +
            "        \"type\": \"date\"\n" +
            "      },\n" +
            "      \"message\": {\n" +
            "        \"type\": \"text\",\n" +
            "        \"fields\": {\n" +
            "          \"keyword\": {\n" +
            "            \"type\": \"keyword\",\n" +
            "            \"ignore_above\": 256\n" +
            "          }\n" +
            "        }\n" +
            "      },\n" +
            "      \"level\": {\n" +
            "        \"type\": \"keyword\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"aliases\": {\n" +
            "    \"mydata\": {}\n" +
            "  },\n" +
            "  \"version\": 1\n" +
            "}";

        putIndexTemplate.setJsonEntity(indexTemplateJson);
        client().performRequest(putIndexTemplate);
    }

    private static void createComponentTemplate() throws Exception {
        Request putComponentTemplate = new Request("PUT", "_component_template/" + componentTemplateName);
        String componentTemplateJson = "{\n" +
            "  \"template\": {\n" +
            "    \"mappings\": {\n" +
            "      \"properties\": {\n" +
            "        \"name\": {\n" +
            "          \"type\": \"text\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"version\": 1\n" +
            "}";

        putComponentTemplate.setJsonEntity(componentTemplateJson);
        client().performRequest(putComponentTemplate);
    }

    private static void createComposableTemplate() throws Exception {
        Request putComposableTemplate = new Request("PUT", "_index_template/composable_template");
        String composableTemplateJson = "{\n" +
            "  \"index_patterns\": [\"te-*\", \"bar-*\"],\n" +
            "  \"template\": {\n" +
            "    \"mappings\": {\n" +
            "      \"properties\": {\n" +
            "        \"email\": {\n" +
            "          \"type\": \"keyword\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"version\": 1,\n" +
            "  \"composed_of\": [\n" +
            "    \"" + componentTemplateName + "\"\n" +
            "  ]\n" +
            "}";

        putComposableTemplate.setJsonEntity(composableTemplateJson);
        client().performRequest(putComposableTemplate);
    }

    private static void addPersistentSettings() throws Exception {
        Request putSettings = new Request("PUT", "_cluster/settings");
        String settingsJson = "{\n" +
            "  \"persistent\": {\n" +
            "    \"cluster\": {\n" +
            "      \"remote\": {\n" +
            "        \"cluster\": {\n" +
            "          \"seeds\": [\n" +
            "            \"127.0.0.1:9300\"\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        putSettings.setJsonEntity(settingsJson);
        assertOK(client().performRequest(putSettings));
    }

    private static void addTransientSettings() throws Exception {
        Request putSettings = new Request("PUT", "_cluster/settings");
        String settingsJson = "{\n" +
            "  \"transient\": {\n" +
            "    \"cluster\": {\n" +
            "      \"remote\": {\n" +
            "        \"cluster\": {\n" +
            "          \"seeds\": [\n" +
            "            \"127.0.0.1:9300\"\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

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

        // Verify index settings
        Map<String, Object> settings = (Map<String, Object>) index.get("settings");
        assertNotNull("Settings should exist in index", settings);

        // Verify index mappings
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

        // Verify index patterns
        List<String> indexPatterns = (List<String>) templateMetadata.get("index_patterns");
        assertEquals("Index patterns should match", Arrays.asList("pattern-*", "log-*"), indexPatterns);

        // Verify settings
        Map<String, Object> settings = (Map<String, Object>) getNestedValue(templateMetadata, "settings");

        assertEquals("Refresh interval should be 5s", "5s", getNestedValue(settings, "index", "refresh_interval"));

        // Verify mappings
        Map<String, Object> mappings = (Map<String, Object>) getNestedValue(templateMetadata, "mappings");
        assertNotNull("Mappings should exist", mappings);

        Map<String, Object> properties = (Map<String, Object>) getNestedValue(mappings, "_doc", "properties");
        assertNotNull("Properties should exist in mappings", properties);

        assertEquals("timestamp should be of type date", "date", getNestedValue(properties, "timestamp", "type"));

        Map<String, Object> messageField = (Map<String, Object>) properties.get("message");
        assertEquals("message should be of type text", "text", messageField.get("type"));

        Map<String, Object> messageFields = (Map<String, Object>) messageField.get("fields");
        assertNotNull("message should have subfields", messageFields);

        Map<String, Object> keywordField = (Map<String, Object>) messageFields.get("keyword");
        assertEquals("message.keyword should be of type keyword", "keyword", keywordField.get("type"));
        assertEquals("message.keyword should ignore above 256", 256, ((Integer) keywordField.get("ignore_above")).intValue());

        assertEquals("level should be of type keyword", "keyword", ((Map<String, Object>) properties.get("level")).get("type"));

        // Verify aliases
        Map<String, Object> aliases = (Map<String, Object>) templateMetadata.get("aliases");
        assertTrue("Alias 'mydata' should exist", aliases.containsKey("mydata"));

        // Verify version
        assertEquals("Template version should be 1", 1, templateMetadata.get("version"));
    }

    private static void verifyComponentTemplateInClusterState(Response clusterStateResponse) throws Exception {
        Map<String, Object> responseMap = entityAsMap(clusterStateResponse);
        Map<String, Object> metadata = (Map<String, Object>) responseMap.get("metadata");
        assertNotNull("Metadata should exist in response", metadata);

        Map<String, Object> componentTemplate = (Map<String, Object>) getNestedValue(metadata, "component_template", "component_template", componentTemplateName);
        assertNotNull("Component template should exist in metadata", componentTemplate);

        Map<String, Object> templateMetadata = (Map<String, Object>) componentTemplate.get("template");
        assertNotNull("Template should exist in component template", templateMetadata);

        // Verify component template mappings
        Map<String, Object> mappings = (Map<String, Object>) templateMetadata.get("mappings");
        assertNotNull("Mappings should exist in component template", mappings);

        // Verify mappings present
        assertNotNull("Mappings should have email field", getNestedValue(mappings, "properties", "name"));
    }

    private static void verifyComposableTemplateInClusterState(Response clusterStateResponse) throws Exception {
        Map<String, Object> responseMap = entityAsMap(clusterStateResponse);
        Map<String, Object> metadata = (Map<String, Object>) responseMap.get("metadata");
        assertNotNull("Metadata should exist in response", metadata);

        Map<String, Object> indexTemplates = (Map<String, Object>) getNestedValue(metadata, "index_template", "index_template");
        assertNotNull("Index templates should exist in metadata", indexTemplates);

        Map<String, Object> composableTemplate = (Map<String, Object>) indexTemplates.get("composable_template");
        assertNotNull("Composable template should exist in metadata", composableTemplate);

        // Verify index patterns
        List<String> indexPatterns = (List<String>) composableTemplate.get("index_patterns");
        assertEquals("Index patterns should match", Arrays.asList("te-*", "bar-*"), indexPatterns);

        // Verify composed_of
        List<String> composedOf = (List<String>) composableTemplate.get("composed_of");
        assertEquals("Composed templates should match", Arrays.asList(componentTemplateName), composedOf);
    }

    private static void verifySettingsInClusterState() throws Exception {
        Request getSettingRequest = new Request("GET", "/_cluster/settings");
        Response response = client().performRequest(getSettingRequest);
        assertOK(response);
        Map<String, Object> responseMap = entityAsMap(response);

        Map<String, Object> persistent = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull("Persistent settings should exist in metadata", persistent);

        Map<String, Object> remote = (Map<String, Object>) getNestedValue(persistent, "cluster", "remote");
        assertNotNull("Remote settings should exist in persistent settings", remote);

        Map<String, Object> cluster = (Map<String, Object>) remote.get("cluster");
        assertNotNull("Cluster settings should exist in remote settings", cluster);

        List<String> seeds = (List<String>) cluster.get("seeds");
        assertEquals("Seeds should match", Arrays.asList("127.0.0.1:9300"), seeds);

        Map<String, Object> transientSettings = (Map<String, Object>) responseMap.get("transient");
        assertNotNull("Transient settings should exist in metadata", transientSettings);

        remote = (Map<String, Object>) getNestedValue(transientSettings, "cluster", "remote");
        assertNotNull("Remote settings should exist in transient settings", remote);

        cluster = (Map<String, Object>) remote.get("cluster");
        assertNotNull("Cluster settings should exist in remote settings", cluster);

        seeds = (List<String>) cluster.get("seeds");
        assertEquals("Seeds should match", Arrays.asList("127.0.0.1:9300"), seeds);
    }

    private static Object getNestedValue(Map<String, Object> map, String... keys) {
        Object current = map;
        for (String key : keys) {
            if (!(current instanceof Map)) {
                return null;
            }
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
