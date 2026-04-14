/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure.native_store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for {@link AzureNativeObjectStorePlugin#buildConfigJson}.
 */
public class AzureNativeObjectStorePluginTests extends OpenSearchTestCase {

    public void testBuildConfigJsonMinimal() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("my-repo", "azure", Settings.builder()
            .put("container", "my-container")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("azure.client.default.account", "myaccount")
            .build();

        final String json = AzureNativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"account\":\"myaccount\""));
        assertTrue(json.contains("\"container\":\"my-container\""));
        assertFalse(json.contains("access_key"));
        assertFalse(json.contains("sas_token"));
    }

    public void testBuildConfigJsonDefaultContainer() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "azure", Settings.EMPTY);
        final Settings nodeSettings = Settings.builder()
            .put("azure.client.default.account", "acc")
            .build();

        final String json = AzureNativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"container\":\"opensearch-snapshots\""));
    }

    public void testBuildConfigJsonWithMaxRetries() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "azure", Settings.builder()
            .put("container", "c")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("azure.client.default.account", "acc")
            .put("azure.client.default.max_retries", "5")
            .build();

        final String json = AzureNativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"max_retries\":5"));
    }

    public void testBuildConfigJsonNoMaxRetriesWhenDefault() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "azure", Settings.builder()
            .put("container", "c")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("azure.client.default.account", "acc")
            .build();

        final String json = AzureNativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertFalse(json.contains("\"max_retries\""));
    }

    public void testBuildConfigJsonNamedClient() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "azure", Settings.builder()
            .put("client", "prod")
            .put("container", "prod-container")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("azure.client.prod.account", "prodaccount")
            .put("azure.client.prod.max_retries", "3")
            .build();

        final String json = AzureNativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"account\":\"prodaccount\""));
        assertTrue(json.contains("\"container\":\"prod-container\""));
        assertTrue(json.contains("\"max_retries\":3"));
    }

    public void testBuildConfigJsonProducesValidJson() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "azure", Settings.builder()
            .put("container", "c")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("azure.client.default.account", "acc")
            .build();

        final String json = AzureNativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), null, json)) {
            final Map<String, Object> map = parser.map();
            assertEquals("acc", map.get("account"));
            assertEquals("c", map.get("container"));
        }
    }
}
