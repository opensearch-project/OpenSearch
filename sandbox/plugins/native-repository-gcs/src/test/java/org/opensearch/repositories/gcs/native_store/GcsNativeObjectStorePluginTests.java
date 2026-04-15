/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.native_store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for {@link GcsNativeObjectStorePlugin#buildConfigJson}.
 */
public class GcsNativeObjectStorePluginTests extends OpenSearchTestCase {

    public void testBuildConfigJsonMinimal() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("my-repo", "gcs", Settings.builder()
            .put("bucket", "my-gcs-bucket")
            .build());

        final String json = GcsNativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        assertTrue(json.contains("\"bucket\":\"my-gcs-bucket\""));
        assertFalse(json.contains("service_account_key"));
    }

    public void testBuildConfigJsonWithClientName() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "gcs", Settings.builder()
            .put("bucket", "b")
            .put("client", "custom")
            .build());

        final String json = GcsNativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        assertTrue(json.contains("\"bucket\":\"b\""));
    }

    public void testBuildConfigJsonProducesValidJson() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "gcs", Settings.builder()
            .put("bucket", "test-bucket")
            .build());

        final String json = GcsNativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), null, json)) {
            final Map<String, Object> map = parser.map();
            assertEquals("test-bucket", map.get("bucket"));
        }
    }
}
