/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs.native_store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for {@link FsNativeObjectStorePlugin#buildConfigJson}.
 */
public class FsNativeObjectStorePluginTests extends OpenSearchTestCase {

    public void testBuildConfigJsonWithLocation() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "my-repo",
            "fs",
            Settings.builder().put("location", "/tmp/test-repo").build()
        );

        final String json = FsNativeObjectStorePlugin.buildConfigJson(metadata);

        assertTrue(json.contains("\"base_path\":\"/tmp/test-repo\""));
    }

    public void testBuildConfigJsonEmptyLocation() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "fs", Settings.EMPTY);

        final String json = FsNativeObjectStorePlugin.buildConfigJson(metadata);

        assertTrue(json.contains("\"base_path\":\"\""));
    }

    public void testBuildConfigJsonEscapesSpecialChars() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata(
            "repo",
            "fs",
            Settings.builder().put("location", "C:\\Users\\test\\repo").build()
        );

        final String json = FsNativeObjectStorePlugin.buildConfigJson(metadata);

        // XContentBuilder handles escaping
        assertTrue(json.contains("C:\\\\Users\\\\test\\\\repo"));
    }

    public void testBuildConfigJsonProducesValidJson() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "fs", Settings.builder().put("location", "/data/repo").build());

        final String json = FsNativeObjectStorePlugin.buildConfigJson(metadata);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), null, json)) {
            final Map<String, Object> map = parser.map();
            assertEquals("/data/repo", map.get("base_path"));
        }
    }
}
