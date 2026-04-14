/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.native_store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for {@link S3NativeObjectStorePlugin#buildConfigJson}.
 * Tests config JSON generation from repo metadata and node settings — no FFM required.
 */
public class S3NativeObjectStorePluginTests extends OpenSearchTestCase {

    public void testBuildConfigJsonMinimal() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("my-repo", "s3", Settings.builder()
            .put("bucket", "my-bucket")
            .build());

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        assertTrue(json.contains("\"bucket\":\"my-bucket\""));
        assertFalse(json.contains("\"region\""));
        assertFalse(json.contains("\"endpoint\""));
        assertTrue(json.contains("\"virtual_hosted_style\":true"));
        assertTrue(json.contains("\"bucket_key\":true"));
    }

    public void testBuildConfigJsonWithRegionAndEndpoint() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("my-repo", "s3", Settings.builder()
            .put("bucket", "test-bucket")
            .put("client", "my-client")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.my-client.region", "us-west-2")
            .put("s3.client.my-client.endpoint", "https://s3.us-west-2.amazonaws.com")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"bucket\":\"test-bucket\""));
        assertTrue(json.contains("\"region\":\"us-west-2\""));
        assertTrue(json.contains("\"endpoint\":\"https://s3.us-west-2.amazonaws.com\""));
    }

    public void testBuildConfigJsonDefaultClient() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.default.region", "eu-west-1")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"region\":\"eu-west-1\""));
    }

    public void testBuildConfigJsonPathStyleAccess() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.default.path_style_access", "true")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"virtual_hosted_style\":false"));
    }

    public void testBuildConfigJsonAllowHttp() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.default.protocol", "http")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"allow_http\":true"));
    }

    public void testBuildConfigJsonHttpsDoesNotSetAllowHttp() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.default.protocol", "https")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertFalse(json.contains("\"allow_http\""));
    }

    public void testBuildConfigJsonProxy() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.default.proxy.host", "proxy.example.com")
            .put("s3.client.default.proxy.port", "8080")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"proxy_url\":\"https://proxy.example.com:8080\""));
    }

    public void testBuildConfigJsonProxyWithHttpProtocol() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.proxy.host", "proxy.local")
            .put("s3.client.default.proxy.port", "3128")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"proxy_url\":\"http://proxy.local:3128\""));
    }

    public void testBuildConfigJsonSseKms() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .put("server_side_encryption_kms_key_id", "arn:aws:kms:us-east-1:123:key/abc")
            .build());

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        assertTrue(json.contains("\"sse_kms_key_id\":\"arn:aws:kms:us-east-1:123:key/abc\""));
    }

    public void testBuildConfigJsonBucketKeyDisabled() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .put("server_side_encryption_bucket_key_enabled", "false")
            .build());

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        assertTrue(json.contains("\"bucket_key\":false"));
    }

    public void testBuildConfigJsonMaxRetries() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.default.max_retries", "5")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"max_retries\":5"));
    }

    public void testBuildConfigJsonNoMaxRetriesWhenDefault() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "b")
            .build());

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        assertFalse(json.contains("\"max_retries\""));
    }

    public void testBuildConfigJsonAllSettings() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("full-repo", "s3", Settings.builder()
            .put("bucket", "prod-bucket")
            .put("client", "prod")
            .put("server_side_encryption_kms_key_id", "arn:aws:kms:us-east-1:123:key/xyz")
            .put("server_side_encryption_bucket_key_enabled", "true")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.prod.region", "us-east-1")
            .put("s3.client.prod.endpoint", "https://s3.us-east-1.amazonaws.com")
            .put("s3.client.prod.path_style_access", "false")
            .put("s3.client.prod.protocol", "https")
            .put("s3.client.prod.proxy.host", "proxy.corp.com")
            .put("s3.client.prod.proxy.port", "8080")
            .put("s3.client.prod.max_retries", "3")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        assertTrue(json.contains("\"bucket\":\"prod-bucket\""));
        assertTrue(json.contains("\"region\":\"us-east-1\""));
        assertTrue(json.contains("\"endpoint\":\"https://s3.us-east-1.amazonaws.com\""));
        assertTrue(json.contains("\"virtual_hosted_style\":true"));
        assertFalse(json.contains("\"allow_http\"")); // https
        assertTrue(json.contains("\"proxy_url\":\"https://proxy.corp.com:8080\""));
        assertTrue(json.contains("\"sse_kms_key_id\":\"arn:aws:kms:us-east-1:123:key/xyz\""));
        assertTrue(json.contains("\"bucket_key\":true"));
        assertTrue(json.contains("\"max_retries\":3"));
        // No credentials in JSON
        assertFalse(json.contains("access_key"));
        assertFalse(json.contains("secret_access_key"));
    }

    public void testBuildConfigJsonProducesValidJson() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "test\"bucket")
            .build());

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, Settings.EMPTY);

        // Parse back to verify it's valid JSON
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), null, json)) {
            final Map<String, Object> map = parser.map();
            assertNotNull(map.get("bucket"));
        }
    }

    public void testBuildConfigJsonAllSettingsRoundTrip() throws IOException {
        final RepositoryMetadata metadata = new RepositoryMetadata("repo", "s3", Settings.builder()
            .put("bucket", "prod-bucket")
            .put("client", "prod")
            .put("server_side_encryption_kms_key_id", "arn:aws:kms:us-east-1:123:key/xyz")
            .put("server_side_encryption_bucket_key_enabled", "true")
            .build());
        final Settings nodeSettings = Settings.builder()
            .put("s3.client.prod.region", "us-east-1")
            .put("s3.client.prod.endpoint", "https://s3.us-east-1.amazonaws.com")
            .put("s3.client.prod.path_style_access", "false")
            .put("s3.client.prod.protocol", "https")
            .put("s3.client.prod.proxy.host", "proxy.corp.com")
            .put("s3.client.prod.proxy.port", "8080")
            .put("s3.client.prod.max_retries", "3")
            .build();

        final String json = S3NativeObjectStorePlugin.buildConfigJson(metadata, nodeSettings);

        // Parse back and verify all fields
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry(), null, json)) {
            final Map<String, Object> map = parser.map();
            assertEquals("prod-bucket", map.get("bucket"));
            assertEquals("us-east-1", map.get("region"));
            assertEquals("https://s3.us-east-1.amazonaws.com", map.get("endpoint"));
            assertEquals(true, map.get("virtual_hosted_style"));
            assertEquals("https://proxy.corp.com:8080", map.get("proxy_url"));
            assertEquals("arn:aws:kms:us-east-1:123:key/xyz", map.get("sse_kms_key_id"));
            assertEquals(true, map.get("bucket_key"));
            assertEquals(3, map.get("max_retries"));
            assertNull(map.get("access_key_id"));
            assertNull(map.get("secret_access_key"));
        }
    }
}
