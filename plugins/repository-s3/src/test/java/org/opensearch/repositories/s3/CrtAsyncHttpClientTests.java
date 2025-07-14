/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.repositories.s3;

import org.junit.Before;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.nio.file.Path;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrtAsyncHttpClientTests extends OpenSearchTestCase {

    private S3ClientSettings clientSettings;
    private CrtAsyncHttpClient client;

    @Before
    public void setup() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.default.max_retries", 10).build(),
            configPath()
        );
        clientSettings = settings.get("default");
        client = new CrtAsyncHttpClient(clientSettings);
    }

    public void testCreateClientWithDefaultSettings() {
        SdkAsyncHttpClient asyncClient = client.asyncHttpClient();
        assertNotNull(asyncClient);
        assertTrue(asyncClient instanceof AwsCrtAsyncHttpClient);
    }

    public void testCreateClientWithProxy() {
        SdkAsyncHttpClient asyncClient = client.asyncHttpClient();
        assertNotNull(asyncClient);
        assertTrue(asyncClient instanceof AwsCrtAsyncHttpClient);
    }

    protected Path configPath() {
        return PathUtils.get("config");
    }
}
