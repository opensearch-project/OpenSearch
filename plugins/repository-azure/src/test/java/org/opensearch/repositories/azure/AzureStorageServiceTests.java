/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.azure;

import reactor.core.scheduler.Schedulers;

import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.common.policy.RequestRetryPolicy;

import org.junit.AfterClass;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class AzureStorageServiceTests extends OpenSearchTestCase {
    @AfterClass
    public static void shutdownSchedulers() {
        Schedulers.shutdownNow();
    }

    public void testReadSecuredSettings() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.endpoint_suffix", "my_endpoint_suffix")
            .build();

        final Map<String, AzureStorageSettings> loadedSettings = AzureStorageSettings.load(settings);
        assertThat(loadedSettings.keySet(), containsInAnyOrder("azure1", "azure2", "azure3", "default"));

        assertThat(loadedSettings.get("azure1").getEndpointSuffix(), is(emptyString()));
        assertThat(loadedSettings.get("azure2").getEndpointSuffix(), is(emptyString()));
        assertThat(loadedSettings.get("azure3").getEndpointSuffix(), equalTo("my_endpoint_suffix"));
    }

    private AzureRepositoryPlugin pluginWithSettingsValidation(Settings settings) {
        final AzureRepositoryPlugin plugin = new AzureRepositoryPlugin(settings);
        new SettingsModule(settings, plugin.getSettings(), Collections.emptyList(), Collections.emptySet());
        return plugin;
    }

    private AzureStorageService storageServiceWithSettingsValidation(Settings settings) {
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            return plugin.azureStoreService;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testCreateClientWithEndpointSuffix() throws IOException {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.endpoint_suffix", "my_endpoint_suffix")
            .build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService;
            final BlobServiceClient client1 = azureStorageService.client("azure1").v1();
            assertThat(client1.getAccountUrl(), equalTo("https://myaccount1.blob.my_endpoint_suffix"));
            final BlobServiceClient client2 = azureStorageService.client("azure2").v1();
            assertThat(client2.getAccountUrl(), equalTo("https://myaccount2.blob.core.windows.net"));
        }
    }

    public void testReinitClientSettings() throws IOException {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setString("azure.client.azure1.account", "myaccount11");
        secureSettings1.setString("azure.client.azure1.key", encodeKey("mykey11"));
        secureSettings1.setString("azure.client.azure2.account", "myaccount12");
        secureSettings1.setString("azure.client.azure2.key", encodeKey("mykey12"));
        final Settings settings1 = Settings.builder().setSecureSettings(secureSettings1).build();
        final MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setString("azure.client.azure1.account", "myaccount21");
        secureSettings2.setString("azure.client.azure1.key", encodeKey("mykey21"));
        secureSettings2.setString("azure.client.azure3.account", "myaccount23");
        secureSettings2.setString("azure.client.azure3.key", encodeKey("mykey23"));
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings2).build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings1)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService;
            final BlobServiceClient client11 = azureStorageService.client("azure1").v1();
            assertThat(client11.getAccountUrl(), equalTo("https://myaccount11.blob.core.windows.net"));
            final BlobServiceClient client12 = azureStorageService.client("azure2").v1();
            assertThat(client12.getAccountUrl(), equalTo("https://myaccount12.blob.core.windows.net"));
            // client 3 is missing
            final SettingsException e1 = expectThrows(SettingsException.class, () -> azureStorageService.client("azure3"));
            assertThat(e1.getMessage(), is("Unable to find client with name [azure3]"));
            // update client settings
            plugin.reload(settings2);
            // old client 1 not changed
            assertThat(client11.getAccountUrl(), equalTo("https://myaccount11.blob.core.windows.net"));
            // new client 1 is changed
            final BlobServiceClient client21 = azureStorageService.client("azure1").v1();
            assertThat(client21.getAccountUrl(), equalTo("https://myaccount21.blob.core.windows.net"));
            // old client 2 not changed
            assertThat(client12.getAccountUrl(), equalTo("https://myaccount12.blob.core.windows.net"));
            // new client2 is gone
            final SettingsException e2 = expectThrows(SettingsException.class, () -> azureStorageService.client("azure2"));
            assertThat(e2.getMessage(), is("Unable to find client with name [azure2]"));
            // client 3 emerged
            final BlobServiceClient client23 = azureStorageService.client("azure3").v1();
            assertThat(client23.getAccountUrl(), equalTo("https://myaccount23.blob.core.windows.net"));
        }
    }

    public void testReinitClientEmptySettings() throws IOException {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey11"));
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService;
            final BlobServiceClient client11 = azureStorageService.client("azure1").v1();
            assertThat(client11.getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
            // reinit with empty settings
            final SettingsException e = expectThrows(SettingsException.class, () -> plugin.reload(Settings.EMPTY));
            assertThat(e.getMessage(), is("If you want to use an azure repository, you need to define a client configuration."));
            // existing client untouched
            assertThat(client11.getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
            // new client also untouched
            final BlobServiceClient client21 = azureStorageService.client("azure1").v1();
            assertThat(client21.getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
        }
    }

    public void testReinitClientWrongSettings() throws IOException {
        final MockSecureSettings secureSettings1 = new MockSecureSettings();
        secureSettings1.setString("azure.client.azure1.account", "myaccount1");
        secureSettings1.setString("azure.client.azure1.key", encodeKey("mykey11"));
        final Settings settings1 = Settings.builder().setSecureSettings(secureSettings1).build();
        final MockSecureSettings secureSettings2 = new MockSecureSettings();
        secureSettings2.setString("azure.client.azure1.account", "myaccount1");
        // missing key
        final Settings settings2 = Settings.builder().setSecureSettings(secureSettings2).build();
        final MockSecureSettings secureSettings3 = new MockSecureSettings();
        secureSettings3.setString("azure.client.azure1.account", "myaccount3");
        secureSettings3.setString("azure.client.azure1.key", encodeKey("mykey33"));
        secureSettings3.setString("azure.client.azure1.sas_token", encodeKey("mysasToken33"));
        final Settings settings3 = Settings.builder().setSecureSettings(secureSettings3).build();
        try (AzureRepositoryPlugin plugin = pluginWithSettingsValidation(settings1)) {
            final AzureStorageService azureStorageService = plugin.azureStoreService;
            final BlobServiceClient client11 = azureStorageService.client("azure1").v1();
            assertThat(client11.getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
            final SettingsException e1 = expectThrows(SettingsException.class, () -> plugin.reload(settings2));
            assertThat(e1.getMessage(), is("Neither a secret key nor a shared access token was set."));
            final SettingsException e2 = expectThrows(SettingsException.class, () -> plugin.reload(settings3));
            assertThat(e2.getMessage(), is("Both a secret as well as a shared access token were set."));
            // existing client untouched
            assertThat(client11.getAccountUrl(), equalTo("https://myaccount1.blob.core.windows.net"));
        }
    }

    public void testGetSelectedClientNonExisting() {
        final AzureStorageService azureStorageService = storageServiceWithSettingsValidation(buildSettings());
        final SettingsException e = expectThrows(SettingsException.class, () -> azureStorageService.client("azure4"));
        assertThat(e.getMessage(), is("Unable to find client with name [azure4]"));
    }

    public void testGetSelectedClientDefaultTimeout() {
        final Settings timeoutSettings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.timeout", "30s")
            .build();
        final AzureStorageService azureStorageService = storageServiceWithSettingsValidation(timeoutSettings);
        assertThat(azureStorageService.getBlobRequestTimeout("azure1"), nullValue());
        assertThat(azureStorageService.getBlobRequestTimeout("azure3"), is(Duration.ofSeconds(30)));
    }

    public void testClientDefaultConnectTimeout() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.connect.timeout", "25s")
            .build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        final TimeValue timeout = mock.storageSettings.get("azure3").getConnectTimeout();

        assertThat(timeout, notNullValue());
        assertThat(timeout, equalTo(TimeValue.timeValueSeconds(25)));
        assertThat(mock.storageSettings.get("azure2").getConnectTimeout(), notNullValue());
        assertThat(mock.storageSettings.get("azure2").getConnectTimeout(), equalTo(TimeValue.timeValueSeconds(10)));
    }

    public void testClientDefaultWriteTimeout() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.write.timeout", "85s")
            .build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        final TimeValue timeout = mock.storageSettings.get("azure3").getWriteTimeout();

        assertThat(timeout, notNullValue());
        assertThat(timeout, equalTo(TimeValue.timeValueSeconds(85)));
        assertThat(mock.storageSettings.get("azure2").getWriteTimeout(), notNullValue());
        assertThat(mock.storageSettings.get("azure2").getWriteTimeout(), equalTo(TimeValue.timeValueSeconds(60)));
    }

    public void testClientDefaultReadTimeout() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.read.timeout", "120s")
            .build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        final TimeValue timeout = mock.storageSettings.get("azure3").getReadTimeout();

        assertThat(timeout, notNullValue());
        assertThat(timeout, equalTo(TimeValue.timeValueSeconds(120)));
        assertThat(mock.storageSettings.get("azure2").getReadTimeout(), notNullValue());
        assertThat(mock.storageSettings.get("azure2").getReadTimeout(), equalTo(TimeValue.timeValueSeconds(60)));
    }

    public void testClientDefaultResponseTimeout() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure3.response.timeout", "1ms")
            .build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        final TimeValue timeout = mock.storageSettings.get("azure3").getResponseTimeout();

        assertThat(timeout, notNullValue());
        assertThat(timeout, equalTo(TimeValue.timeValueMillis(1)));
        assertThat(mock.storageSettings.get("azure2").getResponseTimeout(), notNullValue());
        assertThat(mock.storageSettings.get("azure2").getResponseTimeout(), equalTo(TimeValue.timeValueSeconds(60)));
    }

    public void testGetSelectedClientNoTimeout() {
        final AzureStorageService azureStorageService = storageServiceWithSettingsValidation(buildSettings());
        assertThat(azureStorageService.getBlobRequestTimeout("azure1"), nullValue());
    }

    public void testGetSelectedClientBackoffPolicy() {
        final AzureStorageService azureStorageService = storageServiceWithSettingsValidation(buildSettings());
        final BlobServiceClient client1 = azureStorageService.client("azure1").v1();
        assertThat(requestRetryOptions(client1), is(notNullValue()));
    }

    public void testGetSelectedClientBackoffPolicyNbRetries() {
        final Settings timeoutSettings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.max_retries", 7)
            .build();

        final AzureStorageService azureStorageService = storageServiceWithSettingsValidation(timeoutSettings);
        final BlobServiceClient client1 = azureStorageService.client("azure1").v1();
        assertThat(requestRetryOptions(client1), is(notNullValue()));
    }

    public void testNoProxy() {
        final Settings settings = Settings.builder().setSecureSettings(buildSecureSettings()).build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        assertEquals(mock.storageSettings.get("azure1").getProxySettings(), ProxySettings.NO_PROXY_SETTINGS);
        assertEquals(mock.storageSettings.get("azure2").getProxySettings(), ProxySettings.NO_PROXY_SETTINGS);
        assertEquals(mock.storageSettings.get("azure3").getProxySettings(), ProxySettings.NO_PROXY_SETTINGS);
    }

    public void testProxyHttp() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "http")
            .build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        final ProxySettings azure1Proxy = mock.storageSettings.get("azure1").getProxySettings();

        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.getType(), is(ProxySettings.ProxyType.HTTP));
        assertThat(azure1Proxy.getAddress(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        assertEquals(ProxySettings.NO_PROXY_SETTINGS, mock.storageSettings.get("azure2").getProxySettings());
        assertEquals(ProxySettings.NO_PROXY_SETTINGS, mock.storageSettings.get("azure3").getProxySettings());
    }

    public void testMultipleProxies() throws UnknownHostException {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "http")
            .put("azure.client.azure2.proxy.host", "127.0.0.1")
            .put("azure.client.azure2.proxy.port", 8081)
            .put("azure.client.azure2.proxy.type", "http")
            .build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        final ProxySettings azure1Proxy = mock.storageSettings.get("azure1").getProxySettings();
        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.getType(), is(ProxySettings.ProxyType.HTTP));
        assertThat(azure1Proxy.getAddress(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        final ProxySettings azure2Proxy = mock.storageSettings.get("azure2").getProxySettings();
        assertThat(azure2Proxy, notNullValue());
        assertThat(azure2Proxy.getType(), is(ProxySettings.ProxyType.HTTP));
        assertThat(azure2Proxy.getAddress(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8081)));
        assertTrue(Strings.isNullOrEmpty(azure2Proxy.getUsername()));
        assertTrue(Strings.isNullOrEmpty(azure2Proxy.getPassword()));
        assertEquals(mock.storageSettings.get("azure3").getProxySettings(), ProxySettings.NO_PROXY_SETTINGS);
    }

    public void testProxySocks() throws UnknownHostException {
        final MockSecureSettings secureSettings = buildSecureSettings();
        secureSettings.setString("azure.client.azure1.proxy.username", "user");
        secureSettings.setString("azure.client.azure1.proxy.password", "pwd");
        final Settings settings = Settings.builder()
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", "socks5")
            .setSecureSettings(secureSettings)
            .build();
        final AzureStorageService mock = storageServiceWithSettingsValidation(settings);
        final ProxySettings azure1Proxy = mock.storageSettings.get("azure1").getProxySettings();
        assertThat(azure1Proxy, notNullValue());
        assertThat(azure1Proxy.getType(), is(ProxySettings.ProxyType.SOCKS5));
        assertThat(azure1Proxy.getAddress(), is(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 8080)));
        assertEquals("user", azure1Proxy.getUsername());
        assertEquals("pwd", azure1Proxy.getPassword());
        assertEquals(ProxySettings.NO_PROXY_SETTINGS, mock.storageSettings.get("azure2").getProxySettings());
        assertEquals(ProxySettings.NO_PROXY_SETTINGS, mock.storageSettings.get("azure3").getProxySettings());
    }

    public void testProxyNoHost() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.port", 8080)
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "socks4", "socks5", "http"))
            .build();
        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoPort() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "socks4", "socks5", "http"))
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testProxyNoType() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.host", "127.0.0.1")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure proxy port or host or username or password have been set but proxy type is not defined.", e.getMessage());
    }

    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .setSecureSettings(buildSecureSettings())
            .put("azure.client.azure1.proxy.type", randomFrom("socks", "socks4", "socks5", "http"))
            .put("azure.client.azure1.proxy.host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("azure.client.azure1.proxy.port", 8080)
            .build();

        final SettingsException e = expectThrows(SettingsException.class, () -> storageServiceWithSettingsValidation(settings));
        assertEquals("Azure proxy host is unknown.", e.getMessage());
    }

    public void testBlobNameFromUri() throws URISyntaxException {
        String name = blobNameFromUri(new URI("https://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://myservice.azure.net/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("http://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
        name = blobNameFromUri(new URI("https://127.0.0.1/container/path/to/myfile"));
        assertThat(name, is("path/to/myfile"));
    }

    private static MockSecureSettings buildSecureSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.azure1.account", "myaccount1");
        secureSettings.setString("azure.client.azure1.key", encodeKey("mykey1"));
        secureSettings.setString("azure.client.azure2.account", "myaccount2");
        secureSettings.setString("azure.client.azure2.key", encodeKey("mykey2"));
        secureSettings.setString("azure.client.azure3.account", "myaccount3");
        secureSettings.setString("azure.client.azure3.key", encodeKey("mykey3"));
        return secureSettings;
    }

    private static Settings buildSettings() {
        return Settings.builder().setSecureSettings(buildSecureSettings()).build();
    }

    private static String encodeKey(final String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static RequestRetryPolicy requestRetryOptions(BlobServiceClient client) {
        for (int i = 0; i < client.getHttpPipeline().getPolicyCount(); ++i) {
            final HttpPipelinePolicy policy = client.getHttpPipeline().getPolicy(i);
            if (policy instanceof RequestRetryPolicy) {
                return (RequestRetryPolicy) policy;
            }
        }

        return null;
    }

    /**
     * Extract the blob name from a URI like https://myservice.azure.net/container/path/to/myfile
     * It should remove the container part (first part of the path) and gives path/to/myfile
     * @param uri URI to parse
     * @return The blob name relative to the container
     */
    private static String blobNameFromUri(URI uri) {
        final String path = uri.getPath();
        // We remove the container name from the path
        // The 3 magic number cames from the fact if path is /container/path/to/myfile
        // First occurrence is empty "/"
        // Second occurrence is "container
        // Last part contains "path/to/myfile" which is what we want to get
        final String[] splits = path.split("/", 3);
        // We return the remaining end of the string
        return splits[2];
    }

}
