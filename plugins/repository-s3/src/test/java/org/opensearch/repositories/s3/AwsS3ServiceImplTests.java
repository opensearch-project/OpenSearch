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

package org.opensearch.repositories.s3;

import org.junit.Before;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.s3.utils.Protocol;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.opensearch.repositories.s3.S3ClientSettings.PROTOCOL_SETTING;
import static org.opensearch.repositories.s3.S3ClientSettings.PROXY_TYPE_SETTING;

public class AwsS3ServiceImplTests extends AbstractS3RepositoryTestCase {

    private static final String HOST = "127.0.0.10";
    private static final int PORT = 8080;

    private Settings.Builder settingsBuilder;

    @Override
    @Before
    @SuppressForbidden(reason = "Need to set system property here for AWS SDK v2")
    public void setUp() throws Exception {
        settingsBuilder = Settings.builder()
            .put("s3.client.default.proxy.type", "http")
            .put("s3.client.default.proxy.host", HOST)
            .put("s3.client.default.proxy.port", PORT);
        super.setUp();
    }

    public void testAwsCredentialsDefaultToInstanceProviders() {
        final String inexistentClientName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.EMPTY, inexistentClientName, configPath());
        final AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, clientSettings);
        assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedInstanceProfileCredentialsProvider.class));
    }

    public void testAwsCredentialsFromKeystore() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String clientNamePrefix = "some_client_name_";
        final int clientsCount = randomIntBetween(0, 4);
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            secureSettings.setString("s3.client." + clientName + ".access_key", clientName + "_aws_access_key");
            secureSettings.setString("s3.client." + clientName + ".secret_key", clientName + "_aws_secret_key");
        }
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Map<String, S3ClientSettings> allClientsSettings = S3ClientSettings.load(settings, configPath());
        // no less, no more
        assertThat(allClientsSettings.size(), is(clientsCount + 1)); // including default
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            final S3ClientSettings someClientSettings = allClientsSettings.get(clientName);
            final AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(StaticCredentialsProvider.class));
            assertThat(credentialsProvider.resolveCredentials().accessKeyId(), is(clientName + "_aws_access_key"));
            assertThat(credentialsProvider.resolveCredentials().secretAccessKey(), is(clientName + "_aws_secret_key"));
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AwsCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
        assertThat(defaultCredentialsProvider, instanceOf(S3Service.PrivilegedInstanceProfileCredentialsProvider.class));
    }

    public void testCredentialsAndIrsaWithIdentityTokenFileCredentialsFromKeystore() throws IOException {
        final Map<String, String> plainSettings = new HashMap<>();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String clientNamePrefix = "some_client_name_";
        final int clientsCount = randomIntBetween(0, 4);
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            secureSettings.setString("s3.client." + clientName + ".role_arn", clientName + "_role_arn");

            // Use static AWS credentials for tests
            secureSettings.setString("s3.client." + clientName + ".access_key", clientName + "_aws_access_key");
            secureSettings.setString("s3.client." + clientName + ".secret_key", clientName + "_aws_secret_key");

            // Use explicit region setting
            plainSettings.put("s3.client." + clientName + ".region", "us-east1");
            plainSettings.put("s3.client." + clientName + ".identity_token_file", clientName + "_identity_token_file");
        }
        final Settings settings = Settings.builder().loadFromMap(plainSettings).setSecureSettings(secureSettings).build();
        final Map<String, S3ClientSettings> allClientsSettings = S3ClientSettings.load(settings, configPath());
        // no less, no more
        assertThat(allClientsSettings.size(), is(clientsCount + 1)); // including default
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            final S3ClientSettings someClientSettings = allClientsSettings.get(clientName);
            final AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedSTSAssumeRoleSessionCredentialsProvider.class));
            ((Closeable) credentialsProvider).close();
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AwsCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
        assertThat(defaultCredentialsProvider, instanceOf(S3Service.PrivilegedInstanceProfileCredentialsProvider.class));
    }

    public void testCredentialsAndIrsaCredentialsFromKeystore() throws IOException {
        final Map<String, String> plainSettings = new HashMap<>();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String clientNamePrefix = "some_client_name_";
        final int clientsCount = randomIntBetween(0, 4);
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            secureSettings.setString("s3.client." + clientName + ".role_arn", clientName + "_role_arn");
            secureSettings.setString("s3.client." + clientName + ".role_session_name", clientName + "_role_session_name");

            // Use static AWS credentials for tests
            secureSettings.setString("s3.client." + clientName + ".access_key", clientName + "_aws_access_key");
            secureSettings.setString("s3.client." + clientName + ".secret_key", clientName + "_aws_secret_key");

            // Use explicit region setting
            plainSettings.put("s3.client." + clientName + ".region", "us-east1");
        }
        final Settings settings = Settings.builder().loadFromMap(plainSettings).setSecureSettings(secureSettings).build();
        final Map<String, S3ClientSettings> allClientsSettings = S3ClientSettings.load(settings, configPath());
        // no less, no more
        assertThat(allClientsSettings.size(), is(clientsCount + 1)); // including default
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            final S3ClientSettings someClientSettings = allClientsSettings.get(clientName);
            final AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedSTSAssumeRoleSessionCredentialsProvider.class));
            ((Closeable) credentialsProvider).close();
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AwsCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
        assertThat(defaultCredentialsProvider, instanceOf(S3Service.PrivilegedInstanceProfileCredentialsProvider.class));
    }

    public void testIrsaCredentialsFromKeystore() throws IOException {
        final Map<String, String> plainSettings = new HashMap<>();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String clientNamePrefix = "some_client_name_";
        final int clientsCount = randomIntBetween(0, 4);
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            secureSettings.setString("s3.client." + clientName + ".role_arn", clientName + "_role_arn");
            secureSettings.setString("s3.client." + clientName + ".role_session_name", clientName + "_role_session_name");
        }
        final Settings settings = Settings.builder().loadFromMap(plainSettings).setSecureSettings(secureSettings).build();
        final Map<String, S3ClientSettings> allClientsSettings = S3ClientSettings.load(settings, configPath());
        // no less, no more
        assertThat(allClientsSettings.size(), is(clientsCount + 1)); // including default
        for (int i = 0; i < clientsCount; i++) {
            final String clientName = clientNamePrefix + i;
            final S3ClientSettings someClientSettings = allClientsSettings.get(clientName);
            final AwsCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedSTSAssumeRoleSessionCredentialsProvider.class));
            ((Closeable) credentialsProvider).close();
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AwsCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
        assertThat(defaultCredentialsProvider, instanceOf(S3Service.PrivilegedInstanceProfileCredentialsProvider.class));
    }

    public void testSetDefaultCredential() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String awsAccessKey = randomAlphaOfLength(8);
        final String awsSecretKey = randomAlphaOfLength(8);
        secureSettings.setString("s3.client.default.access_key", awsAccessKey);
        secureSettings.setString("s3.client.default.secret_key", awsSecretKey);
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Map<String, S3ClientSettings> allClientsSettings = S3ClientSettings.load(settings, configPath());
        assertThat(allClientsSettings.size(), is(1));
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AwsCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
        assertThat(defaultCredentialsProvider, instanceOf(StaticCredentialsProvider.class));
        assertThat(defaultCredentialsProvider.resolveCredentials().accessKeyId(), is(awsAccessKey));
        assertThat(defaultCredentialsProvider.resolveCredentials().secretAccessKey(), is(awsSecretKey));
    }

    public void testCredentialsIncomplete() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        final String clientName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final boolean missingOrMissing = randomBoolean();
        if (missingOrMissing) {
            secureSettings.setString("s3.client." + clientName + ".access_key", "aws_access_key");
        } else {
            secureSettings.setString("s3.client." + clientName + ".secret_key", "aws_secret_key");
        }
        final Settings settings = Settings.builder().setSecureSettings(secureSettings).build();
        final Exception e = expectThrows(IllegalArgumentException.class, () -> S3ClientSettings.load(settings, configPath()));
        if (missingOrMissing) {
            assertThat(e.getMessage(), containsString("Missing secret key for s3 client [" + clientName + "]"));
        } else {
            assertThat(e.getMessage(), containsString("Missing access key for s3 client [" + clientName + "]"));
        }
    }

    public void testAWSDefaultConfiguration() {
        SocketAccess.doPrivilegedVoid(
            () -> launchAWSConfigurationTest(Settings.EMPTY, Protocol.HTTPS, null, -1, null, null, 3, true, 50_000)
        );
    }

    public void testAWSConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.proxy.host", HOST)
            .put("s3.client.default.proxy.port", PORT)
            .put("s3.client.default.read_timeout", "10s")
            .build();
        SocketAccess.doPrivilegedVoid(
            () -> launchAWSConfigurationTest(
                settings,
                Protocol.HTTP,
                HOST,
                PORT,
                "aws_proxy_username",
                "aws_proxy_password",
                3,
                true,
                10000
            )
        );
        assertWarnings(
            "Using of "
                + PROTOCOL_SETTING.getConcreteSettingForNamespace("default").getKey()
                + " as proxy type is deprecated and will be removed in future releases. Please use "
                + PROXY_TYPE_SETTING.getConcreteSettingForNamespace("default").getKey()
                + " instead to specify proxy type."
        );
    }

    public void testProxyTypeOverrideProtocolSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.proxy.type", "https")
            .put("s3.client.default.proxy.host", HOST)
            .put("s3.client.default.proxy.port", PORT)
            .put("s3.client.default.read_timeout", "10s")
            .build();
        SocketAccess.doPrivilegedVoid(
            () -> launchAWSConfigurationTest(
                settings,
                Protocol.HTTP,
                HOST,
                PORT,
                "aws_proxy_username",
                "aws_proxy_password",
                3,
                true,
                10000
            )
        );
    }

    public void testSocksProxyConfiguration() throws IOException {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("s3.client.default.proxy.type", "socks")
            .put("s3.client.default.proxy.host", HOST)
            .put("s3.client.default.proxy.port", PORT)
            .put("s3.client.default.read_timeout", "10s")
            .build();

        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, "default", configPath());
        final ProxyConfiguration configuration = S3Service.buildHttpProxyConfiguration(clientSettings);

        assertEquals(0, configuration.port());
        assertNull(configuration.username());
        assertNull(configuration.password());
    }

    public void testRepositoryMaxRetries() {
        final Settings settings = settingsBuilder.put("s3.client.default.max_retries", 5).build();
        SocketAccess.doPrivilegedVoid(() -> launchAWSConfigurationTest(settings, Protocol.HTTPS, HOST, PORT, "", "", 5, true, 50000));
    }

    public void testRepositoryThrottleRetries() {
        final boolean throttling = randomBoolean();

        final Settings settings = settingsBuilder.put("s3.client.default.use_throttle_retries", throttling).build();
        SocketAccess.doPrivilegedVoid(() -> launchAWSConfigurationTest(settings, Protocol.HTTPS, HOST, PORT, "", "", 3, throttling, 50000));
    }

    private void launchAWSConfigurationTest(
        Settings settings,
        Protocol expectedProtocol,
        String expectedProxyHost,
        int expectedProxyPort,
        String expectedProxyUsername,
        String expectedProxyPassword,
        Integer expectedMaxRetries,
        boolean expectedUseThrottleRetries,
        int expectedReadTimeout
    ) {

        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, "default", configPath());
        if (clientSettings.proxySettings != ProxySettings.NO_PROXY_SETTINGS) {
            final ProxyConfiguration proxyConfiguration = S3Service.buildHttpProxyConfiguration(clientSettings);
            assertThat(proxyConfiguration.host(), is(expectedProxyHost));
            assertThat(proxyConfiguration.port(), is(expectedProxyPort));
            assertThat(proxyConfiguration.username(), is(expectedProxyUsername));
            assertThat(proxyConfiguration.password(), is(expectedProxyPassword));
        }

        final ClientOverrideConfiguration clientOverrideConfiguration = S3Service.buildOverrideConfiguration(clientSettings);

        assertTrue(clientOverrideConfiguration.retryPolicy().isPresent());
        assertThat(clientOverrideConfiguration.retryPolicy().get().numRetries(), is(expectedMaxRetries));
        if (expectedUseThrottleRetries) {
            assertThat(
                clientOverrideConfiguration.retryPolicy().get().throttlingBackoffStrategy(),
                is(BackoffStrategy.defaultThrottlingStrategy())
            );
        } else {
            assertThat(clientOverrideConfiguration.retryPolicy().get().throttlingBackoffStrategy(), is(BackoffStrategy.none()));
        }
        // assertThat(proxyConfiguration.getSocketTimeout(), is(expectedReadTimeout));
    }

    public void testEndpointSetting() {
        final Settings settings = Settings.builder().put("s3.client.default.endpoint", "s3.endpoint").build();
        assertEndpoint(Settings.EMPTY, settings, "s3.endpoint");
    }

    private void assertEndpoint(Settings repositorySettings, Settings settings, String expectedEndpoint) {
        final String configName = S3Repository.CLIENT_NAME.get(repositorySettings);
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, configName, configPath());
        assertThat(clientSettings.endpoint, is(expectedEndpoint));
    }
}
