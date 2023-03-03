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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.http.IdleConnectionReaper;

import org.junit.AfterClass;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

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

public class AwsS3ServiceImplTests extends OpenSearchTestCase implements ConfigPathSupport {
    @AfterClass
    public static void shutdownIdleConnectionReaper() {
        // created by default STS client
        IdleConnectionReaper.shutdown();
    }

    public void testAWSCredentialsDefaultToInstanceProviders() {
        final String inexistentClientName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(Settings.EMPTY, inexistentClientName, configPath());
        final AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, clientSettings);
        assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedInstanceProfileCredentialsProvider.class));
    }

    public void testAWSCredentialsFromKeystore() {
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
            final AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
            assertThat(credentialsProvider.getCredentials().getAWSAccessKeyId(), is(clientName + "_aws_access_key"));
            assertThat(credentialsProvider.getCredentials().getAWSSecretKey(), is(clientName + "_aws_secret_key"));
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
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
            final AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedSTSAssumeRoleSessionCredentialsProvider.class));
            ((Closeable) credentialsProvider).close();
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
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
            final AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedSTSAssumeRoleSessionCredentialsProvider.class));
            ((Closeable) credentialsProvider).close();
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
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
            final AWSCredentialsProvider credentialsProvider = S3Service.buildCredentials(logger, someClientSettings);
            assertThat(credentialsProvider, instanceOf(S3Service.PrivilegedSTSAssumeRoleSessionCredentialsProvider.class));
            ((Closeable) credentialsProvider).close();
        }
        // test default exists and is an Instance provider
        final S3ClientSettings defaultClientSettings = allClientsSettings.get("default");
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
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
        final AWSCredentialsProvider defaultCredentialsProvider = S3Service.buildCredentials(logger, defaultClientSettings);
        assertThat(defaultCredentialsProvider, instanceOf(AWSStaticCredentialsProvider.class));
        assertThat(defaultCredentialsProvider.getCredentials().getAWSAccessKeyId(), is(awsAccessKey));
        assertThat(defaultCredentialsProvider.getCredentials().getAWSSecretKey(), is(awsSecretKey));
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
        launchAWSConfigurationTest(
            Settings.EMPTY,
            Protocol.HTTPS,
            null,
            -1,
            null,
            null,
            3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
            ClientConfiguration.DEFAULT_SOCKET_TIMEOUT
        );
    }

    public void testAWSConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.proxy.host", "127.0.0.10")
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.read_timeout", "10s")
            .build();
        launchAWSConfigurationTest(
            settings,
            Protocol.HTTP,
            "127.0.0.10",
            8080,
            "aws_proxy_username",
            "aws_proxy_password",
            3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
            10000
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
            .put("s3.client.default.proxy.host", "127.0.0.10")
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.read_timeout", "10s")
            .build();
        launchAWSConfigurationTest(
            settings,
            Protocol.HTTP,
            "127.0.0.10",
            8080,
            "aws_proxy_username",
            "aws_proxy_password",
            3,
            ClientConfiguration.DEFAULT_THROTTLE_RETRIES,
            10000
        );
    }

    public void testSocksProxyConfiguration() throws IOException {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aws_proxy_username");
        secureSettings.setString("s3.client.default.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("s3.client.default.proxy.type", "socks")
            .put("s3.client.default.proxy.host", "127.0.0.10")
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.read_timeout", "10s")
            .build();

        final S3ClientSettings clientSettings = S3ClientSettings.getClientSettings(settings, "default", configPath());
        final ClientConfiguration configuration = S3Service.buildConfiguration(clientSettings);

        assertEquals(Protocol.HTTPS, configuration.getProtocol());
        assertEquals(Protocol.HTTP, configuration.getProxyProtocol()); // default value in SDK
        assertEquals(-1, configuration.getProxyPort());
        assertNull(configuration.getProxyUsername());
        assertNull(configuration.getProxyPassword());
    }

    public void testRepositoryMaxRetries() {
        final Settings settings = Settings.builder().put("s3.client.default.max_retries", 5).build();
        launchAWSConfigurationTest(settings, Protocol.HTTPS, null, -1, null, null, 5, ClientConfiguration.DEFAULT_THROTTLE_RETRIES, 50000);
    }

    public void testRepositoryThrottleRetries() {
        final boolean throttling = randomBoolean();

        final Settings settings = Settings.builder().put("s3.client.default.use_throttle_retries", throttling).build();
        launchAWSConfigurationTest(settings, Protocol.HTTPS, null, -1, null, null, 3, throttling, 50000);
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
        final ClientConfiguration configuration = S3Service.buildConfiguration(clientSettings);

        assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        assertThat(configuration.getProtocol(), is(expectedProtocol));
        assertThat(configuration.getProxyHost(), is(expectedProxyHost));
        assertThat(configuration.getProxyPort(), is(expectedProxyPort));
        assertThat(configuration.getProxyUsername(), is(expectedProxyUsername));
        assertThat(configuration.getProxyPassword(), is(expectedProxyPassword));
        assertThat(configuration.getMaxErrorRetry(), is(expectedMaxRetries));
        assertThat(configuration.useThrottledRetries(), is(expectedUseThrottleRetries));
        assertThat(configuration.getSocketTimeout(), is(expectedReadTimeout));
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
