/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.kms;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.services.kms.KmsClient;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;

public class KmsServiceTests extends AbstractAwsTestCase {
    private final CryptoMetadata cryptoMetadata = new CryptoMetadata("kp1", "kp2", Settings.EMPTY);

    public void testAWSDefaultConfiguration() {
        try (KmsService kmsService = new KmsService()) {
            // proxy configuration
            final ProxyConfiguration proxyConfiguration = kmsService.buildProxyConfiguration(
                KmsClientSettings.getClientSettings(Settings.EMPTY)
            );

            assertEquals("http", proxyConfiguration.scheme());
            assertNull(proxyConfiguration.host());
            assertEquals(proxyConfiguration.port(), 0);
            assertNull(proxyConfiguration.username());
            assertNull(proxyConfiguration.password());

            // retry policy
            RetryPolicy retryPolicyConfiguration = SocketAccess.doPrivileged(kmsService::buildRetryPolicy);

            assertEquals(retryPolicyConfiguration.numRetries().intValue(), 10);

            ClientOverrideConfiguration clientOverrideConfiguration = SocketAccess.doPrivileged(kmsService::buildOverrideConfiguration);
            assertTrue(clientOverrideConfiguration.retryPolicy().isPresent());
            assertEquals(clientOverrideConfiguration.retryPolicy().get().numRetries().intValue(), 10);
        }
    }

    public void testAWSConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("kms.proxy.username", "aws_proxy_username");
        secureSettings.setString("kms.proxy.password", "aws_proxy_password");

        final Settings settings = Settings.builder()
            // NOTE: a host cannot contain the _ character when parsed by URI, hence aws-proxy-host and not aws_proxy_host
            .put("kms.proxy.host", "aws-proxy-host")
            .put("kms.proxy.port", 8080)
            .put("kms.read_timeout", "10s")
            .setSecureSettings(secureSettings)
            .build();

        try (KmsService kmsService = new KmsService()) {
            // proxy configuration
            final ProxyConfiguration proxyConfiguration = SocketAccess.doPrivileged(
                () -> kmsService.buildProxyConfiguration(KmsClientSettings.getClientSettings(settings))
            );

            assertEquals(proxyConfiguration.host(), "aws-proxy-host");
            assertEquals(proxyConfiguration.port(), 8080);
            assertEquals(proxyConfiguration.username(), "aws_proxy_username");
            assertEquals(proxyConfiguration.password(), "aws_proxy_password");

            // retry policy
            RetryPolicy retryPolicyConfiguration = SocketAccess.doPrivileged(kmsService::buildRetryPolicy);
            assertEquals(retryPolicyConfiguration.numRetries().intValue(), 10);

            ClientOverrideConfiguration clientOverrideConfiguration = SocketAccess.doPrivileged(kmsService::buildOverrideConfiguration);
            assertTrue(clientOverrideConfiguration.retryPolicy().isPresent());
            assertEquals(clientOverrideConfiguration.retryPolicy().get().numRetries().intValue(), 10);
        }
    }

    public void testClientSettingsReInit() {
        final MockSecureSettings mockSecure1 = new MockSecureSettings();
        mockSecure1.setString(KmsClientSettings.ACCESS_KEY_SETTING.getKey(), "kms_access_1");
        mockSecure1.setString(KmsClientSettings.SECRET_KEY_SETTING.getKey(), "kms_secret_1");
        final boolean mockSecure1HasSessionToken = randomBoolean();
        if (mockSecure1HasSessionToken) {
            mockSecure1.setString(KmsClientSettings.SESSION_TOKEN_SETTING.getKey(), "kms_session_token_1");
        }
        mockSecure1.setString(KmsClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_1");
        mockSecure1.setString(KmsClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_1");
        final Settings settings1 = Settings.builder()
            .put(KmsClientSettings.PROXY_HOST_SETTING.getKey(), "proxy-host-1")
            .put(KmsClientSettings.PROXY_PORT_SETTING.getKey(), 881)
            .put(KmsClientSettings.REGION_SETTING.getKey(), "kms_region")
            .put(KmsClientSettings.ENDPOINT_SETTING.getKey(), "kms_endpoint_1")
            .setSecureSettings(mockSecure1)
            .build();
        final MockSecureSettings mockSecure2 = new MockSecureSettings();
        mockSecure2.setString(KmsClientSettings.ACCESS_KEY_SETTING.getKey(), "kms_access_2");
        mockSecure2.setString(KmsClientSettings.SECRET_KEY_SETTING.getKey(), "kms_secret_2");
        final boolean mockSecure2HasSessionToken = randomBoolean();
        if (mockSecure2HasSessionToken) {
            mockSecure2.setString(KmsClientSettings.SESSION_TOKEN_SETTING.getKey(), "kms_session_token_2");
        }
        mockSecure2.setString(KmsClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_2");
        mockSecure2.setString(KmsClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_2");
        final Settings settings2 = Settings.builder()
            .put(KmsClientSettings.PROXY_HOST_SETTING.getKey(), "proxy-host-2")
            .put(KmsClientSettings.PROXY_PORT_SETTING.getKey(), 882)
            .put(KmsClientSettings.REGION_SETTING.getKey(), "kms_region")
            .put(KmsClientSettings.ENDPOINT_SETTING.getKey(), "kms_endpoint_2")
            .setSecureSettings(mockSecure2)
            .build();
        try (CryptoKmsPluginMockTest plugin = new CryptoKmsPluginMockTest(settings1)) {
            try (AmazonKmsClientReference clientReference = plugin.kmsService.client(cryptoMetadata)) {
                {
                    final MockKmsClientTest mockKmsClientTest = (MockKmsClientTest) clientReference.get();
                    assertEquals(mockKmsClientTest.endpoint, "kms_endpoint_1");

                    final AwsCredentials credentials = mockKmsClientTest.credentials.resolveCredentials();
                    assertEquals(credentials.accessKeyId(), "kms_access_1");
                    assertEquals(credentials.secretAccessKey(), "kms_secret_1");
                    if (mockSecure1HasSessionToken) {
                        assertTrue(credentials instanceof AwsSessionCredentials);
                        assertEquals(((AwsSessionCredentials) credentials).sessionToken(), "kms_session_token_1");
                    } else {
                        assertTrue(credentials instanceof AwsBasicCredentials);
                    }

                    assertEquals(mockKmsClientTest.proxyConfiguration.host(), "proxy-host-1");
                    assertEquals(mockKmsClientTest.proxyConfiguration.port(), 881);
                    assertEquals(mockKmsClientTest.proxyConfiguration.username(), "proxy_username_1");
                    assertEquals(mockKmsClientTest.proxyConfiguration.password(), "proxy_password_1");
                    assertFalse(mockKmsClientTest.proxyConfiguration.preemptiveBasicAuthenticationEnabled());
                }
                // reload secure settings2
                plugin.reload(settings2);
                // client is not released, it is still using the old settings
                {
                    final MockKmsClientTest mockKmsClientTest = (MockKmsClientTest) clientReference.get();
                    assertEquals(mockKmsClientTest.endpoint, "kms_endpoint_1");

                    final AwsCredentials credentials = ((MockKmsClientTest) clientReference.get()).credentials.resolveCredentials();
                    if (mockSecure1HasSessionToken) {
                        assertTrue(credentials instanceof AwsSessionCredentials);
                        assertEquals(((AwsSessionCredentials) credentials).sessionToken(), "kms_session_token_1");
                    } else {
                        assertTrue(credentials instanceof AwsBasicCredentials);
                    }

                    assertEquals(mockKmsClientTest.proxyConfiguration.host(), "proxy-host-1");
                    assertEquals(mockKmsClientTest.proxyConfiguration.port(), 881);
                    assertEquals(mockKmsClientTest.proxyConfiguration.username(), "proxy_username_1");
                    assertEquals(mockKmsClientTest.proxyConfiguration.password(), "proxy_password_1");
                    assertFalse(mockKmsClientTest.proxyConfiguration.preemptiveBasicAuthenticationEnabled());
                }
            }
            try (AmazonKmsClientReference clientReference = plugin.kmsService.client(cryptoMetadata)) {
                final MockKmsClientTest mockKmsClientTest = (MockKmsClientTest) clientReference.get();
                assertEquals(mockKmsClientTest.endpoint, "kms_endpoint_2");

                final AwsCredentials credentials = ((MockKmsClientTest) clientReference.get()).credentials.resolveCredentials();
                assertEquals(credentials.accessKeyId(), "kms_access_2");
                assertEquals(credentials.secretAccessKey(), "kms_secret_2");
                if (mockSecure2HasSessionToken) {
                    assertTrue(credentials instanceof AwsSessionCredentials);
                    assertEquals(((AwsSessionCredentials) credentials).sessionToken(), "kms_session_token_2");
                } else {
                    assertTrue(credentials instanceof AwsBasicCredentials);
                }

                assertEquals(mockKmsClientTest.proxyConfiguration.host(), "proxy-host-2");
                assertEquals(mockKmsClientTest.proxyConfiguration.port(), 882);
                assertEquals(mockKmsClientTest.proxyConfiguration.username(), "proxy_username_2");
                assertEquals(mockKmsClientTest.proxyConfiguration.password(), "proxy_password_2");
                assertFalse(mockKmsClientTest.proxyConfiguration.preemptiveBasicAuthenticationEnabled());
            }
        }
    }

    static class CryptoKmsPluginMockTest extends CryptoKmsPlugin {

        CryptoKmsPluginMockTest(Settings settings) {
            super(settings, new KmsService() {
                @Override
                protected KmsClient buildClient(
                    AwsCredentialsProvider credentials,
                    ProxyConfiguration proxyConfiguration,
                    ClientOverrideConfiguration overrideConfiguration,
                    String endpoint,
                    String region,
                    long readTimeoutMillis
                ) {
                    return new MockKmsClientTest(
                        credentials,
                        proxyConfiguration,
                        overrideConfiguration,
                        endpoint,
                        region,
                        readTimeoutMillis
                    );
                }
            });
        }
    }

    static class MockKmsClientTest implements KmsClient {

        String endpoint;
        final String region;
        final AwsCredentialsProvider credentials;
        final ClientOverrideConfiguration clientOverrideConfiguration;
        final ProxyConfiguration proxyConfiguration;
        final long readTimeoutMillis;

        MockKmsClientTest(
            AwsCredentialsProvider credentials,
            ProxyConfiguration proxyConfiguration,
            ClientOverrideConfiguration clientOverrideConfiguration,
            String endpoint,
            String region,
            long readTimeoutMillis
        ) {
            this.credentials = credentials;
            this.proxyConfiguration = proxyConfiguration;
            this.clientOverrideConfiguration = clientOverrideConfiguration;
            this.endpoint = endpoint;
            this.region = region;
            this.readTimeoutMillis = readTimeoutMillis;
        }

        @Override
        public String serviceName() {
            return "kms";
        }

        @Override
        public void close() {
            // ignore
        }
    }
}
