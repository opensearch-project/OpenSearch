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

package org.opensearch.discovery.ec2;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.Node;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;

public class Ec2DiscoveryPluginTests extends AbstractEc2DiscoveryTestCase {
    private Settings getNodeAttributes(Settings settings, String url) {
        final Settings realSettings = Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), true).put(settings).build();
        return Ec2DiscoveryPlugin.getAvailabilityZoneNodeAttributes(realSettings, url);
    }

    private void assertNodeAttributes(Settings settings, String url, String expected) {
        final Settings additional = getNodeAttributes(settings, url);
        if (expected == null) {
            assertTrue(additional.isEmpty());
        } else {
            assertEquals(expected, additional.get(Node.NODE_ATTRIBUTES.getKey() + "aws_availability_zone"));
        }
    }

    public void testNodeAttributesDisabled() {
        final Settings settings = Settings.builder().put(AwsEc2Service.AUTO_ATTRIBUTE_SETTING.getKey(), false).build();
        assertNodeAttributes(settings, "bogus", null);
    }

    public void testNodeAttributes() throws Exception {
        final Path zoneUrl = createTempFile();
        Files.write(zoneUrl, Arrays.asList("us-east-1c"));
        assertNodeAttributes(Settings.EMPTY, zoneUrl.toUri().toURL().toString(), "us-east-1c");
    }

    public void testNodeAttributesBogusUrl() {
        final UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> getNodeAttributes(Settings.EMPTY, "bogus"));
        assertNotNull(e.getCause());
        final String msg = e.getCause().getMessage();
        assertTrue(msg, msg.contains("no protocol: bogus"));
    }

    public void testNodeAttributesEmpty() throws Exception {
        final Path zoneUrl = createTempFile();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> getNodeAttributes(Settings.EMPTY, zoneUrl.toUri().toURL().toString())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("no ec2 metadata returned"));
    }

    public void testNodeAttributesErrorLenient() throws Exception {
        final Path dne = createTempDir().resolve("dne");
        assertNodeAttributes(Settings.EMPTY, dne.toUri().toURL().toString(), null);
    }

    public void testDefaultEndpoint() throws IOException {
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(Settings.EMPTY)) {
            final String endpoint = ((MockEc2Client) plugin.ec2Service.client().get()).endpoint;
            assertEquals(endpoint, "");
        }
    }

    public void testDefaultRegion() throws IOException {
        final Settings settings = Settings.builder().build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings)) {
            final String region = ((MockEc2Client) plugin.ec2Service.client().get()).region;
            assertEquals(region, "");
        }
    }

    public void testSpecificRegion() throws IOException {
        final Settings settings = Settings.builder().put(Ec2ClientSettings.REGION_SETTING.getKey(), "us-west-2").build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings)) {
            final String region = ((MockEc2Client) plugin.ec2Service.client().get()).region;
            assertEquals(region, Region.US_WEST_2.toString());
        }
    }

    public void testSpecificEndpoint() throws IOException {
        final Settings settings = Settings.builder().put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2.endpoint").build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings)) {
            final String endpoint = ((MockEc2Client) plugin.ec2Service.client().get()).endpoint;
            assertEquals(endpoint, "ec2.endpoint");
        }
    }

    public void testClientSettingsReInit() throws IOException {
        final MockSecureSettings mockSecure1 = new MockSecureSettings();
        mockSecure1.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), "ec2_access_1");
        mockSecure1.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_1");
        final boolean mockSecure1HasSessionToken = randomBoolean();
        if (mockSecure1HasSessionToken) {
            mockSecure1.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_1");
        }
        mockSecure1.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_1");
        mockSecure1.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_1");
        final Settings settings1 = Settings.builder()
            .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy-host-1")
            .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 881)
            .put(Ec2ClientSettings.REGION_SETTING.getKey(), "ec2_region")
            .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_1")
            .setSecureSettings(mockSecure1)
            .build();
        final MockSecureSettings mockSecure2 = new MockSecureSettings();
        mockSecure2.setString(Ec2ClientSettings.ACCESS_KEY_SETTING.getKey(), "ec2_access_2");
        mockSecure2.setString(Ec2ClientSettings.SECRET_KEY_SETTING.getKey(), "ec2_secret_2");
        final boolean mockSecure2HasSessionToken = randomBoolean();
        if (mockSecure2HasSessionToken) {
            mockSecure2.setString(Ec2ClientSettings.SESSION_TOKEN_SETTING.getKey(), "ec2_session_token_2");
        }
        mockSecure2.setString(Ec2ClientSettings.PROXY_USERNAME_SETTING.getKey(), "proxy_username_2");
        mockSecure2.setString(Ec2ClientSettings.PROXY_PASSWORD_SETTING.getKey(), "proxy_password_2");
        final Settings settings2 = Settings.builder()
            .put(Ec2ClientSettings.PROXY_HOST_SETTING.getKey(), "proxy-host-2")
            .put(Ec2ClientSettings.PROXY_PORT_SETTING.getKey(), 882)
            .put(Ec2ClientSettings.REGION_SETTING.getKey(), "ec2_region")
            .put(Ec2ClientSettings.ENDPOINT_SETTING.getKey(), "ec2_endpoint_2")
            .setSecureSettings(mockSecure2)
            .build();
        try (Ec2DiscoveryPluginMock plugin = new Ec2DiscoveryPluginMock(settings1)) {
            try (AmazonEc2ClientReference clientReference = plugin.ec2Service.client()) {
                {
                    final MockEc2Client mockEc2Client = (MockEc2Client) clientReference.get();
                    assertEquals(mockEc2Client.endpoint, "ec2_endpoint_1");

                    final AwsCredentials credentials = mockEc2Client.credentials.resolveCredentials();
                    assertEquals(credentials.accessKeyId(), "ec2_access_1");
                    assertEquals(credentials.secretAccessKey(), "ec2_secret_1");
                    if (mockSecure1HasSessionToken) {
                        assertThat(credentials, instanceOf(AwsSessionCredentials.class));
                        assertEquals(((AwsSessionCredentials) credentials).sessionToken(), "ec2_session_token_1");
                    } else {
                        assertThat(credentials, instanceOf(AwsBasicCredentials.class));
                    }

                    assertEquals(
                        mockEc2Client.proxyConfiguration.toString(),
                        "ProxyConfiguration(endpoint=https://proxy-host-1:881, username=proxy_username_1, preemptiveBasicAuthenticationEnabled=false)"
                    );
                    assertEquals(mockEc2Client.proxyConfiguration.host(), "proxy-host-1");
                    assertEquals(mockEc2Client.proxyConfiguration.port(), 881);
                    assertEquals(mockEc2Client.proxyConfiguration.username(), "proxy_username_1");
                    assertEquals(mockEc2Client.proxyConfiguration.password(), "proxy_password_1");
                }
                // reload secure settings2
                plugin.reload(settings2);
                // client is not released, it is still using the old settings
                {
                    final MockEc2Client mockEc2Client = (MockEc2Client) clientReference.get();
                    assertEquals(mockEc2Client.endpoint, "ec2_endpoint_1");

                    final AwsCredentials credentials = ((MockEc2Client) clientReference.get()).credentials.resolveCredentials();
                    if (mockSecure1HasSessionToken) {
                        assertThat(credentials, instanceOf(AwsSessionCredentials.class));
                        assertEquals(((AwsSessionCredentials) credentials).sessionToken(), "ec2_session_token_1");
                    } else {
                        assertThat(credentials, instanceOf(AwsBasicCredentials.class));
                    }

                    assertEquals(
                        mockEc2Client.proxyConfiguration.toString(),
                        "ProxyConfiguration(endpoint=https://proxy-host-1:881, username=proxy_username_1, preemptiveBasicAuthenticationEnabled=false)"
                    );
                    assertEquals(mockEc2Client.proxyConfiguration.host(), "proxy-host-1");
                    assertEquals(mockEc2Client.proxyConfiguration.port(), 881);
                    assertEquals(mockEc2Client.proxyConfiguration.username(), "proxy_username_1");
                    assertEquals(mockEc2Client.proxyConfiguration.password(), "proxy_password_1");
                }
            }
            try (AmazonEc2ClientReference clientReference = plugin.ec2Service.client()) {
                final MockEc2Client mockEc2Client = (MockEc2Client) clientReference.get();
                assertEquals(mockEc2Client.endpoint, "ec2_endpoint_2");

                final AwsCredentials credentials = ((MockEc2Client) clientReference.get()).credentials.resolveCredentials();
                assertEquals(credentials.accessKeyId(), "ec2_access_2");
                assertEquals(credentials.secretAccessKey(), "ec2_secret_2");
                if (mockSecure2HasSessionToken) {
                    assertThat(credentials, instanceOf(AwsSessionCredentials.class));
                    assertEquals(((AwsSessionCredentials) credentials).sessionToken(), "ec2_session_token_2");
                } else {
                    assertThat(credentials, instanceOf(AwsBasicCredentials.class));
                }

                assertEquals(
                    mockEc2Client.proxyConfiguration.toString(),
                    "ProxyConfiguration(endpoint=https://proxy-host-2:882, username=proxy_username_2, preemptiveBasicAuthenticationEnabled=false)"
                );
                assertEquals(mockEc2Client.proxyConfiguration.host(), "proxy-host-2");
                assertEquals(mockEc2Client.proxyConfiguration.port(), 882);
                assertEquals(mockEc2Client.proxyConfiguration.username(), "proxy_username_2");
                assertEquals(mockEc2Client.proxyConfiguration.password(), "proxy_password_2");
            }
        }
    }

    private static class Ec2DiscoveryPluginMock extends Ec2DiscoveryPlugin {

        Ec2DiscoveryPluginMock(Settings settings) {
            super(settings, new AwsEc2ServiceImpl() {
                @Override
                protected Ec2Client buildClient(
                    AwsCredentialsProvider credentials,
                    ProxyConfiguration proxyConfiguration,
                    ClientOverrideConfiguration overrideConfiguration,
                    String endpoint,
                    String region,
                    long readTimeoutMillis
                ) {
                    return new MockEc2Client(credentials, proxyConfiguration, overrideConfiguration, endpoint, region, readTimeoutMillis);
                }
            });
        }
    }

    private static class MockEc2Client implements Ec2Client {

        String endpoint;
        final String region;
        final AwsCredentialsProvider credentials;
        final ClientOverrideConfiguration clientOverrideConfiguration;
        final ProxyConfiguration proxyConfiguration;
        final long readTimeoutMillis;

        MockEc2Client(
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
            return "ec2";
        }

        @Override
        public void close() {
            // ignore
        }
    }
}
