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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.test.OpenSearchTestCase;
import software.amazon.awssdk.core.Protocol;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AwsEc2ServiceImplTests extends OpenSearchTestCase {

    public void testAwsCredentialsWithSystemProviders() {
        final AwsCredentialsProvider credentialsProvider = AwsEc2ServiceImpl.buildCredentials(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.EMPTY)
        );
        assertThat(credentialsProvider, instanceOf(AwsCredentialsProviderChain.class));
    }

    public void testAwsCredentialsWithOpenSearchAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        final AwsCredentials credentials = AwsEc2ServiceImpl.buildCredentials(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
        ).resolveCredentials();
        assertThat(credentials.accessKeyId(), is("aws_key"));
        assertThat(credentials.secretAccessKey(), is("aws_secret"));
    }

    public void testAWSSessionCredentialsWithOpenSearchAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        secureSettings.setString("discovery.ec2.session_token", "aws_session_token");
        final AwsSessionCredentials credentials = (AwsSessionCredentials) AwsEc2ServiceImpl.buildCredentials(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
        ).resolveCredentials();
        assertThat(credentials.accessKeyId(), is("aws_key"));
        assertThat(credentials.secretAccessKey(), is("aws_secret"));
        assertThat(credentials.sessionToken(), is("aws_session_token"));
    }

    public void testDeprecationOfLoneAccessKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        final AwsCredentials credentials = AwsEc2ServiceImpl.buildCredentials(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
        ).resolveCredentials();
        assertThat(credentials.accessKeyId(), is("aws_key"));
        assertThat(credentials.secretAccessKey(), is(""));
        assertSettingDeprecationsAndWarnings(
            new String[] {},
            "Setting [discovery.ec2.access_key] is set but [discovery.ec2.secret_key] is not, which will be unsupported in future"
        );
    }

    public void testDeprecationOfLoneSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        final AwsCredentials credentials = AwsEc2ServiceImpl.buildCredentials(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
        ).resolveCredentials();
        assertThat(credentials.accessKeyId(), is(""));
        assertThat(credentials.secretAccessKey(), is("aws_secret"));
        assertSettingDeprecationsAndWarnings(
            new String[] {},
            "Setting [discovery.ec2.secret_key] is set but [discovery.ec2.access_key] is not, which will be unsupported in future"
        );
    }

    public void testRejectionOfLoneSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.session_token", "aws_session_token");
        SettingsException e = expectThrows(
            SettingsException.class,
            () -> AwsEc2ServiceImpl.buildCredentials(
                logger,
                Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
            )
        );
        assertThat(
            e.getMessage(),
            is("Setting [discovery.ec2.session_token] is set but [discovery.ec2.access_key] and [discovery.ec2.secret_key] are not")
        );
    }

    public void testAWSDefaultConfiguration() {
        testConfiguration(Settings.EMPTY, Protocol.HTTPS, null, -1, null, null, 50 * 1000);
    }

    public void testAWSConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.proxy.username", "aws_proxy_username");
        secureSettings.setString("discovery.ec2.proxy.password", "aws_proxy_password");
        final Settings settings = Settings.builder()
            .put("discovery.ec2.protocol", "http")
            .put("discovery.ec2.proxy.host", "aws_proxy_host")
            .put("discovery.ec2.proxy.port", 8080)
            .put("discovery.ec2.read_timeout", "10s")
            .setSecureSettings(secureSettings)
            .build();
        testConfiguration(settings, Protocol.HTTP, "aws_proxy_host", 8080, "aws_proxy_username", "aws_proxy_password", 10000);
    }

    protected void testConfiguration(
        Settings settings,
        Protocol expectedProtocol,
        String expectedProxyHost,
        int expectedProxyPort,
        String expectedProxyUsername,
        String expectedProxyPassword,
        int expectedReadTimeout
    ) {
        final ProxyConfiguration proxyConfiguration = AwsEc2ServiceImpl.buildProxyConfiguration(
            logger,
            Ec2ClientSettings.getClientSettings(settings)
        );
        assertThat(proxyConfiguration.host(), is(expectedProxyHost));
        assertThat(proxyConfiguration.port(), is(expectedProxyPort));
        assertThat(proxyConfiguration.username(), is(expectedProxyUsername));
        assertThat(proxyConfiguration.password(), is(expectedProxyPassword));

        // final ApacheHttpClient.Builder clientBuilder = AwsEc2ServiceImpl.buildHttpClient(logger,
        // Ec2ClientSettings.getClientSettings(settings));

        // assertThat(configuration.getResponseMetadataCacheSize(), is(0));
        // assertThat(configuration.getProtocol(), is(expectedProtocol));
        // assertThat(configuration.getSocketTimeout(), is(expectedReadTimeout));
    }

}
