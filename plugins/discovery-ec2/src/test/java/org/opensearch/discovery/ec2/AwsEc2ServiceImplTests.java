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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import software.amazon.awssdk.core.Protocol;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AwsEc2ServiceImplTests extends AbstractEc2DiscoveryTestCase {
    public void testAwsCredentialsWithSystemProviders() {
        final AwsCredentialsProvider credentialsProvider = AwsEc2ServiceImpl.buildCredentials(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.EMPTY)
        );
        assertThat(credentialsProvider, instanceOf(AwsCredentialsProvider.class));
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

    public void testRejectionOfLoneAccessKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.access_key", "aws_key");
        SettingsException e = expectThrows(
            SettingsException.class,
            () -> AwsEc2ServiceImpl.buildCredentials(
                logger,
                Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
            )
        );
        assertThat(e.getMessage(), is("Setting [discovery.ec2.access_key] is set but [discovery.ec2.secret_key] is not"));
    }

    public void testDeprecationOfLoneSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.secret_key", "aws_secret");
        SettingsException e = expectThrows(
            SettingsException.class,
            () -> AwsEc2ServiceImpl.buildCredentials(
                logger,
                Ec2ClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
            )
        );
        assertThat(e.getMessage(), is("Setting [discovery.ec2.secret_key] is set but [discovery.ec2.access_key] is not"));
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
        // proxy configuration
        final ProxyConfiguration proxyConfiguration = AwsEc2ServiceImpl.buildProxyConfiguration(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.EMPTY)
        );

        assertNull(proxyConfiguration.scheme());
        assertNull(proxyConfiguration.host());
        assertThat(proxyConfiguration.port(), is(0));
        assertNull(proxyConfiguration.username());
        assertNull(proxyConfiguration.password());

        // retry policy
        RetryPolicy retryPolicyConfiguration = AwsEc2ServiceImpl.buildRetryPolicy(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.EMPTY)
        );

        assertThat(retryPolicyConfiguration.numRetries(), is(10));

        final AwsCredentials credentials = AwsEc2ServiceImpl.buildCredentials(logger, Ec2ClientSettings.getClientSettings(Settings.EMPTY))
            .resolveCredentials();

        assertThat(credentials.accessKeyId(), is("aws-access-key-id"));
        assertThat(credentials.secretAccessKey(), is("aws-secret-access-key"));

        ClientOverrideConfiguration clientOverrideConfiguration = AwsEc2ServiceImpl.buildOverrideConfiguration(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.EMPTY)
        );
        assertTrue(clientOverrideConfiguration.retryPolicy().isPresent());
        assertThat(clientOverrideConfiguration.retryPolicy().get().numRetries(), is(10));
    }

    public void testAWSConfigurationWithAwsSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("discovery.ec2.proxy.username", "aws_proxy_username");
        secureSettings.setString("discovery.ec2.proxy.password", "aws_proxy_password");

        final Settings settings = Settings.builder()
            .put("discovery.ec2.protocol", "http")
            // NOTE: a host cannot contain the _ character when parsed by URI, hence aws-proxy-host and not aws_proxy_host
            .put("discovery.ec2.proxy.host", "aws-proxy-host")
            .put("discovery.ec2.proxy.port", 8080)
            .put("discovery.ec2.read_timeout", "10s")
            .setSecureSettings(secureSettings)
            .build();

        // proxy configuration
        final ProxyConfiguration proxyConfiguration = AwsEc2ServiceImpl.buildProxyConfiguration(
            logger,
            Ec2ClientSettings.getClientSettings(settings)
        );

        assertThat(proxyConfiguration.scheme(), is(Protocol.HTTP.toString()));
        assertThat(proxyConfiguration.host(), is("aws-proxy-host"));
        assertThat(proxyConfiguration.port(), is(8080));
        assertThat(proxyConfiguration.username(), is("aws_proxy_username"));
        assertThat(proxyConfiguration.password(), is("aws_proxy_password"));

        // retry policy
        RetryPolicy retryPolicyConfiguration = AwsEc2ServiceImpl.buildRetryPolicy(logger, Ec2ClientSettings.getClientSettings(settings));
        assertThat(retryPolicyConfiguration.numRetries(), is(10));

        final AwsCredentials credentials = AwsEc2ServiceImpl.buildCredentials(logger, Ec2ClientSettings.getClientSettings(Settings.EMPTY))
            .resolveCredentials();

        assertThat(credentials.accessKeyId(), is("aws-access-key-id"));
        assertThat(credentials.secretAccessKey(), is("aws-secret-access-key"));

        ClientOverrideConfiguration clientOverrideConfiguration = AwsEc2ServiceImpl.buildOverrideConfiguration(
            logger,
            Ec2ClientSettings.getClientSettings(Settings.EMPTY)
        );
        assertTrue(clientOverrideConfiguration.retryPolicy().isPresent());
        assertThat(clientOverrideConfiguration.retryPolicy().get().numRetries(), is(10));
    }
}
