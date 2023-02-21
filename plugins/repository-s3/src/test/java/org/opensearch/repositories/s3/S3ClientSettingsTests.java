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
import com.amazonaws.services.s3.AmazonS3Client;

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class S3ClientSettingsTests extends OpenSearchTestCase implements ConfigPathSupport {
    public void testThereIsADefaultClientByDefault() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.EMPTY, configPath());
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.credentials, nullValue());
        assertThat(defaultSettings.endpoint, is(emptyString()));
        assertThat(defaultSettings.protocol, is(Protocol.HTTPS));
        assertThat(defaultSettings.proxySettings, is(ProxySettings.NO_PROXY_SETTINGS));
        assertThat(defaultSettings.readTimeoutMillis, is(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT));
        assertThat(defaultSettings.maxRetries, is(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry()));
        assertThat(defaultSettings.throttleRetries, is(ClientConfiguration.DEFAULT_THROTTLE_RETRIES));
    }

    public void testDefaultClientSettingsCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.default.max_retries", 10).build(),
            configPath()
        );
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(10));
    }

    public void testNondefaultClientCreatedBySettingItsSettings() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.another_client.max_retries", 10).build(),
            configPath()
        );
        assertThat(settings.keySet(), contains("default", "another_client"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.maxRetries, is(ClientConfiguration.DEFAULT_RETRY_POLICY.getMaxErrorRetry()));

        final S3ClientSettings anotherClientSettings = settings.get("another_client");
        assertThat(anotherClientSettings.maxRetries, is(10));
    }

    public void testRejectionOfLoneAccessKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "aws_key");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build(), configPath())
        );
        assertThat(e.getMessage(), is("Missing secret key for s3 client [default]"));
    }

    public void testRejectionOfLoneSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.secret_key", "aws_key");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build(), configPath())
        );
        assertThat(e.getMessage(), is("Missing access key for s3 client [default]"));
    }

    public void testRejectionOfLoneSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.session_token", "aws_key");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3ClientSettings.load(Settings.builder().setSecureSettings(secureSettings).build(), configPath())
        );
        assertThat(e.getMessage(), is("Missing access key and secret key for s3 client [default]"));
    }

    public void testIrsaCredentialsTypeWithIdentityTokenFile() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.default.identity_token_file", "file").build(),
            configPath()
        );
        final S3ClientSettings defaultSettings = settings.get("default");
        final S3ClientSettings.IrsaCredentials credentials = defaultSettings.irsaCredentials;
        assertThat(credentials.getIdentityTokenFile(), is("config/file"));
        assertThat(credentials.getRoleArn(), is(nullValue()));
        assertThat(credentials.getRoleSessionName(), startsWith("s3-sdk-java-"));
    }

    public void testIrsaCredentialsTypeRoleArn() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.role_arn", "role");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).build(),
            configPath()
        );
        final S3ClientSettings defaultSettings = settings.get("default");
        final S3ClientSettings.IrsaCredentials credentials = defaultSettings.irsaCredentials;
        assertThat(credentials.getRoleArn(), is("role"));
        assertThat(credentials.getRoleSessionName(), startsWith("s3-sdk-java-"));
    }

    public void testIrsaCredentialsTypeWithRoleArnAndRoleSessionName() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.role_arn", "role");
        secureSettings.setString("s3.client.default.role_session_name", "session");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).build(),
            configPath()
        );
        final S3ClientSettings defaultSettings = settings.get("default");
        final S3ClientSettings.IrsaCredentials credentials = defaultSettings.irsaCredentials;
        assertThat(credentials.getRoleArn(), is("role"));
        assertThat(credentials.getRoleSessionName(), is("session"));
    }

    public void testIrsaCredentialsTypeWithRoleArnAndRoleSessionNameAndIdentityTokeFileRelative() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.role_arn", "role");
        secureSettings.setString("s3.client.default.role_session_name", "session");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).put("s3.client.default.identity_token_file", "file").build(),
            configPath()
        );
        final S3ClientSettings defaultSettings = settings.get("default");
        final S3ClientSettings.IrsaCredentials credentials = defaultSettings.irsaCredentials;
        assertThat(credentials.getIdentityTokenFile(), is("config/file"));
        assertThat(credentials.getRoleArn(), is("role"));
        assertThat(credentials.getRoleSessionName(), is("session"));
    }

    public void testIrsaCredentialsTypeWithRoleArnAndRoleSessionNameAndIdentityTokeFileAbsolute() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.role_arn", "role");
        secureSettings.setString("s3.client.default.role_session_name", "session");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).put("s3.client.default.identity_token_file", "/file").build(),
            configPath()
        );
        final S3ClientSettings defaultSettings = settings.get("default");
        final S3ClientSettings.IrsaCredentials credentials = defaultSettings.irsaCredentials;
        assertThat(credentials.getIdentityTokenFile(), is("/file"));
        assertThat(credentials.getRoleArn(), is("role"));
        assertThat(credentials.getRoleSessionName(), is("session"));
    }

    public void testCredentialsTypeWithAccessKeyAndSecretKey() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).build(),
            configPath()
        );
        final S3ClientSettings defaultSettings = settings.get("default");
        S3BasicCredentials credentials = defaultSettings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
    }

    public void testCredentialsTypeWithAccessKeyAndSecretKeyAndSessionToken() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        secureSettings.setString("s3.client.default.session_token", "session_token");
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).build(),
            configPath()
        );
        final S3ClientSettings defaultSettings = settings.get("default");
        S3BasicSessionCredentials credentials = (S3BasicSessionCredentials) defaultSettings.credentials;
        assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
        assertThat(credentials.getAWSSecretKey(), is("secret_key"));
        assertThat(credentials.getSessionToken(), is("session_token"));
    }

    public void testRefineWithRepoSettings() {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "access_key");
        secureSettings.setString("s3.client.default.secret_key", "secret_key");
        secureSettings.setString("s3.client.default.session_token", "session_token");
        final S3ClientSettings baseSettings = S3ClientSettings.load(
            Settings.builder().setSecureSettings(secureSettings).build(),
            configPath()
        ).get("default");

        {
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.EMPTY);
            assertSame(refinedSettings, baseSettings);
        }

        {
            final String endpoint = "some.host";
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.builder().put("endpoint", endpoint).build());
            assertThat(refinedSettings.endpoint, is(endpoint));
            S3BasicSessionCredentials credentials = (S3BasicSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
            assertThat(credentials.getAWSSecretKey(), is("secret_key"));
            assertThat(credentials.getSessionToken(), is("session_token"));
        }

        {
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.builder().put("path_style_access", true).build());
            assertThat(refinedSettings.pathStyleAccess, is(true));
            S3BasicSessionCredentials credentials = (S3BasicSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.getAWSAccessKeyId(), is("access_key"));
            assertThat(credentials.getAWSSecretKey(), is("secret_key"));
            assertThat(credentials.getSessionToken(), is("session_token"));
        }
    }

    public void testPathStyleAccessCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.path_style_access", true).build(),
            configPath()
        );
        assertThat(settings.get("default").pathStyleAccess, is(false));
        assertThat(settings.get("other").pathStyleAccess, is(true));
    }

    public void testUseChunkedEncodingCanBeSet() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.disable_chunked_encoding", true).build(),
            configPath()
        );
        assertThat(settings.get("default").disableChunkedEncoding, is(false));
        assertThat(settings.get("other").disableChunkedEncoding, is(true));
    }

    public void testRegionCanBeSet() {
        final String region = randomAlphaOfLength(5);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.region", region).build(),
            configPath()
        );
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").region, is(region));
        try (S3Service s3Service = new S3Service(configPath())) {
            AmazonS3Client other = (AmazonS3Client) s3Service.buildClient(settings.get("other")).client();
            assertThat(other.getSignerRegionOverride(), is(region));
        }
    }

    public void testSignerOverrideCanBeSet() {
        final String signerOverride = randomAlphaOfLength(5);
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.signer_override", signerOverride).build(),
            configPath()
        );
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").signerOverride, is(signerOverride));
        ClientConfiguration defaultConfiguration = S3Service.buildConfiguration(settings.get("default"));
        assertThat(defaultConfiguration.getSignerOverride(), nullValue());
        ClientConfiguration configuration = S3Service.buildConfiguration(settings.get("other"));
        assertThat(configuration.getSignerOverride(), is(signerOverride));
    }

    public void testSetProxySettings() throws Exception {
        final int port = randomIntBetween(10, 1080);
        final String userName = randomAlphaOfLength(10);
        final String password = randomAlphaOfLength(10);
        final String proxyType = randomFrom("http", "https", "socks");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", userName);
        secureSettings.setString("s3.client.default.proxy.password", password);

        final Settings settings = Settings.builder()
            .put("s3.client.default.proxy.type", proxyType)
            .put("s3.client.default.proxy.host", randomFrom("127.0.0.10"))
            .put("s3.client.default.proxy.port", randomFrom(port))
            .setSecureSettings(secureSettings)
            .build();

        final S3ClientSettings s3ClientSettings = S3ClientSettings.load(settings, configPath()).get("default");

        assertEquals(ProxySettings.ProxyType.valueOf(proxyType.toUpperCase(Locale.ROOT)), s3ClientSettings.proxySettings.getType());
        assertEquals(new InetSocketAddress(InetAddress.getByName("127.0.0.10"), port), s3ClientSettings.proxySettings.getAddress());
        assertEquals(userName, s3ClientSettings.proxySettings.getUsername());
        assertEquals(password, s3ClientSettings.proxySettings.getPassword());
    }

    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .put("s3.client.default.proxy.type", randomFrom("socks", "http"))
            .put("s3.client.default.proxy.host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("s3.client.default.proxy.port", 8080)
            .build();
        final SettingsException e = expectThrows(SettingsException.class, () -> S3ClientSettings.load(settings, configPath()));
        assertEquals("S3 proxy host is unknown.", e.getMessage());
    }

    public void testProxyTypeNotSet() {
        final Settings hostPortSettings = Settings.builder()
            .put("s3.client.default.proxy.host", "127.0.0.1")
            .put("s3.client.default.proxy.port", 8080)
            .build();

        SettingsException e = expectThrows(SettingsException.class, () -> S3ClientSettings.load(hostPortSettings, configPath()));
        assertEquals("S3 proxy port or host or username or password have been set but proxy type is not defined.", e.getMessage());

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.proxy.username", "aaaa");
        secureSettings.setString("s3.client.default.proxy.password", "bbbb");
        final Settings usernamePasswordSettings = Settings.builder().setSecureSettings(secureSettings).build();

        e = expectThrows(SettingsException.class, () -> S3ClientSettings.load(usernamePasswordSettings, configPath()));
        assertEquals("S3 proxy port or host or username or password have been set but proxy type is not defined.", e.getMessage());
    }

    public void testProxyHostNotSet() {
        final Settings settings = Settings.builder()
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.proxy.type", randomFrom("socks", "http", "https"))
            .build();
        final SettingsException e = expectThrows(SettingsException.class, () -> S3ClientSettings.load(settings, configPath()));
        assertEquals("S3 proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    public void testSocksDoesNotSupportForHttpProtocol() {
        final Settings settings = Settings.builder()
            .put("s3.client.default.proxy.host", "127.0.0.1")
            .put("s3.client.default.proxy.port", 8080)
            .put("s3.client.default.protocol", "http")
            .put("s3.client.default.proxy.type", "socks")
            .build();
        expectThrows(SettingsException.class, () -> S3ClientSettings.load(settings, configPath()));
    }
}
