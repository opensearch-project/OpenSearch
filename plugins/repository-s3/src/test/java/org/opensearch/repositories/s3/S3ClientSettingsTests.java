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

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.repositories.s3.utils.AwsRequestSigner;
import org.opensearch.repositories.s3.utils.Protocol;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class S3ClientSettingsTests extends AbstractS3RepositoryTestCase {
    public void testThereIsADefaultClientByDefault() {
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(Settings.EMPTY, configPath());
        assertThat(settings.keySet(), contains("default"));

        final S3ClientSettings defaultSettings = settings.get("default");
        assertThat(defaultSettings.credentials, nullValue());
        assertThat(defaultSettings.endpoint, is(emptyString()));
        assertThat(defaultSettings.protocol, is(Protocol.HTTPS));
        assertThat(defaultSettings.proxySettings, is(ProxySettings.NO_PROXY_SETTINGS));
        assertThat(defaultSettings.readTimeoutMillis, is(50 * 1000));
        assertThat(defaultSettings.requestTimeoutMillis, is(120 * 1000));
        assertThat(defaultSettings.connectionTimeoutMillis, is(10 * 1000));
        assertThat(defaultSettings.connectionTTLMillis, is(5 * 1000));
        assertThat(defaultSettings.maxConnections, is(100));
        assertThat(defaultSettings.maxRetries, is(3));
        assertThat(defaultSettings.throttleRetries, is(true));
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
        assertThat(defaultSettings.maxRetries, is(3));

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
        AwsCredentials credentials = defaultSettings.credentials;
        assertThat(credentials.accessKeyId(), is("access_key"));
        assertThat(credentials.secretAccessKey(), is("secret_key"));
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
        AwsSessionCredentials credentials = (AwsSessionCredentials) defaultSettings.credentials;
        assertThat(credentials.accessKeyId(), is("access_key"));
        assertThat(credentials.secretAccessKey(), is("secret_key"));
        assertThat(credentials.sessionToken(), is("session_token"));
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
            AwsSessionCredentials credentials = (AwsSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.accessKeyId(), is("access_key"));
            assertThat(credentials.secretAccessKey(), is("secret_key"));
            assertThat(credentials.sessionToken(), is("session_token"));
        }

        {
            final S3ClientSettings refinedSettings = baseSettings.refine(Settings.builder().put("path_style_access", true).build());
            assertThat(refinedSettings.pathStyleAccess, is(true));
            AwsSessionCredentials credentials = (AwsSessionCredentials) refinedSettings.credentials;
            assertThat(credentials.accessKeyId(), is("access_key"));
            assertThat(credentials.secretAccessKey(), is("secret_key"));
            assertThat(credentials.sessionToken(), is("session_token"));
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
        final String region = randomFrom(Region.regions().stream().map(Region::toString).toArray(String[]::new));
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.region", region).build(),
            configPath()
        );
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").region, is(region));
        try (
            S3Service s3Service = new S3Service(configPath());
            S3Client other = SocketAccess.doPrivileged(() -> s3Service.buildClient(settings.get("other")).client());
        ) {
            assertThat(other.serviceClientConfiguration().region(), is(Region.of(region)));
        }
    }

    public void testSignerOverrideCanBeSet() {
        S3Service.setDefaultAwsProfilePath();
        final String signerOverride = randomFrom(AwsRequestSigner.values()).getName();
        final Map<String, S3ClientSettings> settings = S3ClientSettings.load(
            Settings.builder().put("s3.client.other.signer_override", signerOverride).build(),
            configPath()
        );
        assertThat(settings.get("default").region, is(""));
        assertThat(settings.get("other").signerOverride, is(signerOverride));

        ClientOverrideConfiguration defaultConfiguration = SocketAccess.doPrivileged(
            () -> S3Service.buildOverrideConfiguration(settings.get("default"))
        );
        Optional<Signer> defaultSigner = defaultConfiguration.advancedOption(SdkAdvancedClientOption.SIGNER);
        assertFalse(defaultSigner.isPresent());

        ClientOverrideConfiguration configuration = SocketAccess.doPrivileged(
            () -> S3Service.buildOverrideConfiguration(settings.get("other"))
        );
        Optional<Signer> otherSigner = configuration.advancedOption(SdkAdvancedClientOption.SIGNER);
        assertTrue(otherSigner.isPresent());
        assertThat(otherSigner.get(), sameInstance(AwsRequestSigner.fromSignerName(signerOverride).getSigner()));
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
