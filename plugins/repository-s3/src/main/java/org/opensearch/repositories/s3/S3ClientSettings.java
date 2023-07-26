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

import org.opensearch.core.common.Strings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.repositories.s3.utils.Protocol;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * A container for settings used to create an S3 client.
 */
final class S3ClientSettings {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(S3ClientSettings.class);

    // prefix for s3 client settings
    private static final String PREFIX = "s3.client.";

    /** Placeholder client name for normalizing client settings in the repository settings. */
    private static final String PLACEHOLDER_CLIENT = "placeholder";

    // Properties to support using IAM Roles for Service Accounts (IRSA)

    /** The identity token file for connecting to s3. */
    static final Setting.AffixSetting<String> IDENTITY_TOKEN_FILE_SETTING = Setting.affixKeySetting(
        PREFIX,
        "identity_token_file",
        key -> SecureSetting.simpleString(key, Property.NodeScope)
    );

    /** The role ARN (Amazon Resource Name) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> ROLE_ARN_SETTING = Setting.affixKeySetting(
        PREFIX,
        "role_arn",
        key -> SecureSetting.secureString(key, null)
    );

    /** The role session name for connecting to s3. */
    static final Setting.AffixSetting<SecureString> ROLE_SESSION_NAME_SETTING = Setting.affixKeySetting(
        PREFIX,
        "role_session_name",
        key -> SecureSetting.secureString(key, null)
    );

    /** The access key (ie login id) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> ACCESS_KEY_SETTING = Setting.affixKeySetting(
        PREFIX,
        "access_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> SECRET_KEY_SETTING = Setting.affixKeySetting(
        PREFIX,
        "secret_key",
        key -> SecureSetting.secureString(key, null)
    );

    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<SecureString> SESSION_TOKEN_SETTING = Setting.affixKeySetting(
        PREFIX,
        "session_token",
        key -> SecureSetting.secureString(key, null)
    );

    /** An override for the s3 endpoint to connect to. */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "endpoint",
        key -> new Setting<>(key, "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope)
    );

    /** The protocol to use to connect to s3. */
    static final Setting.AffixSetting<Protocol> PROTOCOL_SETTING = Setting.affixKeySetting(
        PREFIX,
        "protocol",
        key -> new Setting<>(key, "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope)
    );

    /** The protocol to use to connect to s3. */
    static final Setting.AffixSetting<ProxySettings.ProxyType> PROXY_TYPE_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.type",
        key -> new Setting<>(key, "direct", s -> ProxySettings.ProxyType.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope)
    );

    /** The host name of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.host",
        key -> Setting.simpleString(key, Property.NodeScope)
    );

    /** The port of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.port",
        key -> Setting.intSetting(key, 80, 0, (1 << 16) - 1, Property.NodeScope)
    );

    /** The username of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_USERNAME_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.username",
        key -> SecureSetting.secureString(key, null)
    );

    /** The password of a proxy to connect to s3 through. */
    static final Setting.AffixSetting<SecureString> PROXY_PASSWORD_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.password",
        key -> SecureSetting.secureString(key, null)
    );

    /** The socket timeout for connecting to s3. */
    static final Setting.AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "read_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueMillis(50_000), Property.NodeScope)
    );

    /** The request timeout for connecting to s3. */
    static final Setting.AffixSetting<TimeValue> REQUEST_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "request_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueMinutes(2), Property.NodeScope)
    );

    /** The connection timeout for connecting to s3. */
    static final Setting.AffixSetting<TimeValue> CONNECTION_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "connection_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(10), Property.NodeScope)
    );

    /** The connection TTL for connecting to s3. */
    static final Setting.AffixSetting<TimeValue> CONNECTION_TTL_SETTING = Setting.affixKeySetting(
        PREFIX,
        "connection_ttl",
        key -> Setting.timeSetting(key, TimeValue.timeValueMillis(5000), Property.NodeScope)
    );

    /** The maximum connections to s3. */
    static final Setting.AffixSetting<Integer> MAX_CONNECTIONS_SETTING = Setting.affixKeySetting(
        PREFIX,
        "max_connections",
        key -> Setting.intSetting(key, 100, Property.NodeScope)
    );

    /** Connection acquisition timeout for new connections to S3. */
    static final Setting.AffixSetting<TimeValue> CONNECTION_ACQUISITION_TIMEOUT = Setting.affixKeySetting(
        PREFIX,
        "connection_acquisition_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueMinutes(2), Property.NodeScope)
    );

    /** The maximum pending connections to S3. */
    static final Setting.AffixSetting<Integer> MAX_PENDING_CONNECTION_ACQUIRES = Setting.affixKeySetting(
        PREFIX,
        "max_pending_connection_acquires",
        key -> Setting.intSetting(key, 10_000, Property.NodeScope)
    );

    /** The number of retries to use when an s3 request fails. */
    static final Setting.AffixSetting<Integer> MAX_RETRIES_SETTING = Setting.affixKeySetting(
        PREFIX,
        "max_retries",
        key -> Setting.intSetting(key, 3, 0, Property.NodeScope)
    );

    /** Whether retries should be throttled (ie use backoff). */
    static final Setting.AffixSetting<Boolean> USE_THROTTLE_RETRIES_SETTING = Setting.affixKeySetting(
        PREFIX,
        "use_throttle_retries",
        key -> Setting.boolSetting(key, true, Property.NodeScope)
    );

    /** Whether the s3 client should use path style access. */
    static final Setting.AffixSetting<Boolean> USE_PATH_STYLE_ACCESS = Setting.affixKeySetting(
        PREFIX,
        "path_style_access",
        key -> Setting.boolSetting(key, false, Property.NodeScope)
    );

    /** Whether chunked encoding should be disabled or not (Default is false). */
    static final Setting.AffixSetting<Boolean> DISABLE_CHUNKED_ENCODING = Setting.affixKeySetting(
        PREFIX,
        "disable_chunked_encoding",
        key -> Setting.boolSetting(key, false, Property.NodeScope)
    );

    /** An override for the s3 region to use for signing requests. */
    static final Setting.AffixSetting<String> REGION = Setting.affixKeySetting(
        PREFIX,
        "region",
        key -> new Setting<>(key, "", Function.identity(), Property.NodeScope)
    );

    /** An override for the signer to use. */
    static final Setting.AffixSetting<String> SIGNER_OVERRIDE = Setting.affixKeySetting(
        PREFIX,
        "signer_override",
        key -> new Setting<>(key, "", Function.identity(), Property.NodeScope)
    );

    /** Credentials to authenticate with s3. */
    final AwsCredentials credentials;

    /** Credentials to authenticate with s3 using IAM Roles for Service Accounts (IRSA). */
    final IrsaCredentials irsaCredentials;

    /** The s3 endpoint the client should talk to, or empty string to use the default. */
    final String endpoint;

    /** The protocol to use to talk to s3. Defaults to https. */
    final Protocol protocol;

    /** An optional proxy settings that requests to s3 should be made through. */
    final ProxySettings proxySettings;

    /** The read timeout for the s3 client. */
    final int readTimeoutMillis;

    /** The request timeout for the s3 client */
    final int requestTimeoutMillis;

    /** The connection timeout for the s3 client */
    final int connectionTimeoutMillis;

    /** The connection TTL for the s3 client */
    final int connectionTTLMillis;

    /** The max number of connections for the s3 client */
    final int maxConnections;

    /** The connnection acquisition timeout for the s3 async client */
    final int connectionAcquisitionTimeoutMillis;

    /** The number of retries to use for the s3 client. */
    final int maxRetries;

    /** Whether the s3 client should use an exponential backoff retry policy. */
    final boolean throttleRetries;

    /** Whether the s3 client should use path style access. */
    final boolean pathStyleAccess;

    /** Whether chunked encoding should be disabled or not. */
    final boolean disableChunkedEncoding;

    /** Region to use for signing requests or empty string to use default. */
    final String region;

    /** Signer override to use or empty string to use default. */
    final String signerOverride;

    private S3ClientSettings(
        AwsCredentials credentials,
        IrsaCredentials irsaCredentials,
        String endpoint,
        Protocol protocol,
        int readTimeoutMillis,
        int requestTimeoutMillis,
        int connectionTimeoutMillis,
        int connectionTTLMillis,
        int maxConnections,
        int connectionAcquisitionTimeoutMillis,
        int maxRetries,
        boolean throttleRetries,
        boolean pathStyleAccess,
        boolean disableChunkedEncoding,
        String region,
        String signerOverride,
        ProxySettings proxySettings
    ) {
        this.credentials = credentials;
        this.irsaCredentials = irsaCredentials;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.readTimeoutMillis = readTimeoutMillis;
        this.requestTimeoutMillis = requestTimeoutMillis;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.connectionTTLMillis = connectionTTLMillis;
        this.maxConnections = maxConnections;
        this.connectionAcquisitionTimeoutMillis = connectionAcquisitionTimeoutMillis;
        this.maxRetries = maxRetries;
        this.throttleRetries = throttleRetries;
        this.pathStyleAccess = pathStyleAccess;
        this.disableChunkedEncoding = disableChunkedEncoding;
        this.region = region;
        this.signerOverride = signerOverride;
        this.proxySettings = proxySettings;
    }

    /**
     * Overrides the settings in this instance with settings found in repository metadata.
     *
     * @param repositorySettings found in repository metadata
     * @return S3ClientSettings
     */
    S3ClientSettings refine(Settings repositorySettings) {
        // Normalize settings to placeholder client settings prefix so that we can use the affix settings directly
        final Settings normalizedSettings = Settings.builder()
            .put(repositorySettings)
            .normalizePrefix(PREFIX + PLACEHOLDER_CLIENT + '.')
            .build();
        final String newEndpoint = getRepoSettingOrDefault(ENDPOINT_SETTING, normalizedSettings, endpoint);

        final Protocol newProtocol = getRepoSettingOrDefault(PROTOCOL_SETTING, normalizedSettings, protocol);

        final String newProxyHost = getRepoSettingOrDefault(PROXY_HOST_SETTING, normalizedSettings, proxySettings.getHostName());
        final int newProxyPort = getRepoSettingOrDefault(PROXY_PORT_SETTING, normalizedSettings, proxySettings.getPort());

        final int newReadTimeoutMillis = Math.toIntExact(
            getRepoSettingOrDefault(READ_TIMEOUT_SETTING, normalizedSettings, TimeValue.timeValueMillis(readTimeoutMillis)).millis()
        );
        final int newRequestTimeoutMillis = Math.toIntExact(
            getRepoSettingOrDefault(REQUEST_TIMEOUT_SETTING, normalizedSettings, TimeValue.timeValueMillis(requestTimeoutMillis)).millis()
        );
        final int newConnectionTimeoutMillis = Math.toIntExact(
            getRepoSettingOrDefault(CONNECTION_TIMEOUT_SETTING, normalizedSettings, TimeValue.timeValueMillis(connectionTimeoutMillis))
                .millis()
        );
        final int newConnectionTTLMillis = Math.toIntExact(
            getRepoSettingOrDefault(CONNECTION_TTL_SETTING, normalizedSettings, TimeValue.timeValueMillis(connectionTTLMillis)).millis()
        );
        final int newConnectionAcquisitionTimeoutMillis = Math.toIntExact(
            getRepoSettingOrDefault(
                CONNECTION_ACQUISITION_TIMEOUT,
                normalizedSettings,
                TimeValue.timeValueMillis(connectionAcquisitionTimeoutMillis)
            ).millis()
        );
        final int newMaxConnections = Math.toIntExact(getRepoSettingOrDefault(MAX_CONNECTIONS_SETTING, normalizedSettings, maxConnections));
        final int newMaxRetries = getRepoSettingOrDefault(MAX_RETRIES_SETTING, normalizedSettings, maxRetries);
        final boolean newThrottleRetries = getRepoSettingOrDefault(USE_THROTTLE_RETRIES_SETTING, normalizedSettings, throttleRetries);
        final boolean newPathStyleAccess = getRepoSettingOrDefault(USE_PATH_STYLE_ACCESS, normalizedSettings, pathStyleAccess);
        final boolean newDisableChunkedEncoding = getRepoSettingOrDefault(
            DISABLE_CHUNKED_ENCODING,
            normalizedSettings,
            disableChunkedEncoding
        );
        final AwsCredentials newCredentials;
        if (checkDeprecatedCredentials(repositorySettings)) {
            newCredentials = loadDeprecatedCredentials(repositorySettings);
        } else {
            newCredentials = credentials;
        }
        final String newRegion = getRepoSettingOrDefault(REGION, normalizedSettings, region);
        final String newSignerOverride = getRepoSettingOrDefault(SIGNER_OVERRIDE, normalizedSettings, signerOverride);
        if (Objects.equals(endpoint, newEndpoint)
            && protocol == newProtocol
            && Objects.equals(proxySettings.getHostName(), newProxyHost)
            && proxySettings.getPort() == newProxyPort
            && newReadTimeoutMillis == readTimeoutMillis
            && newRequestTimeoutMillis == requestTimeoutMillis
            && newConnectionTimeoutMillis == connectionTimeoutMillis
            && newConnectionTTLMillis == connectionTTLMillis
            && newMaxConnections == maxConnections
            && newConnectionAcquisitionTimeoutMillis == connectionAcquisitionTimeoutMillis
            && maxRetries == newMaxRetries
            && newThrottleRetries == throttleRetries
            && Objects.equals(credentials, newCredentials)
            && newPathStyleAccess == pathStyleAccess
            && newDisableChunkedEncoding == disableChunkedEncoding
            && Objects.equals(region, newRegion)
            && Objects.equals(signerOverride, newSignerOverride)) {
            return this;
        }

        validateInetAddressFor(newProxyHost);
        return new S3ClientSettings(
            newCredentials,
            irsaCredentials,
            newEndpoint,
            newProtocol,
            newReadTimeoutMillis,
            newRequestTimeoutMillis,
            newConnectionTimeoutMillis,
            newConnectionTTLMillis,
            newMaxConnections,
            newConnectionAcquisitionTimeoutMillis,
            newMaxRetries,
            newThrottleRetries,
            newPathStyleAccess,
            newDisableChunkedEncoding,
            newRegion,
            newSignerOverride,
            proxySettings.recreateWithNewHostAndPort(newProxyHost, newProxyPort)
        );
    }

    /**
     * Load all client settings from the given settings.
     *
     * Note this will always at least return a client named "default".
     */
    static Map<String, S3ClientSettings> load(final Settings settings, final Path configPath) {
        final Set<String> clientNames = settings.getGroups(PREFIX).keySet();
        final Map<String, S3ClientSettings> clients = new HashMap<>();
        for (final String clientName : clientNames) {
            clients.put(clientName, getClientSettings(settings, clientName, configPath));
        }
        if (clients.containsKey("default") == false) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clients.put("default", getClientSettings(settings, "default", configPath));
        }
        return Collections.unmodifiableMap(clients);
    }

    static boolean checkDeprecatedCredentials(Settings repositorySettings) {
        if (S3Repository.ACCESS_KEY_SETTING.exists(repositorySettings)) {
            if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings) == false) {
                throw new IllegalArgumentException(
                    "Repository setting ["
                        + S3Repository.ACCESS_KEY_SETTING.getKey()
                        + " must be accompanied by setting ["
                        + S3Repository.SECRET_KEY_SETTING.getKey()
                        + "]"
                );
            }
            return true;
        } else if (S3Repository.SECRET_KEY_SETTING.exists(repositorySettings)) {
            throw new IllegalArgumentException(
                "Repository setting ["
                    + S3Repository.SECRET_KEY_SETTING.getKey()
                    + " must be accompanied by setting ["
                    + S3Repository.ACCESS_KEY_SETTING.getKey()
                    + "]"
            );
        }
        return false;
    }

    // backcompat for reading keys out of repository settings (clusterState)
    private static AwsCredentials loadDeprecatedCredentials(Settings repositorySettings) {
        assert checkDeprecatedCredentials(repositorySettings);
        try (
            SecureString key = S3Repository.ACCESS_KEY_SETTING.get(repositorySettings);
            SecureString secret = S3Repository.SECRET_KEY_SETTING.get(repositorySettings)
        ) {
            return AwsBasicCredentials.create(key.toString(), secret.toString());
        }
    }

    private static AwsCredentials loadCredentials(Settings settings, String clientName) {
        try (
            SecureString accessKey = getConfigValue(settings, clientName, ACCESS_KEY_SETTING);
            SecureString secretKey = getConfigValue(settings, clientName, SECRET_KEY_SETTING);
            SecureString sessionToken = getConfigValue(settings, clientName, SESSION_TOKEN_SETTING)
        ) {
            if (accessKey.length() != 0) {
                if (secretKey.length() != 0) {
                    if (sessionToken.length() != 0) {
                        return AwsSessionCredentials.create(accessKey.toString(), secretKey.toString(), sessionToken.toString());
                    } else {
                        return AwsBasicCredentials.create(accessKey.toString(), secretKey.toString());
                    }
                } else {
                    throw new IllegalArgumentException("Missing secret key for s3 client [" + clientName + "]");
                }
            } else {
                if (secretKey.length() != 0) {
                    throw new IllegalArgumentException("Missing access key for s3 client [" + clientName + "]");
                }
                if (sessionToken.length() != 0) {
                    throw new IllegalArgumentException("Missing access key and secret key for s3 client [" + clientName + "]");
                }
                return null;
            }
        }
    }

    @SuppressForbidden(reason = "PathUtils#get")
    private static IrsaCredentials loadIrsaCredentials(Settings settings, String clientName, Path configPath) {
        String identityTokenFile = getConfigValue(settings, clientName, IDENTITY_TOKEN_FILE_SETTING);
        if (identityTokenFile.length() != 0) {
            final Path identityTokenFilePath = PathUtils.get(identityTokenFile);
            // If the path is not absolute, resolve it relatively to config path
            if (!identityTokenFilePath.isAbsolute()) {
                identityTokenFile = PathUtils.get(new Path[] { configPath }, identityTokenFile).toString();
            }
        }

        try (
            SecureString roleArn = getConfigValue(settings, clientName, ROLE_ARN_SETTING);
            SecureString roleSessionName = getConfigValue(settings, clientName, ROLE_SESSION_NAME_SETTING)
        ) {
            if (identityTokenFile.length() != 0 || roleArn.length() != 0 || roleSessionName.length() != 0) {
                return new IrsaCredentials(identityTokenFile.toString(), roleArn.toString(), roleSessionName.toString());
            }

            return null;
        }
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    static S3ClientSettings getClientSettings(final Settings settings, final String clientName, final Path configPath) {
        final Protocol awsProtocol = getConfigValue(settings, clientName, PROTOCOL_SETTING);
        return new S3ClientSettings(
            S3ClientSettings.loadCredentials(settings, clientName),
            S3ClientSettings.loadIrsaCredentials(settings, clientName, configPath),
            getConfigValue(settings, clientName, ENDPOINT_SETTING),
            awsProtocol,
            Math.toIntExact(getConfigValue(settings, clientName, READ_TIMEOUT_SETTING).millis()),
            Math.toIntExact(getConfigValue(settings, clientName, REQUEST_TIMEOUT_SETTING).millis()),
            Math.toIntExact(getConfigValue(settings, clientName, CONNECTION_TIMEOUT_SETTING).millis()),
            Math.toIntExact(getConfigValue(settings, clientName, CONNECTION_TTL_SETTING).millis()),
            Math.toIntExact(getConfigValue(settings, clientName, MAX_CONNECTIONS_SETTING)),
            Math.toIntExact(getConfigValue(settings, clientName, CONNECTION_ACQUISITION_TIMEOUT).millis()),
            getConfigValue(settings, clientName, MAX_RETRIES_SETTING),
            getConfigValue(settings, clientName, USE_THROTTLE_RETRIES_SETTING),
            getConfigValue(settings, clientName, USE_PATH_STYLE_ACCESS),
            getConfigValue(settings, clientName, DISABLE_CHUNKED_ENCODING),
            getConfigValue(settings, clientName, REGION),
            getConfigValue(settings, clientName, SIGNER_OVERRIDE),
            validateAndCreateProxySettings(settings, clientName, awsProtocol)
        );
    }

    static ProxySettings validateAndCreateProxySettings(final Settings settings, final String clientName, final Protocol awsProtocol) {
        ProxySettings.ProxyType proxyType = getConfigValue(settings, clientName, PROXY_TYPE_SETTING);
        final String proxyHost = getConfigValue(settings, clientName, PROXY_HOST_SETTING);
        final int proxyPort = getConfigValue(settings, clientName, PROXY_PORT_SETTING);
        final SecureString proxyUserName = getConfigValue(settings, clientName, PROXY_USERNAME_SETTING);
        final SecureString proxyPassword = getConfigValue(settings, clientName, PROXY_PASSWORD_SETTING);
        if (awsProtocol != Protocol.HTTPS && proxyType == ProxySettings.ProxyType.DIRECT && Strings.hasText(proxyHost)) {
            // This is backward compatibility for the current behaviour.
            // The default value for Protocol settings is HTTPS,
            // The expectation of ex-developers that protocol is the same as the proxy protocol
            // which is a separate setting for AWS SDK.
            // In this case, proxy type should be the same as a protocol,
            // when proxy host and port have been set
            proxyType = ProxySettings.ProxyType.valueOf(awsProtocol.name());
            deprecationLogger.deprecate(
                PROTOCOL_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                "Using of "
                    + PROTOCOL_SETTING.getConcreteSettingForNamespace(clientName).getKey()
                    + " as proxy type is deprecated and will be removed in future releases. Please use "
                    + PROXY_TYPE_SETTING.getConcreteSettingForNamespace(clientName).getKey()
                    + " instead to specify proxy type."
            );
        }
        // Validate proxy settings
        if (proxyType == ProxySettings.ProxyType.DIRECT
            && (proxyPort != 80 || Strings.hasText(proxyHost) || Strings.hasText(proxyUserName) || Strings.hasText(proxyPassword))) {
            throw new SettingsException("S3 proxy port or host or username or password have been set but proxy type is not defined.");
        }
        if (proxyType != ProxySettings.ProxyType.DIRECT && Strings.isEmpty(proxyHost)) {
            throw new SettingsException("S3 proxy type has been set but proxy host or port is not defined.");
        }
        if (proxyType == ProxySettings.ProxyType.DIRECT) {
            return ProxySettings.NO_PROXY_SETTINGS;
        }
        if (awsProtocol == Protocol.HTTP && proxyType == ProxySettings.ProxyType.SOCKS) {
            throw new SettingsException("SOCKS proxy is not supported for HTTP protocol");
        }
        validateInetAddressFor(proxyHost);
        return new ProxySettings(proxyType, proxyHost, proxyPort, proxyUserName.toString(), proxyPassword.toString());
    }

    static void validateInetAddressFor(final String proxyHost) {
        try {
            InetAddress.getByName(proxyHost);
        } catch (final UnknownHostException e) {
            throw new SettingsException("S3 proxy host is unknown.", e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final S3ClientSettings that = (S3ClientSettings) o;
        return readTimeoutMillis == that.readTimeoutMillis
            && requestTimeoutMillis == that.requestTimeoutMillis
            && connectionTimeoutMillis == that.connectionTimeoutMillis
            && connectionTTLMillis == that.connectionTTLMillis
            && maxConnections == that.maxConnections
            && connectionAcquisitionTimeoutMillis == that.connectionAcquisitionTimeoutMillis
            && maxRetries == that.maxRetries
            && throttleRetries == that.throttleRetries
            && Objects.equals(credentials, that.credentials)
            && Objects.equals(endpoint, that.endpoint)
            && protocol == that.protocol
            && proxySettings.equals(that.proxySettings)
            && Objects.equals(disableChunkedEncoding, that.disableChunkedEncoding)
            && Objects.equals(region, that.region)
            && Objects.equals(signerOverride, that.signerOverride)
            && Objects.equals(irsaCredentials, that.irsaCredentials);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            credentials,
            endpoint,
            protocol,
            proxySettings,
            readTimeoutMillis,
            requestTimeoutMillis,
            connectionTimeoutMillis,
            connectionTTLMillis,
            maxConnections,
            connectionAcquisitionTimeoutMillis,
            maxRetries,
            throttleRetries,
            disableChunkedEncoding,
            region,
            signerOverride
        );
    }

    private static <T> T getConfigValue(Settings settings, String clientName, Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    private static <T> T getRepoSettingOrDefault(Setting.AffixSetting<T> setting, Settings normalizedSettings, T defaultValue) {
        if (setting.getConcreteSettingForNamespace(PLACEHOLDER_CLIENT).exists(normalizedSettings)) {
            return getConfigValue(normalizedSettings, PLACEHOLDER_CLIENT, setting);
        }
        return defaultValue;
    }

    /**
     * Class to store IAM Roles for Service Accounts (IRSA) credentials
     * See please: https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html
     */
    static class IrsaCredentials {
        private final String identityTokenFile;
        private final String roleArn;
        private final String roleSessionName;

        IrsaCredentials(String identityTokenFile, String roleArn, String roleSessionName) {
            this.identityTokenFile = Strings.isNullOrEmpty(identityTokenFile) ? null : identityTokenFile;
            this.roleArn = Strings.isNullOrEmpty(roleArn) ? null : roleArn;
            this.roleSessionName = Strings.isNullOrEmpty(roleSessionName) ? "s3-sdk-java-" + System.currentTimeMillis() : roleSessionName;
        }

        public String getIdentityTokenFile() {
            return identityTokenFile;
        }

        public String getRoleArn() {
            return roleArn;
        }

        public String getRoleSessionName() {
            return roleSessionName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final IrsaCredentials that = (IrsaCredentials) o;
            return Objects.equals(identityTokenFile, that.identityTokenFile)
                && Objects.equals(roleArn, that.roleArn)
                && Objects.equals(roleSessionName, that.roleSessionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(identityTokenFile, roleArn, roleSessionName);
        }
    }
}
