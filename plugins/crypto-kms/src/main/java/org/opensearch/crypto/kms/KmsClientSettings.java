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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.settings.SecureString;

import java.util.Locale;
import java.util.Objects;

/**
 * A container for settings used to create an kms client.
 */
public class KmsClientSettings {

    /** The access key (ie login id) for connecting to kms. */
    static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.secureString("kms.access_key", null);

    /** The secret key (ie password) for connecting to kms. */
    static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.secureString("kms.secret_key", null);

    /** The session token for connecting to kms. */
    static final Setting<SecureString> SESSION_TOKEN_SETTING = SecureSetting.secureString("kms.session_token", null);

    /** The host name of a proxy to connect to kms through. */
    static final Setting<String> PROXY_HOST_SETTING = Setting.simpleString("kms.proxy.host", Property.NodeScope);

    /** The port of a proxy to connect to kms through. */
    static final Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("kms.proxy.port", 80, 0, 1 << 16, Property.NodeScope);

    /** An override for the kms endpoint to connect to. */
    static final Setting<String> ENDPOINT_SETTING = new Setting<>("kms.endpoint", "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope);

    /** An override for the scoping region for authentication. */
    static final Setting<String> REGION_SETTING = new Setting<>("kms.region", "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope);

    /** The username of a proxy to connect to kms through. */
    static final Setting<SecureString> PROXY_USERNAME_SETTING = SecureSetting.secureString("kms.proxy.username", null);

    /** The password of a proxy to connect to kms through. */
    static final Setting<SecureString> PROXY_PASSWORD_SETTING = SecureSetting.secureString("kms.proxy.password", null);

    /** The socket timeout for connecting to kms. */
    static final Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting(
        "kms.read_timeout",
        TimeValue.timeValueMillis(50_000),
        Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(KmsClientSettings.class);

    /** Credentials to authenticate with kms. */
    final AwsCredentials credentials;

    /**
     * The kms endpoint the client should talk to, or empty string to use the
     * default.
     */
    final String endpoint;

    /**
     * The kms signing region.
     */
    final String region;

    /** An optional proxy host that requests to kms should be made through. */
    final String proxyHost;

    /** The port number the proxy host should be connected on. */
    final int proxyPort;

    // these should be "secure" yet the api for the kms client only takes String, so
    // storing them
    // as SecureString here won't really help with anything
    /** An optional username for the proxy host, for basic authentication. */
    final String proxyUsername;

    /** An optional password for the proxy host, for basic authentication. */
    final String proxyPassword;

    /** The read timeout for the kms client. */
    final int readTimeoutMillis;

    protected KmsClientSettings(
        AwsCredentials credentials,
        String endpoint,
        String region,
        String proxyHost,
        int proxyPort,
        String proxyUsername,
        String proxyPassword,
        int readTimeoutMillis
    ) {
        this.credentials = credentials;
        this.endpoint = endpoint;
        this.region = region;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.readTimeoutMillis = readTimeoutMillis;
    }

    static AwsCredentials loadCredentials(Settings settings) {
        try (
            SecureString key = ACCESS_KEY_SETTING.get(settings);
            SecureString secret = SECRET_KEY_SETTING.get(settings);
            SecureString sessionToken = SESSION_TOKEN_SETTING.get(settings)
        ) {
            if (key.length() == 0 && secret.length() == 0) {
                if (sessionToken.length() > 0) {
                    throw new SettingsException(
                        "Setting [{}] is set but [{}] and [{}] are not",
                        SESSION_TOKEN_SETTING.getKey(),
                        ACCESS_KEY_SETTING.getKey(),
                        SECRET_KEY_SETTING.getKey()
                    );
                }

                logger.debug("Using either environment variables, system properties or instance profile credentials");
                return null;
            } else {
                if (key.length() == 0) {
                    throw new SettingsException(
                        "Setting [{}] is set but [{}] is not",
                        SECRET_KEY_SETTING.getKey(),
                        ACCESS_KEY_SETTING.getKey(),
                        SECRET_KEY_SETTING.getKey()
                    );
                }
                if (secret.length() == 0) {
                    throw new SettingsException(
                        "Setting [{}] is set but [{}] is not",
                        ACCESS_KEY_SETTING.getKey(),
                        SECRET_KEY_SETTING.getKey()
                    );
                }

                final AwsCredentials credentials;
                if (sessionToken.length() == 0) {
                    logger.debug("Using basic key/secret credentials");
                    credentials = AwsBasicCredentials.create(key.toString(), secret.toString());
                } else {
                    logger.debug("Using basic session credentials");
                    credentials = AwsSessionCredentials.create(key.toString(), secret.toString(), sessionToken.toString());
                }
                return credentials;
            }
        }
    }

    /** Parse settings for a single client. */
    static KmsClientSettings getClientSettings(Settings settings) {
        final AwsCredentials credentials = loadCredentials(settings);
        try (
            SecureString proxyUsername = PROXY_USERNAME_SETTING.get(settings);
            SecureString proxyPassword = PROXY_PASSWORD_SETTING.get(settings)
        ) {
            return new KmsClientSettings(
                credentials,
                ENDPOINT_SETTING.get(settings),
                REGION_SETTING.get(settings),
                PROXY_HOST_SETTING.get(settings),
                PROXY_PORT_SETTING.get(settings),
                proxyUsername.toString(),
                proxyPassword.toString(),
                (int) READ_TIMEOUT_SETTING.get(settings).millis()
            );
        }
    }

    KmsClientSettings getMetadataSettings(Settings settings) {
        AwsCredentials newCredentials = loadCredentials(settings);
        newCredentials = newCredentials == null ? this.credentials : newCredentials;
        final Settings normalizedSettings = Settings.builder().put(settings).normalizePrefix("kms.").build();

        String newProxyUsername = this.proxyUsername, newProxyPassword = this.proxyPassword;
        if (PROXY_USERNAME_SETTING.exists(normalizedSettings)) {
            try (SecureString proxyUsername = PROXY_USERNAME_SETTING.get(settings)) {
                newProxyUsername = proxyUsername.toString();
            }
        }
        if (PROXY_PASSWORD_SETTING.exists(normalizedSettings)) {
            try (SecureString proxyPassword = PROXY_PASSWORD_SETTING.get(settings)) {
                newProxyPassword = proxyPassword.toString();
            }
        }

        String newEndpoint = getCryptoMetadataSettingOrExisting(ENDPOINT_SETTING, normalizedSettings, this.endpoint);
        String newRegion = getCryptoMetadataSettingOrExisting(REGION_SETTING, normalizedSettings, this.region);
        String newProxyHost = getCryptoMetadataSettingOrExisting(PROXY_HOST_SETTING, normalizedSettings, this.proxyHost);
        int newProxyPort = getCryptoMetadataSettingOrExisting(PROXY_PORT_SETTING, normalizedSettings, this.proxyPort);
        TimeValue newReadTimeout = getCryptoMetadataSettingOrExisting(
            READ_TIMEOUT_SETTING,
            normalizedSettings,
            TimeValue.timeValueMillis(this.readTimeoutMillis)
        );

        return new KmsClientSettings(
            newCredentials,
            newEndpoint,
            newRegion,
            newProxyHost,
            newProxyPort,
            newProxyUsername,
            newProxyPassword,
            (int) newReadTimeout.millis()
        );
    }

    private static <T> T getCryptoMetadataSettingOrExisting(Setting<T> setting, Settings normalizedSettings, T defaultValue) {
        if (setting.exists(normalizedSettings)) {
            return setting.get(normalizedSettings);
        }
        return defaultValue;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KmsClientSettings that = (KmsClientSettings) o;
        return readTimeoutMillis == that.readTimeoutMillis
            && Objects.equals(credentials, that.credentials)
            && Objects.equals(endpoint, that.endpoint)
            && Objects.equals(region, that.region)
            && Objects.equals(proxyHost, that.proxyHost)
            && Objects.equals(proxyPort, that.proxyPort)
            && Objects.equals(proxyUsername, that.proxyUsername)
            && Objects.equals(proxyPassword, that.proxyPassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readTimeoutMillis, credentials, endpoint, region, proxyHost, proxyPort, proxyUsername, proxyPassword);
    }
}
