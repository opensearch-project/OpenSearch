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

package org.opensearch.repositories.gcs;

import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.ServiceAccountCredentials;

import org.opensearch.common.Strings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.SecureString;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.settings.Setting.timeSetting;

/**
 * Container for Google Cloud Storage clients settings.
 */
public class GoogleCloudStorageClientSettings {

    private static final String PREFIX = "gcs.client.";

    /** A json Service Account file loaded from secure settings. */
    static final Setting.AffixSetting<InputStream> CREDENTIALS_FILE_SETTING = Setting.affixKeySetting(
        PREFIX,
        "credentials_file",
        key -> SecureSetting.secureFile(key, null)
    );

    /** An override for the Storage endpoint to connect to. */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "endpoint",
        key -> Setting.simpleString(key, Setting.Property.NodeScope)
    );

    /** An override for the Google Project ID. */
    static final Setting.AffixSetting<String> PROJECT_ID_SETTING = Setting.affixKeySetting(
        PREFIX,
        "project_id",
        key -> Setting.simpleString(key, Setting.Property.NodeScope)
    );

    /** An override for the Token Server URI in the oauth flow. */
    static final Setting.AffixSetting<URI> TOKEN_URI_SETTING = Setting.affixKeySetting(
        PREFIX,
        "token_uri",
        key -> new Setting<>(key, "", URI::create, Setting.Property.NodeScope)
    );

    /**
     * The timeout to establish a connection. A value of {@code -1} corresponds to an infinite timeout. A value of {@code 0}
     * corresponds to the default timeout of the Google Cloud Storage Java Library.
     */
    static final Setting.AffixSetting<TimeValue> CONNECT_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "connect_timeout",
        key -> timeSetting(key, TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope)
    );

    /**
     * The timeout to read data from an established connection. A value of {@code -1} corresponds to an infinite timeout. A value of
     * {@code 0} corresponds to the default timeout of the Google Cloud Storage Java Library.
     */
    static final Setting.AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "read_timeout",
        key -> timeSetting(key, TimeValue.ZERO, TimeValue.MINUS_ONE, Setting.Property.NodeScope)
    );

    /** Name used by the client when it uses the Google Cloud JSON API. */
    static final Setting.AffixSetting<String> APPLICATION_NAME_SETTING = Setting.affixKeySetting(
        PREFIX,
        "application_name",
        key -> new Setting<>(key, "repository-gcs", Function.identity(), Setting.Property.NodeScope, Setting.Property.Deprecated)
    );

    /** Proxy type */
    static final Setting.AffixSetting<Proxy.Type> PROXY_TYPE_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.type",
        (key) -> new Setting<Proxy.Type>(
            key,
            Proxy.Type.DIRECT.name(),
            s -> Proxy.Type.valueOf(s.toUpperCase(Locale.ROOT)),
            Setting.Property.NodeScope
        )
    );

    /** The host of a proxy to connect */
    static final Setting.AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.host",
        key -> Setting.simpleString(key, Setting.Property.NodeScope),
        () -> PROXY_TYPE_SETTING
    );

    /** The port of a proxy to connect */
    static final Setting.AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.port",
        key -> Setting.intSetting(key, 0, 0, (1 << 16) - 1, Setting.Property.NodeScope),
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING
    );

    /** The username of a proxy to connect */
    static final Setting.AffixSetting<SecureString> PROXY_USERNAME_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.username",
        key -> SecureSetting.secureString(key, null),
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING
    );

    /** The password of a proxy to connect */
    static final Setting.AffixSetting<SecureString> PROXY_PASSWORD_SETTING = Setting.affixKeySetting(
        PREFIX,
        "proxy.password",
        key -> SecureSetting.secureString(key, null),
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING,
        () -> PROXY_USERNAME_SETTING
    );

    /** The credentials used by the client to connect to the Storage endpoint. */
    private final ServiceAccountCredentials credential;

    /** The Storage endpoint URL the client should talk to. Null value sets the default. */
    private final String endpoint;

    /** The Google project ID overriding the default way to infer it. Null value sets the default. */
    private final String projectId;

    /** The timeout to establish a connection */
    private final TimeValue connectTimeout;

    /** The timeout to read data from an established connection */
    private final TimeValue readTimeout;

    /** The Storage client application name */
    private final String applicationName;

    /** The token server URI. This leases access tokens in the oauth flow. */
    private final URI tokenUri;

    /** The GCS SDK Proxy settings. */
    private final ProxySettings proxySettings;

    GoogleCloudStorageClientSettings(
        final ServiceAccountCredentials credential,
        final String endpoint,
        final String projectId,
        final TimeValue connectTimeout,
        final TimeValue readTimeout,
        final String applicationName,
        final URI tokenUri,
        final ProxySettings proxySettings
    ) {
        this.credential = credential;
        this.endpoint = endpoint;
        this.projectId = projectId;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.applicationName = applicationName;
        this.tokenUri = tokenUri;
        this.proxySettings = proxySettings;
    }

    public ServiceAccountCredentials getCredential() {
        return credential;
    }

    public String getHost() {
        return endpoint;
    }

    public String getProjectId() {
        return Strings.hasLength(projectId) ? projectId : (credential != null ? credential.getProjectId() : null);
    }

    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    public TimeValue getReadTimeout() {
        return readTimeout;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public URI getTokenUri() {
        return tokenUri;
    }

    public ProxySettings getProxySettings() {
        return proxySettings;
    }

    public static Map<String, GoogleCloudStorageClientSettings> load(final Settings settings) {
        final Map<String, GoogleCloudStorageClientSettings> clients = new HashMap<>();
        for (final String clientName : settings.getGroups(PREFIX).keySet()) {
            clients.put(clientName, getClientSettings(settings, clientName));
        }
        if (clients.containsKey("default") == false) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clients.put("default", getClientSettings(settings, "default"));
        }
        return Collections.unmodifiableMap(clients);
    }

    static GoogleCloudStorageClientSettings getClientSettings(final Settings settings, final String clientName) {
        return new GoogleCloudStorageClientSettings(
            loadCredential(settings, clientName),
            getConfigValue(settings, clientName, ENDPOINT_SETTING),
            getConfigValue(settings, clientName, PROJECT_ID_SETTING),
            getConfigValue(settings, clientName, CONNECT_TIMEOUT_SETTING),
            getConfigValue(settings, clientName, READ_TIMEOUT_SETTING),
            getConfigValue(settings, clientName, APPLICATION_NAME_SETTING),
            getConfigValue(settings, clientName, TOKEN_URI_SETTING),
            validateAndCreateProxySettings(settings, clientName)
        );
    }

    static ProxySettings validateAndCreateProxySettings(final Settings settings, final String clientName) {
        final Proxy.Type proxyType = getConfigValue(settings, clientName, PROXY_TYPE_SETTING);
        final String proxyHost = getConfigValue(settings, clientName, PROXY_HOST_SETTING);
        final int proxyPort = getConfigValue(settings, clientName, PROXY_PORT_SETTING);
        final SecureString proxyUserName = getConfigValue(settings, clientName, PROXY_USERNAME_SETTING);
        final SecureString proxyPassword = getConfigValue(settings, clientName, PROXY_PASSWORD_SETTING);
        // Validate proxy settings
        if (proxyType == Proxy.Type.DIRECT
            && (proxyPort != 0 || Strings.hasText(proxyHost) || Strings.hasText(proxyUserName) || Strings.hasText(proxyPassword))) {
            throw new SettingsException(
                "Google Cloud Storage proxy port or host or username or password have been set but proxy type is not defined."
            );
        }
        if (proxyType != Proxy.Type.DIRECT && (proxyPort == 0 || Strings.isEmpty(proxyHost))) {
            throw new SettingsException("Google Cloud Storage proxy type has been set but proxy host or port is not defined.");
        }
        if (proxyType == Proxy.Type.DIRECT) {
            return ProxySettings.NO_PROXY_SETTINGS;
        }

        try {
            final InetAddress proxyHostAddress = InetAddress.getByName(proxyHost);
            return new ProxySettings(proxyType, proxyHostAddress, proxyPort, proxyUserName.toString(), proxyPassword.toString());
        } catch (final UnknownHostException e) {
            throw new SettingsException("Google Cloud Storage proxy host is unknown.", e);
        }
    }

    /**
     * Loads the service account file corresponding to a given client name. If no
     * file is defined for the client, a {@code null} credential is returned.
     *
     * @param settings
     *            the {@link Settings}
     * @param clientName
     *            the client name
     *
     * @return the {@link ServiceAccountCredentials} to use for the given client,
     *         {@code null} if no service account is defined.
     */
    static ServiceAccountCredentials loadCredential(final Settings settings, final String clientName) {
        try {
            if (CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).exists(settings) == false) {
                // explicitly returning null here so that the default credential
                // can be loaded later when creating the Storage client
                return null;
            }
            try (InputStream credStream = CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).get(settings)) {
                final Collection<String> scopes = Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL);
                return SocketAccess.doPrivilegedIOException(() -> {
                    final ServiceAccountCredentials credentials = ServiceAccountCredentials.fromStream(credStream);
                    if (credentials.createScopedRequired()) {
                        return (ServiceAccountCredentials) credentials.createScoped(scopes);
                    }
                    return credentials;
                });
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <T> T getConfigValue(final Settings settings, final String clientName, final Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }
}
