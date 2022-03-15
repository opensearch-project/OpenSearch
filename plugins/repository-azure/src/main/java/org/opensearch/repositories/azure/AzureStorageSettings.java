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

package org.opensearch.repositories.azure;

import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.SecureString;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.AffixSetting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

final class AzureStorageSettings {

    // prefix for azure client settings
    private static final String AZURE_CLIENT_PREFIX_KEY = "azure.client.";

    /** Azure account name */
    public static final AffixSetting<SecureString> ACCOUNT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "account",
        key -> SecureSetting.secureString(key, null)
    );

    /** Azure key */
    public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "key",
        key -> SecureSetting.secureString(key, null)
    );

    /** Azure SAS token */
    public static final AffixSetting<SecureString> SAS_TOKEN_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "sas_token",
        key -> SecureSetting.secureString(key, null)
    );

    /** max_retries: Number of retries in case of Azure errors. Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT). */
    public static final AffixSetting<Integer> MAX_RETRIES_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "max_retries",
        (key) -> Setting.intSetting(key, 3, Setting.Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );
    /**
     * Azure endpoint suffix. Default to core.windows.net (CloudStorageAccount.DEFAULT_DNS).
     */
    public static final AffixSetting<String> ENDPOINT_SUFFIX_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "endpoint_suffix",
        key -> Setting.simpleString(key, Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    // The overall operation timeout
    public static final AffixSetting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueMinutes(-1), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_CONNECT_TIMEOUT
    public static final AffixSetting<TimeValue> CONNECT_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "connect.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(10), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_WRITE_TIMEOUT
    public static final AffixSetting<TimeValue> WRITE_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "write.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_READ_TIMEOUT
    public static final AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "read.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_RESPONSE_TIMEOUT
    public static final AffixSetting<TimeValue> RESPONSE_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "response.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    /** The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks */
    public static final AffixSetting<ProxySettings.ProxyType> PROXY_TYPE_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.type",
        (key) -> new Setting<>(key, "direct", s -> ProxySettings.ProxyType.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    /** The host name of a proxy to connect to azure through. */
    public static final AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.host",
        (key) -> Setting.simpleString(key, Property.NodeScope),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING
    );

    /** The port of a proxy to connect to azure through. */
    public static final AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.port",
        (key) -> Setting.intSetting(key, 0, 0, 65535, Setting.Property.NodeScope),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING
    );

    /** The username of a proxy to connect */
    static final AffixSetting<SecureString> PROXY_USERNAME_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.username",
        key -> SecureSetting.secureString(key, null),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING
    );

    /** The password of a proxy to connect */
    static final AffixSetting<SecureString> PROXY_PASSWORD_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.password",
        key -> SecureSetting.secureString(key, null),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING,
        () -> PROXY_USERNAME_SETTING
    );

    private final String account;
    private final String connectString;
    private final String endpointSuffix;
    private final TimeValue timeout;
    private final int maxRetries;
    private final LocationMode locationMode;
    private final TimeValue connectTimeout;
    private final TimeValue writeTimeout;
    private final TimeValue readTimeout;
    private final TimeValue responseTimeout;
    private final ProxySettings proxySettings;

    // copy-constructor
    private AzureStorageSettings(
        String account,
        String connectString,
        String endpointSuffix,
        TimeValue timeout,
        int maxRetries,
        LocationMode locationMode,
        TimeValue connectTimeout,
        TimeValue writeTimeout,
        TimeValue readTimeout,
        TimeValue responseTimeout,
        ProxySettings proxySettings
    ) {
        this.account = account;
        this.connectString = connectString;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.locationMode = locationMode;
        this.connectTimeout = connectTimeout;
        this.writeTimeout = writeTimeout;
        this.readTimeout = readTimeout;
        this.responseTimeout = responseTimeout;
        this.proxySettings = proxySettings;
    }

    private AzureStorageSettings(
        String account,
        String key,
        String sasToken,
        String endpointSuffix,
        TimeValue timeout,
        int maxRetries,
        TimeValue connectTimeout,
        TimeValue writeTimeout,
        TimeValue readTimeout,
        TimeValue responseTimeout,
        ProxySettings proxySettings
    ) {
        this.account = account;
        this.connectString = buildConnectString(account, key, sasToken, endpointSuffix);
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.locationMode = LocationMode.PRIMARY_ONLY;
        this.connectTimeout = connectTimeout;
        this.writeTimeout = writeTimeout;
        this.readTimeout = readTimeout;
        this.responseTimeout = responseTimeout;
        this.proxySettings = proxySettings;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public ProxySettings getProxySettings() {
        return proxySettings;
    }

    public String getConnectString() {
        return connectString;
    }

    private static String buildConnectString(String account, @Nullable String key, @Nullable String sasToken, String endpointSuffix) {
        final boolean hasSasToken = Strings.hasText(sasToken);
        final boolean hasKey = Strings.hasText(key);
        if (hasSasToken == false && hasKey == false) {
            throw new SettingsException("Neither a secret key nor a shared access token was set.");
        }
        if (hasSasToken && hasKey) {
            throw new SettingsException("Both a secret as well as a shared access token were set.");
        }
        final StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append("DefaultEndpointsProtocol=https").append(";AccountName=").append(account);
        if (hasKey) {
            connectionStringBuilder.append(";AccountKey=").append(key);
        } else {
            connectionStringBuilder.append(";SharedAccessSignature=").append(sasToken);
        }
        if (Strings.hasText(endpointSuffix)) {
            connectionStringBuilder.append(";EndpointSuffix=").append(endpointSuffix);
        }
        return connectionStringBuilder.toString();
    }

    public LocationMode getLocationMode() {
        return locationMode;
    }

    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    public TimeValue getWriteTimeout() {
        return writeTimeout;
    }

    public TimeValue getReadTimeout() {
        return readTimeout;
    }

    public TimeValue getResponseTimeout() {
        return responseTimeout;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AzureStorageSettings{");
        sb.append("account='").append(account).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", endpointSuffix='").append(endpointSuffix).append('\'');
        sb.append(", maxRetries=").append(maxRetries);
        sb.append(", proxySettings=").append(proxySettings != ProxySettings.NO_PROXY_SETTINGS ? "PROXY_SET" : "PROXY_NOT_SET");
        sb.append(", locationMode='").append(locationMode).append('\'');
        sb.append(", connectTimeout='").append(connectTimeout).append('\'');
        sb.append(", writeTimeout='").append(writeTimeout).append('\'');
        sb.append(", readTimeout='").append(readTimeout).append('\'');
        sb.append(", responseTimeout='").append(responseTimeout).append('\'');
        sb.append('}');
        return sb.toString();
    }

    /**
     * Parse and read all settings available under the azure.client.* namespace
     * @param settings settings to parse
     * @return All the named configurations
     */
    public static Map<String, AzureStorageSettings> load(Settings settings) {
        // Get the list of existing named configurations
        final Map<String, AzureStorageSettings> storageSettings = new HashMap<>();
        for (final String clientName : ACCOUNT_SETTING.getNamespaces(settings)) {
            storageSettings.put(clientName, getClientSettings(settings, clientName));
        }
        if (false == storageSettings.containsKey("default") && false == storageSettings.isEmpty()) {
            // in case no setting named "default" has been set, let's define our "default"
            // as the first named config we get
            final AzureStorageSettings defaultSettings = storageSettings.values().iterator().next();
            storageSettings.put("default", defaultSettings);
        }
        assert storageSettings.containsKey("default") || storageSettings.isEmpty() : "always have 'default' if any";
        return Collections.unmodifiableMap(storageSettings);
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    private static AzureStorageSettings getClientSettings(Settings settings, String clientName) {
        try (
            SecureString account = getConfigValue(settings, clientName, ACCOUNT_SETTING);
            SecureString key = getConfigValue(settings, clientName, KEY_SETTING);
            SecureString sasToken = getConfigValue(settings, clientName, SAS_TOKEN_SETTING)
        ) {
            return new AzureStorageSettings(
                account.toString(),
                key.toString(),
                sasToken.toString(),
                getValue(settings, clientName, ENDPOINT_SUFFIX_SETTING),
                getValue(settings, clientName, TIMEOUT_SETTING),
                getValue(settings, clientName, MAX_RETRIES_SETTING),
                getValue(settings, clientName, CONNECT_TIMEOUT_SETTING),
                getValue(settings, clientName, WRITE_TIMEOUT_SETTING),
                getValue(settings, clientName, READ_TIMEOUT_SETTING),
                getValue(settings, clientName, RESPONSE_TIMEOUT_SETTING),
                validateAndCreateProxySettings(settings, clientName)
            );
        }
    }

    static ProxySettings validateAndCreateProxySettings(final Settings settings, final String clientName) {
        final ProxySettings.ProxyType proxyType = getConfigValue(settings, clientName, PROXY_TYPE_SETTING);
        final String proxyHost = getConfigValue(settings, clientName, PROXY_HOST_SETTING);
        final int proxyPort = getConfigValue(settings, clientName, PROXY_PORT_SETTING);
        final SecureString proxyUserName = getConfigValue(settings, clientName, PROXY_USERNAME_SETTING);
        final SecureString proxyPassword = getConfigValue(settings, clientName, PROXY_PASSWORD_SETTING);
        // Validate proxy settings
        if (proxyType == ProxySettings.ProxyType.DIRECT
            && (proxyPort != 0 || Strings.hasText(proxyHost) || Strings.hasText(proxyUserName) || Strings.hasText(proxyPassword))) {
            throw new SettingsException("Azure proxy port or host or username or password have been set but proxy type is not defined.");
        }
        if (proxyType != ProxySettings.ProxyType.DIRECT && (proxyPort == 0 || Strings.isEmpty(proxyHost))) {
            throw new SettingsException("Azure proxy type has been set but proxy host or port is not defined.");
        }

        if (proxyType == ProxySettings.ProxyType.DIRECT) {
            return ProxySettings.NO_PROXY_SETTINGS;
        }

        try {
            final InetAddress proxyHostAddress = InetAddress.getByName(proxyHost);
            return new ProxySettings(proxyType, proxyHostAddress, proxyPort, proxyUserName.toString(), proxyPassword.toString());
        } catch (final UnknownHostException e) {
            throw new SettingsException("Azure proxy host is unknown.", e);
        }
    }

    private static <T> T getConfigValue(Settings settings, String clientName, Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    private static <T> T getValue(Settings settings, String groupName, Setting<T> setting) {
        final Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        final String fullKey = k.toConcreteKey(groupName).toString();
        return setting.getConcreteSetting(fullKey).get(settings);
    }

    static Map<String, AzureStorageSettings> overrideLocationMode(
        Map<String, AzureStorageSettings> clientsSettings,
        LocationMode locationMode
    ) {
        final MapBuilder<String, AzureStorageSettings> mapBuilder = new MapBuilder<>();
        for (final Map.Entry<String, AzureStorageSettings> entry : clientsSettings.entrySet()) {
            mapBuilder.put(
                entry.getKey(),
                new AzureStorageSettings(
                    entry.getValue().account,
                    entry.getValue().connectString,
                    entry.getValue().endpointSuffix,
                    entry.getValue().timeout,
                    entry.getValue().maxRetries,
                    locationMode,
                    entry.getValue().connectTimeout,
                    entry.getValue().writeTimeout,
                    entry.getValue().readTimeout,
                    entry.getValue().responseTimeout,
                    entry.getValue().getProxySettings()
                )
            );
        }
        return mapBuilder.immutableMap();
    }
}
